// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
)

func (s *ClientFunctionalSuite) TestMaxBufferedEventsLimit() {
	/*
		This test starts a workflow, and block its workflow task, then sending
		signals to it which will be buffered. The default max buffered event
		count limit is 100. When the test sends 101 signal, the blocked workflow
		task will be forced to close.
	*/
	closeStartChanOnce := sync.Once{}
	waitStartChan := make(chan struct{})
	waitSignalChan := make(chan struct{})

	localActivityFn := func(ctx context.Context) error {
		// notify that workflow task has started
		closeStartChanOnce.Do(func() {
			close(waitStartChan)
		})

		// block workflow task so all signals will be buffered.
		<-waitSignalChan
		return nil
	}

	workflowFn := func(ctx workflow.Context) (int, error) {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 20 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		if err := f1.Get(ctx, nil); err != nil {
			return 0, err
		}

		sigCh := workflow.GetSignalChannel(ctx, "test-signal")

		sigCount := 0
		for sigCh.ReceiveAsync(nil) {
			sigCount++
		}
		return sigCount, nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	testCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	wid := "test-max-buffered-events-limit"
	wf1, err1 := s.sdkClient.ExecuteWorkflow(testCtx, client.StartWorkflowOptions{
		ID:                  wid,
		TaskQueue:           s.taskQueue,
		WorkflowTaskTimeout: time.Second * 20,
	}, workflowFn)

	s.NoError(err1)

	// block until workflow task started
	<-waitStartChan

	// now send 100 signals, all of them will be buffered
	for i := 0; i < 100; i++ {
		err := s.sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", i)
		s.NoError(err)
	}

	// send 101 signal, this will fail the started workflow task
	err := s.sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", 100)
	s.NoError(err)

	// unblock goroutine that runs local activity
	close(waitSignalChan)

	var sigCount int
	err = wf1.Get(testCtx, &sigCount)
	s.NoError(err)
	s.Equal(101, sigCount)

	historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{WorkflowId: wf1.GetID()})
	// Not using historyrequire here because history is not deterministic.
	var failedCause enumspb.WorkflowTaskFailedCause
	for _, evt := range historyEvents {
		if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			failedCause = evt.GetWorkflowTaskFailedEventAttributes().Cause
			break
		}
	}
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND, failedCause)
}

func (s *ClientFunctionalSuite) TestBufferedEventsMutableStateSizeLimit() {
	/*
			This test starts a workflow, and block its workflow task, then sending
			signals to it which will be buffered. The default max mutable state
		    size is 16MB and each signal will have a 1MB payload, so when the 17th
			signal is received, the workflow will be force terminated.
	*/
	closeStartChanOnce := sync.Once{}
	waitStartChan := make(chan struct{})
	waitSignalChan := make(chan struct{})

	localActivityFn := func(ctx context.Context) error {
		// notify that workflow task has started
		closeStartChanOnce.Do(func() {
			close(waitStartChan)
		})

		// block workflow task so all signals will be buffered.
		<-waitSignalChan
		return nil
	}

	workflowFn := func(ctx workflow.Context) (int, error) {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 20 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		if err := f1.Get(ctx, nil); err != nil {
			return 0, err
		}

		sigCh := workflow.GetSignalChannel(ctx, "test-signal")

		sigCount := 0
		for sigCh.ReceiveAsync(nil) {
			sigCount++
		}
		return sigCount, nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	testCtx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	wid := "test-max-buffered-events-limit"
	wf1, err1 := s.sdkClient.ExecuteWorkflow(testCtx, client.StartWorkflowOptions{
		ID:                  wid,
		TaskQueue:           s.taskQueue,
		WorkflowTaskTimeout: time.Second * 20,
	}, workflowFn)

	s.NoError(err1)

	// block until workflow task started
	<-waitStartChan

	// now send 15 signals with 1MB payload each, all of them will be buffered
	buf := make([]byte, 1048577)
	largePayload := payloads.EncodeBytes(buf)
	for i := 0; i < 16; i++ {
		err := s.sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
		s.NoError(err)
	}

	// send 16th signal, this will fail the started workflow task and force terminate the workflow
	err := s.sdkClient.SignalWorkflow(testCtx, wid, "", "test-signal", largePayload)
	s.NoError(err)

	// unblock goroutine that runs local activity
	close(waitSignalChan)

	var sigCount int
	err = wf1.Get(testCtx, &sigCount)
	s.NoError(err)
	s.Equal(17, sigCount)

	historyEvents := s.getHistory(s.namespace, &commonpb.WorkflowExecution{WorkflowId: wf1.GetID()})
	// Not using historyrequire here because history is not deterministic.
	var failedCause enumspb.WorkflowTaskFailedCause
	for _, evt := range historyEvents {
		if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			failedCause = evt.GetWorkflowTaskFailedEventAttributes().Cause
			break
		}
	}
	s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND, failedCause)
}
