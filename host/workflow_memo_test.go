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

package host

import (
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

type RunIdGetter interface {
	GetRunId() string
}
type startFunc func() (RunIdGetter, error)

func (s *integrationSuite) TestStartWithMemo() {
	id := "integration-start-with-memo-test"
	wt := "integration-start-with-memo-test-type"
	tl := "integration-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString(id),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
		Memo:                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.StartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &taskqueuepb.TaskQueue{Name: tl}, memo)
}

func (s *integrationSuite) TestSignalWithStartWithMemo() {
	id := "integration-signal-with-start-with-memo-test"
	wt := "integration-signal-with-start-with-memo-test-type"
	tl := "integration-signal-with-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString(id),
		},
	}

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		Memo:                memo,
	}

	fn := func() (RunIdGetter, error) {
		return s.engine.SignalWithStartWorkflowExecution(NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, &taskqueuepb.TaskQueue{Name: tl}, memo)
}

// helper function for TestStartWithMemo and TestSignalWithStartWithMemo to reduce duplicate code
func (s *integrationSuite) startWithMemoHelper(startFn startFunc, id string, taskQueue *taskqueuepb.TaskQueue, memo *commonpb.Memo) {
	identity := "worker1"

	we, err0 := startFn()
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	wtHandler := func(execution *commonpb.WorkflowExecution, workflowType *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// verify open visibility
	var openExecutionInfo *workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListOpenWorkflowExecutions(NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamp.TimePtr(time.Time{}),
				LatestTime:   timestamp.TimePtr(time.Now().UTC()),
			},
			Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err1)
		if len(resp.Executions) == 1 {
			openExecutionInfo = resp.Executions[0]
			break
		}
		s.Logger.Info("Open WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.NotNil(openExecutionInfo)
	s.Equal(memo, openExecutionInfo.Memo)

	// make progress of workflow
	_, err := poller.PollAndProcessWorkflowTask(false, false)
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// verify history
	execution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}
	historyResponse, historyErr := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace,
		Execution: execution,
	})
	s.Nil(historyErr)
	history := historyResponse.History
	firstEvent := history.Events[0]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, firstEvent.GetEventType())
	startdEventAttributes := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Equal(memo, startdEventAttributes.Memo)

	// verify DescribeWorkflowExecution result
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace,
		Execution: execution,
	}
	descResp, err := s.engine.DescribeWorkflowExecution(NewContext(), descRequest)
	s.NoError(err)
	s.Equal(memo, descResp.WorkflowExecutionInfo.Memo)

	// verify closed visibility
	var closdExecutionInfo *workflowpb.WorkflowExecutionInfo
	for i := 0; i < 10; i++ {
		resp, err1 := s.engine.ListClosedWorkflowExecutions(NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 100,
			StartTimeFilter: &filterpb.StartTimeFilter{
				EarliestTime: timestamp.TimePtr(time.Time{}),
				LatestTime:   timestamp.TimePtr(time.Now().UTC()),
			},
			Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
				WorkflowId: id,
			}},
		})
		s.NoError(err1)
		if len(resp.Executions) == 1 {
			closdExecutionInfo = resp.Executions[0]
			break
		}
		s.Logger.Info("Closed WorkflowExecution is not yet visible")
		time.Sleep(100 * time.Millisecond)
	}
	s.NotNil(closdExecutionInfo)
	s.Equal(memo, closdExecutionInfo.Memo)
}
