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
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc"
)

type (
	clientIntegrationSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		IntegrationBase
		hostPort  string
		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string
	}
)

var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func TestClientIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(clientIntegrationSuite))
}

func (s *clientIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/clientintegrationtestcluster.yaml")

	s.hostPort = "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		s.hostPort = TestFlags.FrontendAddr
	}
}

func (s *clientIntegrationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *clientIntegrationSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	s.taskQueue = s.randomizeStr("tq")
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker", tag.Error(err))
	}
}

func (s *clientIntegrationSuite) TearDownTest() {
	s.worker.Stop()
	s.sdkClient.Close()
}

// testDataConverter implements encoded.DataConverter using gob
type testDataConverter struct {
	NumOfCallToPayloads   int // for testing to know testDataConverter is called as expected
	NumOfCallFromPayloads int
}

func (tdc *testDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	tdc.NumOfCallToPayloads++
	result := &commonpb.Payloads{}
	for i, value := range values {
		p, err := tdc.ToPayload(value)
		if err != nil {
			return nil, fmt.Errorf(
				"args[%d], %T: %w", i, value, err)
		}
		result.Payloads = append(result.Payloads, p)
	}
	return result, nil
}

func (tdc *testDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	tdc.NumOfCallFromPayloads++
	for i, p := range payloads.GetPayloads() {
		err := tdc.FromPayload(p, valuePtrs[i])
		if err != nil {
			return fmt.Errorf("args[%d]: %w", i, err)
		}
	}
	return nil
}

func (tdc *testDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	p := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("gob"),
		},
		Data: buf.Bytes(),
	}
	return p, nil
}

func (tdc *testDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported
	}

	return decodeGob(payload, valuePtr)
}

func (tdc *testDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	var result []string
	for _, p := range payloads.GetPayloads() {
		result = append(result, tdc.ToString(p))
	}

	return result
}

func decodeGob(payload *commonpb.Payload, valuePtr interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(payload.GetData()))
	return dec.Decode(valuePtr)
}

func (tdc *testDataConverter) ToString(payload *commonpb.Payload) string {
	encoding, ok := payload.GetMetadata()["encoding"]
	if !ok {
		return ErrEncodingIsNotSet.Error()
	}

	e := string(encoding)
	if e != "gob" {
		return ErrEncodingIsNotSupported.Error()
	}

	var value interface{}
	err := decodeGob(payload, &value)
	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("%+v", value)
}

func newTestDataConverter() converter.DataConverter {
	return &testDataConverter{}
}

func testActivity(ctx context.Context, msg string) (string, error) {
	return "hello_" + msg, nil
}

func testDataConverterWorkflow(ctx workflow.Context, tl string) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: 20 * time.Second,
		StartToCloseTimeout:    40 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, testActivity, "world").Get(ctx, &result)
	if err != nil {
		return "", err
	}

	// use another converter to run activity,
	// with new taskQueue so that worker with same data converter can properly process tasks.
	var result1 string
	ctx1 := workflow.WithDataConverter(ctx, newTestDataConverter())
	ctx1 = workflow.WithTaskQueue(ctx1, tl)
	err1 := workflow.ExecuteActivity(ctx1, testActivity, "world1").Get(ctx1, &result1)
	if err1 != nil {
		return "", err1
	}
	return result + "," + result1, nil
}

func (s *clientIntegrationSuite) startWorkerWithDataConverter(tl string, dataConverter converter.DataConverter) (sdkclient.Client, worker.Worker) {
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:      s.hostPort,
		Namespace:     s.namespace,
		DataConverter: dataConverter,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}

	worker := worker.New(sdkClient, tl, worker.Options{})
	worker.RegisterActivity(testActivity)
	worker.RegisterWorkflow(testChildWorkflow)

	if err := worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker with data converter", tag.Error(err))
	}
	return sdkClient, worker
}

func (s *clientIntegrationSuite) TestClientDataConverter() {
	tl := "client-integration-data-converter-activity-taskqueue"
	dc := newTestDataConverter()
	sdkClient, worker := s.startWorkerWithDataConverter(tl, dc)
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()
	s.worker.RegisterWorkflow(testDataConverterWorkflow)
	s.worker.RegisterActivity(testActivity)
	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("hello_world,hello_world1", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(1, d.NumOfCallToPayloads)
	s.Equal(1, d.NumOfCallFromPayloads)
}

func (s *clientIntegrationSuite) TestClientDataConverter_Failed() {
	tl := "client-integration-data-converter-activity-failed-taskqueue"
	sdkClient, worker := s.startWorkerWithDataConverter(tl, nil) // mismatch of data converter
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-failed-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()

	s.worker.RegisterWorkflow(testDataConverterWorkflow)
	s.worker.RegisterActivity(testActivity)
	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testDataConverterWorkflow, tl)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.Error(err)

	// Get history to make sure only the 2nd activity is failed because of mismatch of data converter
	iter := s.sdkClient.GetWorkflowHistory(ctx, id, we.GetRunID(), false, 0)
	completedAct := 0
	failedAct := 0
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			completedAct++
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED {
			failedAct++
			s.NotNil(event.GetActivityTaskFailedEventAttributes().GetFailure().GetApplicationFailureInfo())
			s.True(strings.HasPrefix(event.GetActivityTaskFailedEventAttributes().GetFailure().GetMessage(), "unable to decode the activity function input payload with error"))
		}
	}
	s.Equal(1, completedAct)
	s.Equal(1, failedAct)
}

var childTaskQueue = "client-integration-data-converter-child-taskqueue"

func testParentWorkflow(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	execution := workflow.GetInfo(ctx).WorkflowExecution
	childID := fmt.Sprintf("child_workflow:%v", execution.RunID)
	cwo := workflow.ChildWorkflowOptions{
		WorkflowID:         childID,
		WorkflowRunTimeout: time.Minute,
	}
	ctx = workflow.WithChildOptions(ctx, cwo)
	var result string
	err := workflow.ExecuteChildWorkflow(ctx, testChildWorkflow, 0, 3).Get(ctx, &result)
	if err != nil {
		logger.Error("Parent execution received child execution failure", "error", err)
		return "", err
	}

	childID1 := fmt.Sprintf("child_workflow1:%v", execution.RunID)
	cwo1 := workflow.ChildWorkflowOptions{
		WorkflowID:         childID1,
		WorkflowRunTimeout: time.Minute,
		TaskQueue:          childTaskQueue,
	}
	ctx1 := workflow.WithChildOptions(ctx, cwo1)
	ctx1 = workflow.WithDataConverter(ctx1, newTestDataConverter())
	var result1 string
	err1 := workflow.ExecuteChildWorkflow(ctx1, testChildWorkflow, 0, 2).Get(ctx1, &result1)
	if err1 != nil {
		logger.Error("Parent execution received child execution 1 failure", "error", err1)
		return "", err1
	}

	res := fmt.Sprintf("Complete child1 %s times, complete child2 %s times", result, result1)
	logger.Info("Parent execution completed", "Result", res)
	return res, nil
}

func testChildWorkflow(ctx workflow.Context, totalCount, runCount int) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow execution started")
	if runCount <= 0 {
		logger.Error("Invalid valid for run count", "RunCount", runCount)
		return "", errors.New("invalid run count")
	}

	totalCount++
	runCount--
	if runCount == 0 {
		result := fmt.Sprintf("Child workflow execution completed after %v runs", totalCount)
		logger.Info("Child workflow completed", "Result", result)
		return strconv.Itoa(totalCount), nil
	}

	logger.Info("Child workflow starting new run", "RunCount", runCount, "TotalCount", totalCount)
	return "", workflow.NewContinueAsNewError(ctx, testChildWorkflow, totalCount, runCount)
}

func (s *clientIntegrationSuite) TestClientDataConverter_WithChild() {
	dc := newTestDataConverter()
	sdkClient, worker := s.startWorkerWithDataConverter(childTaskQueue, dc)
	defer func() {
		worker.Stop()
		sdkClient.Close()
	}()

	id := "client-integration-data-converter-with-child-workflow"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: time.Minute,
	}
	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(time.Minute)
	defer cancel()
	s.worker.RegisterWorkflow(testParentWorkflow)
	s.worker.RegisterWorkflow(testChildWorkflow)

	we, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, testParentWorkflow)
	if err != nil {
		s.Logger.Fatal("Start workflow with err", tag.Error(err))
	}
	s.NotNil(we)
	s.True(we.GetRunID() != "")

	var res string
	err = we.Get(ctx, &res)
	s.NoError(err)
	s.Equal("Complete child1 3 times, complete child2 2 times", res)

	// to ensure custom data converter is used, this number might be different if client changed.
	d := dc.(*testDataConverter)
	s.Equal(2, d.NumOfCallToPayloads)
	s.Equal(2, d.NumOfCallFromPayloads)
}

func (s *clientIntegrationSuite) Test_StickyWorkerRestartWorkflowTask() {
	testCases := []struct {
		name       string
		waitTime   time.Duration
		doQuery    bool
		doSignal   bool
		delayCheck func(duration time.Duration) bool
	}{
		{
			name:     "new workflow task after 10s, no delay",
			waitTime: 10 * time.Second,
			doSignal: true,
			delayCheck: func(duration time.Duration) bool {
				return duration < 5*time.Second
			},
		},
		{
			name:     "new workflow task immediately, expect 5s delay",
			waitTime: 0,
			doSignal: true,
			delayCheck: func(duration time.Duration) bool {
				return duration > 5*time.Second
			},
		},
		{
			name:     "new query after 10s, no delay",
			waitTime: 10 * time.Second,
			doQuery:  true,
			delayCheck: func(duration time.Duration) bool {
				return duration < 5*time.Second
			},
		},
		{
			name:     "new query immediately, expect 5s delay",
			waitTime: 0,
			doQuery:  true,
			delayCheck: func(duration time.Duration) bool {
				return duration > 5*time.Second
			},
		},
	}
	for _, tt := range testCases {
		s.Run(tt.name, func() {
			workflowFn := func(ctx workflow.Context) (string, error) {
				workflow.SetQueryHandler(ctx, "test", func() (string, error) {
					return "query works", nil
				})

				signalCh := workflow.GetSignalChannel(ctx, "test")
				var msg string
				signalCh.Receive(ctx, &msg)
				return msg, nil
			}

			taskQueue := "task-queue-" + tt.name

			oldWorker := worker.New(s.sdkClient, taskQueue, worker.Options{})
			oldWorker.RegisterWorkflow(workflowFn)
			if err := oldWorker.Start(); err != nil {
				s.Logger.Fatal("Error when start worker", tag.Error(err))
			}

			id := "test-sticky-delay" + tt.name
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:                 id,
				TaskQueue:          taskQueue,
				WorkflowRunTimeout: 20 * time.Second,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			if err != nil {
				s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
			}

			s.NotNil(workflowRun)
			s.True(workflowRun.GetRunID() != "")

			findFirstWorkflowTaskCompleted := false
		WaitForFirstWorkflowTaskComplete:
			for i := 0; i < 10; i++ {
				// wait until first workflow task completed (so we know sticky is set on workflow)
				iter := s.sdkClient.GetWorkflowHistory(ctx, id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
				for iter.HasNext() {
					evt, err := iter.Next()
					s.NoError(err)
					if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
						findFirstWorkflowTaskCompleted = true
						break WaitForFirstWorkflowTaskComplete
					}
				}
				time.Sleep(time.Second)
			}
			s.True(findFirstWorkflowTaskCompleted)

			// stop old worker
			oldWorker.Stop()

			// maybe wait for 10s, which will make matching aware the old sticky worker is unavailable
			time.Sleep(tt.waitTime)

			// start a new worker
			newWorker := worker.New(s.sdkClient, taskQueue, worker.Options{})
			newWorker.RegisterWorkflow(workflowFn)
			if err := newWorker.Start(); err != nil {
				s.Logger.Fatal("Error when start worker", tag.Error(err))
			}
			defer newWorker.Stop()

			startTime := time.Now()
			// send a signal, and workflow should complete immediately, there should not be 5s delay
			if tt.doSignal {
				err = s.sdkClient.SignalWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				err = workflowRun.Get(ctx, nil)
				s.NoError(err)
			} else if tt.doQuery {
				// send a signal, and workflow should complete immediately, there should not be 5s delay
				queryResult, err := s.sdkClient.QueryWorkflow(ctx, id, "", "test", "test")
				s.NoError(err)

				var queryResultStr string
				err = queryResult.Get(&queryResultStr)
				s.NoError(err)
				s.Equal("query works", queryResultStr)
			}
			endTime := time.Now()
			duration := endTime.Sub(startTime)
			s.True(tt.delayCheck(duration), "delay check failed: %s", duration)
		})
	}
}

func (s *clientIntegrationSuite) Test_ActivityTimeouts() {
	activityFn := func(ctx context.Context) error {
		info := activity.GetInfo(ctx)
		if info.ActivityID == "Heartbeat" {
			go func() {
				// NOTE: due to client side heartbeat batching, heartbeat may be sent
				// later than expected.
				// e.g. if activity heartbeat timeout is 2s,
				// and we call RecordHeartbeat() at 0s, 0.5s, 1s, 1.5s
				// the client by default will send two heartbeats at 0s and 2*0.8=1.6s
				// Now if when running the test, this heartbeat goroutine becomes slow,
				// and call RecordHeartbeat() after 1.6s, then that heartbeat will be sent
				// to server at 3.2s (the next batch).
				// Since the entire activity will finish at 5s, there won't be
				// any heartbeat timeout error.
				// so here, we reduce the duration between two heartbeats, so that they are
				// more likey be sent in the heartbeat batch at 1.6s
				// (basically increasing the room for delay in heartbeat goroutine from 0.1s to 1s)
				for i := 0; i < 3; i++ {
					activity.RecordHeartbeat(ctx, i)
					time.Sleep(200 * time.Millisecond)
				}
			}()
		}

		time.Sleep(5 * time.Second)
		return nil
	}

	var err1, err2, err3, err4 error
	workflowFn := func(ctx workflow.Context) error {
		noRetryPolicy := &temporal.RetryPolicy{
			MaximumAttempts: 1, // disable retry
		}
		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "ScheduleToStart",
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			TaskQueue:              "NoWorkerTaskQueue",
			RetryPolicy:            noRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)

		ctx2 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "StartToClose",
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            noRetryPolicy,
		})
		f2 := workflow.ExecuteActivity(ctx2, activityFn)

		ctx3 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "ScheduleToClose",
			ScheduleToCloseTimeout: 2 * time.Second,
			StartToCloseTimeout:    3 * time.Second,
			RetryPolicy:            noRetryPolicy,
		})
		f3 := workflow.ExecuteActivity(ctx3, activityFn)

		ctx4 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          "Heartbeat",
			StartToCloseTimeout: 10 * time.Second,
			HeartbeatTimeout:    1 * time.Second,
			RetryPolicy:         noRetryPolicy,
		})
		f4 := workflow.ExecuteActivity(ctx4, activityFn)

		err1 = f1.Get(ctx1, nil)
		err2 = f2.Get(ctx2, nil)
		err3 = f3.Get(ctx3, nil)
		err4 = f4.Get(ctx4, nil)

		return nil
	}

	s.worker.RegisterActivity(activityFn)
	s.worker.RegisterWorkflow(workflowFn)

	id := "integration-test-activity-timeouts"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")
	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	// verify activity timeout type
	s.Error(err1)
	activityErr, ok := err1.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("ScheduleToStart", activityErr.ActivityID())
	timeoutErr, ok := activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, timeoutErr.TimeoutType())

	s.Error(err2)
	activityErr, ok = err2.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("StartToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())

	s.Error(err3)
	activityErr, ok = err3.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("ScheduleToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())

	s.Error(err4)
	activityErr, ok = err4.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("Heartbeat", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, timeoutErr.TimeoutType())
	s.True(timeoutErr.HasLastHeartbeatDetails())
	var v int
	s.NoError(timeoutErr.LastHeartbeatDetails(&v))
	s.Equal(2, v)

	// s.printHistory(id, workflowRun.GetRunID())
}

// This test simulates workflow try to complete itself while there is buffered event.
// Event sequence:
//  1st WorkflowTask runs a local activity.
//  While local activity is running, a signal is received by server.
//  After signal is received, local activity completed, and workflow drains signal chan (no signal yet) and complete workflow.
//  Server failed the complete request because there is unhandled signal.
//  Server rescheduled a new workflow task.
//  Workflow runs the local activity again and drain the signal chan (with one signal) and complete workflow.
//  Server complete workflow as requested.
func (s *clientIntegrationSuite) Test_UnhandledCommandAndNewTask() {
	sigReadyToSendChan := make(chan struct{}, 1)
	sigSendDoneChan := make(chan struct{})
	localActivityFn := func(ctx context.Context) error {
		// to unblock signal sending, so signal is send after first workflow task started.
		select {
		case sigReadyToSendChan <- struct{}{}:
		default:
		}

		// this will block workflow task and cause the signal to become buffered event
		select {
		case <-sigSendDoneChan:
		case <-ctx.Done():
		}

		return nil
	}

	var err1 error
	var receivedSig string
	workflowFn := func(ctx workflow.Context) error {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		err1 = f1.Get(ctx1, nil)
		if err1 != nil {
			return err1
		}

		sigCh := workflow.GetSignalChannel(ctx, "signal-name")

		for {
			var sigVal string
			ok := sigCh.ReceiveAsync(&sigVal)
			if !ok {
				break
			}
			receivedSig = sigVal
		}

		return nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	id := "integration-test-unhandled-command-new-task"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.taskQueue,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// block until first workflow task started
	select {
	case <-sigReadyToSendChan:
	case <-ctx.Done():
	}

	err = s.sdkClient.SignalWorkflow(ctx, id, workflowRun.GetRunID(), "signal-name", "signal-value")
	s.NoError(err)

	close(sigSendDoneChan)

	err = workflowRun.Get(ctx, nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value", receivedSig)

	// verify event sequence
	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,        // due to unhandled signal
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, // this is the buffered signal
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_MARKER_RECORDED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}
	s.assertHistory(id, workflowRun.GetRunID(), expectedHistory)
}

// This test simulates workflow generate command with invalid attributes.
// Server is expected to fail the workflow task and schedule a retry immediately for first attempt,
// but if workflow task keeps failing, server will drop the task and wait for timeout to schedule additional retries.
// This is the same behavior as the SDK used to do, but now we would do on server.
func (s *clientIntegrationSuite) Test_InvalidCommandAttribute() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	var calledTime []time.Time
	workflowFn := func(ctx workflow.Context) error {
		calledTime = append(calledTime, time.Now().UTC())
		ao := workflow.ActivityOptions{} // invalid activity option without StartToClose timeout
		ctx = workflow.WithActivityOptions(ctx, ao)

		err := workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
		return err
	}

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)

	id := "integration-test-invalid-command-attributes"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.taskQueue,
		// With 3s TaskTimeout and 5s RunTimeout, we expect to see total of 3 attempts.
		// First attempt follow by immediate retry follow by timeout and 3rd attempt after WorkflowTaskTimeout.
		WorkflowTaskTimeout: 3 * time.Second,
		WorkflowRunTimeout:  5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// wait until workflow close (it will be timeout)
	err = workflowRun.Get(ctx, nil)
	s.Error(err)
	s.Contains(err.Error(), "timeout")

	// verify event sequence
	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
	}
	s.assertHistory(id, workflowRun.GetRunID(), expectedHistory)

	// assert workflow task retried 3 times
	s.Equal(3, len(calledTime))

	s.True(calledTime[1].Sub(calledTime[0]) < time.Second)   // retry immediately
	s.True(calledTime[2].Sub(calledTime[1]) > time.Second*3) // retry after WorkflowTaskTimeout
}

func (s *clientIntegrationSuite) Test_BufferedQuery() {
	localActivityFn := func(ctx context.Context) error {
		time.Sleep(5 * time.Second) // use local activity sleep to block workflow task to force query to be buffered
		return nil
	}

	wfStarted := sync.WaitGroup{}
	wfStarted.Add(1)
	workflowFn := func(ctx workflow.Context) error {
		wfStarted.Done()
		status := "init"
		workflow.SetQueryHandler(ctx, "foo", func() (string, error) {
			return status, nil
		})
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			ScheduleToCloseTimeout: 10 * time.Second,
		})
		status = "calling"
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		status = "waiting"
		err1 := f1.Get(ctx1, nil)
		status = "done"

		workflow.Sleep(ctx, 5*time.Second)

		return err1
	}

	s.worker.RegisterWorkflow(workflowFn)

	id := "integration-test-buffered-query"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// wait until first wf task started
	wfStarted.Wait()

	go func() {
		// sleep 2s to make sure DescribeMutableState is called after QueryWorkflow
		time.Sleep(2 * time.Second)
		// make DescribeMutableState call, which force mutable state to reload from db
		s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      workflowRun.GetRunID(),
			},
		})
	}()

	// this query will be buffered in mutable state because workflow task is in-flight.
	encodedQueryResult, err := s.sdkClient.QueryWorkflow(ctx, id, workflowRun.GetRunID(), "foo")

	s.NoError(err)
	var queryResult string
	err = encodedQueryResult.Get(&queryResult)
	s.NoError(err)
	s.Equal("done", queryResult)

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
}

// Uncomment if you need to debug history.
// func (s *clientIntegrationSuite) printHistory(workflowID string, runID string) {
// 	iter := s.sdkClient.GetWorkflowHistory(context.Background(), workflowID, runID, false, 0)
// 	history := &historypb.History{}
// 	for iter.HasNext() {
// 		event, err := iter.Next()
// 		s.NoError(err)
// 		history.Events = append(history.Events, event)
// 	}
// 	common.PrettyPrintHistory(history, s.Logger)
// }

func (s *clientIntegrationSuite) assertHistory(wid, rid string, expected []enumspb.EventType) {
	iter := s.sdkClient.GetWorkflowHistory(context.Background(), wid, rid, false, 0)
	var events []enumspb.EventType
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		events = append(events, event.GetEventType())
	}

	s.Equal(expected, events)
}
