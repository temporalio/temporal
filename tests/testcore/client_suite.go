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

package testcore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.uber.org/multierr"
)

type (
	ClientFunctionalSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		FunctionalTestBase
		historyrequire.HistoryRequire
		sdkClient sdkclient.Client
		worker    worker.Worker
		taskQueue string
	}
)

var (
	ErrEncodingIsNotSet       = errors.New("payload encoding metadata is not set")
	ErrEncodingIsNotSupported = errors.New("payload encoding is not supported")
)

func (s *ClientFunctionalSuite) Worker() worker.Worker {
	return s.worker
}

func (s *ClientFunctionalSuite) SdkClient() sdkclient.Client {
	return s.sdkClient
}

func (s *ClientFunctionalSuite) TaskQueue() string {
	return s.taskQueue
}

func (s *ClientFunctionalSuite) SetupSuite() {
	// these limits are higher in production, but our tests would take too long if we set them that high
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.NumPendingChildExecutionsLimitError.Key():             ClientSuiteLimit,
		dynamicconfig.NumPendingActivitiesLimitError.Key():                  ClientSuiteLimit,
		dynamicconfig.NumPendingCancelRequestsLimitError.Key():              ClientSuiteLimit,
		dynamicconfig.NumPendingSignalsLimitError.Key():                     ClientSuiteLimit,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key():      true,
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): ClientSuiteLimit,
		dynamicconfig.EnableNexus.Key():                                     true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key():                    1 * time.Millisecond,
		callbacks.AllowedAddresses.Key():                                    []any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	}
	s.SetDynamicConfigOverrides(dynamicConfigOverrides)
	s.FunctionalTestBase.SetupSuite("testdata/client_cluster.yaml")

}

func (s *ClientFunctionalSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *ClientFunctionalSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())

	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.HostPort(),
		Namespace: s.Namespace(),
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	s.taskQueue = RandomizeStr("tq")

	// We need to set this timeout to 0 to disable the deadlock detector. Otherwise, the deadlock detector will cause
	// TestTooManyChildWorkflows to fail because it thinks there is a deadlock due to the blocked child workflows.
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{DeadlockDetectionTimeout: 0})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when start worker", tag.Error(err))
	}
}

func (s *ClientFunctionalSuite) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *ClientFunctionalSuite) EventuallySucceeds(ctx context.Context, operationCtx backoff.OperationCtx) {
	s.T().Helper()
	s.NoError(backoff.ThrottleRetryContext(
		ctx,
		operationCtx,
		backoff.NewExponentialRetryPolicy(time.Second),
		func(err error) bool {
			// all errors are retryable
			return true
		},
	))
}

func (s *ClientFunctionalSuite) HistoryContainsFailureCausedBy(
	ctx context.Context,
	workflowId string,
	cause enumspb.WorkflowTaskFailedCause,
) {
	s.T().Helper()
	s.EventuallySucceeds(ctx, func(ctx context.Context) error {
		history := s.sdkClient.GetWorkflowHistory(
			ctx,
			workflowId,
			"",
			true,
			enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		)
		for history.HasNext() {
			event, err := history.Next()
			s.NoError(err)
			switch a := event.Attributes.(type) {
			case *historypb.HistoryEvent_WorkflowTaskFailedEventAttributes:
				if a.WorkflowTaskFailedEventAttributes.Cause == cause {
					return nil
				}
			}
		}
		return fmt.Errorf("did not find a failed task whose cause was %q", cause)
	})
}

func (s *ClientFunctionalSuite) Test_StickyWorkerRestartWorkflowTask() {
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
				if err := workflow.SetQueryHandler(ctx, "test", func() (string, error) {
					return "query works", nil
				}); err != nil {
					return "", err
				}

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

			s.Eventually(func() bool {
				// wait until first workflow task completed (so we know sticky is set on workflow)
				iter := s.sdkClient.GetWorkflowHistory(ctx, id, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
				for iter.HasNext() {
					evt, err := iter.Next()
					s.NoError(err)
					if evt.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
						return true
					}
				}
				return false
			}, 10*time.Second, 200*time.Millisecond)

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

func (s *ClientFunctionalSuite) Test_ActivityTimeouts() {
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

	id := "functional-test-activity-timeouts"
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
//
//	1st WorkflowTask runs a local activity.
//	While local activity is running, a signal is received by server.
//	After signal is received, local activity completed, and workflow drains signal chan (no signal yet) and complete workflow.
//	Server failed the complete request because there is unhandled signal.
//	Server rescheduled a new workflow task.
//	Workflow runs the local activity again and drain the signal chan (with one signal) and complete workflow.
//	Server complete workflow as requested.
func (s *ClientFunctionalSuite) Test_BufferedSignalCausesUnhandledCommandAndSchedulesNewTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tv := testvars.New(s.T()).WithTaskQueue(s.taskQueue)

	sigReadyToSendChan := make(chan struct{}, 1)
	sigSendDoneChan := make(chan struct{})
	localActivityFn := func(ctx context.Context) error {
		// Unblock signal sending, so it is sent after first workflow task started.
		sigReadyToSendChan <- struct{}{}
		// Block workflow task and cause the signal to become buffered event.
		select {
		case <-sigSendDoneChan:
		case <-ctx.Done():
		}
		return nil
	}

	var receivedSig string
	workflowFn := func(ctx workflow.Context) error {
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		if err := workflow.ExecuteLocalActivity(ctx1, localActivityFn).Get(ctx1, nil); err != nil {
			return err
		}
		sigCh := workflow.GetSignalChannel(ctx, tv.HandlerName())
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

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        tv.WorkflowID(),
		TaskQueue: tv.TaskQueue().Name,
		// Intentionally use same timeout for WorkflowTaskTimeout and WorkflowRunTimeout so if workflow task is not
		// correctly dispatched, it would time out which would fail the workflow and cause test to fail.
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")
	tv = tv.WithRunID(workflowRun.GetRunID())

	// block until first workflow task started
	<-sigReadyToSendChan

	err = s.sdkClient.SignalWorkflow(ctx, tv.WorkflowID(), tv.RunID(), tv.HandlerName(), "signal-value")
	s.NoError(err)

	close(sigSendDoneChan)

	err = workflowRun.Get(ctx, nil)
	s.NoError(err) // if new workflow task is not correctly dispatched, it would cause timeout error here
	s.Equal("signal-value", receivedSig)

	s.HistoryRequire.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskFailed        // Unhandled signal prevented workflow completion
	5 WorkflowExecutionSignaled // This is the buffered signal
	6 WorkflowTaskScheduled
	7 WorkflowTaskStarted
	8 WorkflowTaskCompleted
	9 MarkerRecorded
	10 WorkflowExecutionCompleted`,
		s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

// Analogous to Test_BufferedSignalCausesUnhandledCommandAndSchedulesNewTask
// TODO: rename to previous name (Test_AdmittedUpdateCausesUnhandledCommandAndSchedulesNewTask) when/if admitted updates start to block workflow from completing.
//
//  1. The worker starts executing the first WFT, before any update is sent.
//  2. While the first WFT is being executed, an update is sent.
//  3. Once the server has received the update, the workflow tries to complete itself.
//  4. The server fails update request with error and completes WF.
func (s *ClientFunctionalSuite) Test_WorkflowCanBeCompletedDespiteAdmittedUpdate() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s.T()).WithTaskQueue(s.taskQueue)

	readyToSendUpdate := make(chan bool, 1)
	updateHasBeenAdmitted := make(chan bool)

	localActivityFn := func(ctx context.Context) error {
		readyToSendUpdate <- true // Ensure update is sent after first WFT has started.
		<-updateHasBeenAdmitted   // Ensure WF completion is not attempted until after update has been admitted.
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		err := workflow.SetUpdateHandler(ctx, tv.HandlerName(), func(ctx workflow.Context, arg string) (string, error) {
			return "my-update-result", nil
		})
		if err != nil {
			return err
		}
		laCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		return workflow.ExecuteLocalActivity(laCtx, localActivityFn).Get(laCtx, nil)
	}

	s.worker.RegisterWorkflow(workflowFn)

	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  tv.WorkflowID(),
		TaskQueue:           tv.TaskQueue().Name,
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  10 * time.Second,
	}, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	// Block until first workflow task started.
	<-readyToSendUpdate

	tv = tv.WithRunID(workflowRun.GetRunID())

	// Send update and wait until it is admitted. This isn't convenient: since Admitted is non-durable, we do not expose
	// an API for doing it directly. Instead we send the update and poll until it's reported to be in admitted state.
	updateHandleCh := make(chan sdkclient.WorkflowUpdateHandle)
	updateErrCh := make(chan error)
	go func() {
		handle, err := s.sdkClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
			UpdateID:     tv.UpdateID(),
			UpdateName:   tv.HandlerName(),
			WorkflowID:   tv.WorkflowID(),
			RunID:        tv.RunID(),
			Args:         []interface{}{"update-value"},
			WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
		})
		updateErrCh <- err
		updateHandleCh <- handle
	}()
	for {
		time.Sleep(10 * time.Millisecond)
		_, err = s.sdkClient.WorkflowService().PollWorkflowExecutionUpdate(ctx, &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace: s.Namespace(),
			UpdateRef: tv.UpdateRef(),
			Identity:  "my-identity",
			WaitPolicy: &updatepb.WaitPolicy{
				LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED,
			},
		})
		if err == nil {
			// Update is admitted but doesn't block WF from completion.
			close(updateHasBeenAdmitted)
			break
		}
	}

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
	updateErr := <-updateErrCh
	s.Error(updateErr)
	var notFound *serviceerror.NotFound
	s.ErrorAs(updateErr, &notFound)
	s.Equal("workflow execution already completed", updateErr.Error())
	updateHandle := <-updateHandleCh
	s.Nil(updateHandle)
	// Uncomment the following when durable admitted is implemented.
	// var updateResult string
	// err = updateHandle.Get(ctx, &updateResult)
	// s.NoError(err)
	// s.Equal("my-update-result", updateResult)

	s.HistoryRequire.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	5 MarkerRecorded
	6 WorkflowExecutionCompleted`,
		s.GetHistory(s.Namespace(), tv.WorkflowExecution()))
}

func (s *ClientFunctionalSuite) Test_CancelActivityAndTimerBeforeComplete() {
	workflowFn := func(ctx workflow.Context) error {
		ctx, cancelFunc := workflow.WithCancel(ctx)

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToStartTimeout: 10 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
			StartToCloseTimeout:    1 * time.Second,
			TaskQueue:              "bad_tq",
		})
		_ = workflow.ExecuteActivity(activityCtx, "Prefix_ToUpper", "hello")

		_ = workflow.NewTimer(ctx, 15*time.Second)

		err := workflow.NewTimer(ctx, time.Second).Get(ctx, nil)
		if err != nil {
			return err
		}
		cancelFunc()
		return nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	id := s.T().Name()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}
	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
}

// This test simulates workflow generate command with invalid attributes.
// Server is expected to fail the workflow task and schedule a retry immediately for first attempt,
// but if workflow task keeps failing, server will drop the task and wait for timeout to schedule additional retries.
// This is the same behavior as the SDK used to do, but now we would do on server.
func (s *ClientFunctionalSuite) Test_InvalidCommandAttribute() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	var startedTime []time.Time
	workflowFn := func(ctx workflow.Context) error {
		info := workflow.GetInfo(ctx)

		// Simply record time.Now() and check if the difference between the recorded time
		// is higher than the workflow task timeout will not work, because there is a delay
		// between server starts the workflow task and this code is executed.

		var currentAttemptStartedTime time.Time
		err := workflow.SideEffect(ctx, func(_ workflow.Context) interface{} {
			rpcCtx := context.Background()
			if deadline, ok := ctx.Deadline(); ok {
				var cancel context.CancelFunc
				rpcCtx, cancel = context.WithDeadline(rpcCtx, deadline)
				defer cancel()
			}

			resp, err := s.sdkClient.DescribeWorkflowExecution(
				rpcCtx,
				info.WorkflowExecution.ID,
				info.WorkflowExecution.RunID,
			)
			if err != nil {
				panic(err)
			}
			return resp.PendingWorkflowTask.StartedTime.AsTime()
		}).Get(&currentAttemptStartedTime)
		if err != nil {
			return err
		}

		startedTime = append(startedTime, currentAttemptStartedTime)
		ao := workflow.ActivityOptions{} // invalid activity option without StartToClose timeout
		ctx = workflow.WithActivityOptions(ctx, ao)

		return workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
	}

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)

	id := "functional-test-invalid-command-attributes"
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
	s.Equal(3, len(startedTime))

	s.True(startedTime[1].Sub(startedTime[0]) < time.Second)   // retry immediately
	s.True(startedTime[2].Sub(startedTime[1]) > time.Second*3) // retry after WorkflowTaskTimeout
}

func (s *ClientFunctionalSuite) Test_BufferedQuery() {
	localActivityFn := func(ctx context.Context) error {
		time.Sleep(5 * time.Second) // use local activity sleep to block workflow task to force query to be buffered
		return nil
	}

	wfStarted := sync.WaitGroup{}
	wfStarted.Add(1)
	workflowFn := func(ctx workflow.Context) error {
		wfStarted.Done()
		status := "init"
		if err := workflow.SetQueryHandler(ctx, "foo", func() (string, error) {
			return status, nil
		}); err != nil {
			return err
		}
		ctx1 := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			ScheduleToCloseTimeout: 10 * time.Second,
		})
		status = "calling"
		f1 := workflow.ExecuteLocalActivity(ctx1, localActivityFn)
		status = "waiting"
		err1 := f1.Get(ctx1, nil)
		status = "done"

		return multierr.Combine(err1, workflow.Sleep(ctx, 5*time.Second))
	}

	s.worker.RegisterWorkflow(workflowFn)

	id := "functional-test-buffered-query"
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
		_, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      workflowRun.GetRunID(),
			},
		})
		s.Assert().NoError(err)
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
// func (s *ClientFunctionalSuite) printHistory(workflowID string, runID string) {
// 	iter := s.sdkClient.GetWorkflowHistory(context.Background(), workflowID, runID, false, 0)
// 	history := &historypb.History{}
// 	for iter.HasNext() {
// 		event, err := iter.Next()
// 		s.NoError(err)
// 		history.Events = append(history.Events, event)
// 	}
// 	common.PrettyPrintHistory(history, s.Logger)
// }

func (s *ClientFunctionalSuite) assertHistory(wid, rid string, expected []enumspb.EventType) {
	iter := s.sdkClient.GetWorkflowHistory(context.Background(), wid, rid, false, 0)
	var events []enumspb.EventType
	for iter.HasNext() {
		event, err := iter.Next()
		s.NoError(err)
		events = append(events, event.GetEventType())
	}

	s.Equal(expected, events)
}

func (s *ClientFunctionalSuite) TestBatchSignal() {

	type myData struct {
		Stuff  string
		Things []int
	}

	workflowFn := func(ctx workflow.Context) (myData, error) {
		var receivedData myData
		workflow.GetSignalChannel(ctx, "my-signal").Receive(ctx, &receivedData)
		return receivedData, nil
	}
	s.worker.RegisterWorkflow(workflowFn)

	workflowRun, err := s.sdkClient.ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
		ID:                       uuid.New(),
		TaskQueue:                s.taskQueue,
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	input1 := myData{
		Stuff:  "here's some data",
		Things: []int{7, 8, 9},
	}
	inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(input1)
	s.NoError(err)

	_, err = s.sdkClient.WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal: "my-signal",
				Input:  inputPayloads,
			},
		},
		Executions: []*commonpb.WorkflowExecution{
			{
				WorkflowId: workflowRun.GetID(),
				RunId:      workflowRun.GetRunID(),
			},
		},
		JobId:  uuid.New(),
		Reason: "test",
	})
	s.NoError(err)

	var returnedData myData
	err = workflowRun.Get(context.Background(), &returnedData)
	s.NoError(err)

	s.Equal(input1, returnedData)
}

func (s *ClientFunctionalSuite) TestBatchReset() {
	var count atomic.Int32

	activityFn := func(ctx context.Context) (int32, error) {
		if val := count.Load(); val != 0 {
			return val, nil
		}
		return 0, temporal.NewApplicationError("some random error", "", false, nil)
	}
	workflowFn := func(ctx workflow.Context) (int, error) {
		ao := workflow.ActivityOptions{
			ScheduleToStartTimeout: 20 * time.Second,
			StartToCloseTimeout:    40 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result int
		err := workflow.ExecuteActivity(ctx, activityFn).Get(ctx, &result)
		return result, err
	}
	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterActivity(activityFn)

	workflowRun, err := s.sdkClient.ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
		ID:                       uuid.New(),
		TaskQueue:                s.taskQueue,
		WorkflowExecutionTimeout: 10 * time.Second,
	}, workflowFn)
	s.NoError(err)

	// make sure it failed the first time
	var result int
	err = workflowRun.Get(context.Background(), &result)
	s.Error(err)

	count.Add(1)

	_, err = s.sdkClient.WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				ResetType: enumspb.RESET_TYPE_FIRST_WORKFLOW_TASK,
			},
		},
		Executions: []*commonpb.WorkflowExecution{
			{
				WorkflowId: workflowRun.GetID(),
				RunId:      workflowRun.GetRunID(),
			},
		},
		JobId:  uuid.New(),
		Reason: "test",
	})
	s.NoError(err)

	// latest run should complete successfully
	s.Eventually(func() bool {
		workflowRun = s.sdkClient.GetWorkflow(context.Background(), workflowRun.GetID(), "")
		err = workflowRun.Get(context.Background(), &result)
		return err == nil && result == 1
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ClientFunctionalSuite) TestBatchResetByBuildId() {
	tq := RandomizeStr(s.T().Name())
	buildPrefix := uuid.New()[:6] + "-"
	v1 := buildPrefix + "v1"
	v2 := buildPrefix + "v2"
	v3 := buildPrefix + "v3"

	var act1count, act2count, act3count, badcount atomic.Int32
	act1 := func() error { act1count.Add(1); return nil }
	act2 := func() error { act2count.Add(1); return nil }
	act3 := func() error { act3count.Add(1); return nil }
	badact := func() error { badcount.Add(1); return nil }

	wf1 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		return "done 1!", nil
	}

	wf2 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		// same as wf1 up to here

		// run act2
		s.NoError(workflow.ExecuteActivity(ao, "act2").Get(ctx, nil))

		// now do something bad in a loop.
		// (we want something that's visible in history, not just failing workflow tasks,
		// otherwise we wouldn't need a reset to "fix" it, just a new build would be enough.)
		for i := 0; i < 1000; i++ {
			s.NoError(workflow.ExecuteActivity(ao, "badact").Get(ctx, nil))
			workflow.Sleep(ctx, time.Second)
		}

		return "done 2!", nil
	}

	wf3 := func(ctx workflow.Context) (string, error) {
		ao := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Second})

		s.NoError(workflow.ExecuteActivity(ao, "act1").Get(ctx, nil))

		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)

		s.NoError(workflow.ExecuteActivity(ao, "act2").Get(ctx, nil))

		// same as wf2 up to here

		// instead of calling badact, do something different to force a non-determinism error
		// (the change of activity type below isn't enough)
		workflow.Sleep(ctx, time.Second)

		// call act3 once
		s.NoError(workflow.ExecuteActivity(ao, "act3").Get(ctx, nil))

		return "done 3!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w1 := worker.New(s.sdkClient, tq, worker.Options{BuildID: v1})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	s.NoError(w1.Start())

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	ex := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	// wait for first wft and first activity to complete
	s.Eventually(func() bool { return len(s.GetHistory(s.Namespace(), ex)) >= 10 }, 5*time.Second, 100*time.Millisecond)

	w1.Stop()

	// should see one run of act1
	s.Equal(int32(1), act1count.Load())

	w2 := worker.New(s.sdkClient, tq, worker.Options{BuildID: v2})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w2.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	// wait until we see three calls to badact
	s.Eventually(func() bool { return badcount.Load() >= 3 }, 10*time.Second, 200*time.Millisecond)

	// at this point act2 should have been invokved once also
	s.Equal(int32(1), act2count.Load())

	w2.Stop()

	w3 := worker.New(s.sdkClient, tq, worker.Options{BuildID: v3})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	w3.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act1"})
	w3.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act2"})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act3"})
	w3.RegisterActivityWithOptions(badact, activity.RegisterOptions{Name: "badact"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// but v3 is not quite compatible, the workflow should be blocked on non-determinism errors for now.
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s.Error(run.Get(waitCtx, nil))

	// wait for it to appear in visibility
	query := fmt.Sprintf(`%s = "%s" and %s = "%s"`,
		searchattribute.ExecutionStatus, "Running",
		searchattribute.BuildIds, worker_versioning.UnversionedBuildIdSearchAttribute(v2))
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace(),
			Query:     query,
		})
		return err == nil && len(resp.Executions) == 1
	}, 10*time.Second, 500*time.Millisecond)

	// reset it using v2 as the bad build ID
	_, err = s.FrontendClient().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace:       s.Namespace(),
		VisibilityQuery: query,
		JobId:           uuid.New(),
		Reason:          "test",
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Options: &commonpb.ResetOptions{
					Target: &commonpb.ResetOptions_BuildId{
						BuildId: v2,
					},
				},
			},
		},
	})
	s.NoError(err)

	// now it can complete on v3. (need to loop since runid will be resolved early and we need
	// to re-resolve to pick up the new run instead of the terminated one)
	s.Eventually(func() bool {
		var out string
		return s.sdkClient.GetWorkflow(ctx, run.GetID(), "").Get(ctx, &out) == nil && out == "done 3!"
	}, 10*time.Second, 200*time.Millisecond)

	s.Equal(int32(1), act1count.Load()) // we should not see an addition run of act1
	s.Equal(int32(2), act2count.Load()) // we should see an addition run of act2 (reset point was before it)
	s.Equal(int32(1), act3count.Load()) // we should see one run of act3
}

func (s *ClientFunctionalSuite) Test_FinishWorkflowWithDeferredCommands() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	childWorkflowFn := func(ctx workflow.Context) error {
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		defer workflow.ExecuteActivity(ctx, activityFn)

		childID := "child_workflow"
		cwo := workflow.ChildWorkflowOptions{
			WorkflowID:         childID,
			WorkflowRunTimeout: 10 * time.Second,
			TaskQueue:          s.taskQueue,
		}
		ctx = workflow.WithChildOptions(ctx, cwo)
		defer workflow.ExecuteChildWorkflow(ctx, childWorkflowFn)
		workflow.NewTimer(ctx, time.Second)
		return nil
	}

	s.worker.RegisterWorkflow(workflowFn)
	s.worker.RegisterWorkflow(childWorkflowFn)
	s.worker.RegisterActivity(activityFn)

	id := "functional-test-finish-workflow-with-deffered-commands"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 id,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 10 * time.Second,
	}

	ctx := context.Background()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	if err != nil {
		s.Logger.Fatal("Start workflow failed with err", tag.Error(err))
	}

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	// verify event sequence
	expectedHistory := []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}
	s.assertHistory(id, workflowRun.GetRunID(), expectedHistory)
}
