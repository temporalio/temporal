package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ActivityTestSuite struct {
	testcore.FunctionalTestBase
}

type ActivityClientTestSuite struct {
	testcore.FunctionalTestBase
}

func TestActivityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &ActivityTestSuite{})
}

func TestActivityClientTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ActivityClientTestSuite))
}

func (s *ActivityClientTestSuite) TestActivityScheduleToClose_FiredDuringBackoff() {
	// We have activity that always fails.
	// We have backoff timers and schedule_to_close activity timeout happens during that backoff timer.
	// activity will be scheduled twice. After second failure (that should happen at ~4.2 sec) next retry will not
	// be scheduled because "schedule_to_close" will happen before retry happens
	initialRetryInterval := time.Second * 2
	scheduleToCloseTimeout := 3 * time.Second
	startToCloseTimeout := 1 * time.Second

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    initialRetryInterval,
		BackoffCoefficient: 1,
		MaximumInterval:    time.Second * 10,
	}

	var activityCompleted atomic.Int32
	activityFunction := func() (string, error) {
		activityErr := errors.New("bad-luck-please-retry") //nolint:err113
		activityCompleted.Add(1)
		return "", activityErr
	}

	workflowFn := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution:  true,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			RetryPolicy:            activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return "done!", err
	}

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	wfId := "functional-test-gethistoryreverse"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.Error(err)
	var wfExecutionError *temporal.WorkflowExecutionError
	s.True(errors.As(err, &wfExecutionError))
	var activityError *temporal.ActivityError
	s.True(errors.As(wfExecutionError.Unwrap(), &activityError))
	s.Equal(enumspb.RETRY_STATE_TIMEOUT, activityError.RetryState())

	s.Equal(int32(2), activityCompleted.Load())

}

func (s *ActivityClientTestSuite) TestActivityScheduleToClose_FiredDuringActivityRun() {
	// We have activity that always fails.
	// We have backoff timers and schedule_to_close activity timeout happens while activity is running.
	// activity will be scheduled twice.
	// "schedule_to_close" timer should fire while activity is running for a second time
	scheduleToCloseTimeout := 7 * time.Second
	startToCloseTimeout := 3 * time.Second

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second * 1,
		BackoffCoefficient: 1,
	}
	var activityFinishedAt time.Time
	var workflowFinishedAt time.Time

	var activityCompleted atomic.Int32
	var wg sync.WaitGroup

	// we need schedule_to_close timeout to fire when 3rd retry is running
	// with 2 sec run time and 1 sec between retries 3rd retry will start around 2+1+2+1=6 sec
	// with 2 sec run time it will finish at 8 sec
	// schedule to close is set to 7 sec. This way schedule to close timeout should fire.
	activityFunction := func() (string, error) {
		defer wg.Done()
		activityErr := errors.New("bad-luck-please-retry") //nolint:err113
		time.Sleep(2 * time.Second)                        //nolint:forbidigo
		activityCompleted.Add(1)
		activityFinishedAt = time.Now().UTC()
		return "", activityErr
	}

	wg.Add(3) // activity should be executed 3 times
	workflowFn := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution:  true,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			RetryPolicy:            activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return "done!", err
	}

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        s.T().Name(),
		TaskQueue: s.TaskQueue(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	var out string
	err = workflowRun.Get(ctx, &out)
	var activityError *temporal.ActivityError
	s.True(errors.As(err, &activityError))
	s.Equal(enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activityError.RetryState())
	var timeoutError *temporal.TimeoutError
	s.True(errors.As(activityError.Unwrap(), &timeoutError))
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutError.TimeoutType())
	// schedule to close timeout should fire while last activity is still running.
	s.Equal(int32(2), activityCompleted.Load())

	// we expect activities to be executed 3 times. Wait for the last activity to finish
	workflowFinishedAt = time.Now().UTC()
	wg.Wait() // Wait for activity to finish
	s.Equal(int32(3), activityCompleted.Load())
	s.True(activityFinishedAt.After(workflowFinishedAt))
}

func (s *ActivityClientTestSuite) Test_ActivityTimeouts() {
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
				// more likely be sent in the heartbeat batch at 1.6s
				// (basically increasing the room for delay in heartbeat goroutine from 0.1s to 1s)
				for i := 0; i < 3; i++ {
					activity.RecordHeartbeat(ctx, i)
					time.Sleep(200 * time.Millisecond) //nolint:forbidigo
				}
			}()
		}

		time.Sleep(5 * time.Second) //nolint:forbidigo
		return nil
	}

	var err1, err2, err3, err4, err5, err6 error
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
			ActivityID:          "ScheduleToCloseNotSet",
			StartToCloseTimeout: 2 * time.Second,
			RetryPolicy:         noRetryPolicy,
		})
		f4 := workflow.ExecuteActivity(ctx4, activityFn)

		ctx5 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "StartToCloseNotSet",
			ScheduleToCloseTimeout: 2 * time.Second,
			RetryPolicy:            noRetryPolicy,
		})
		f5 := workflow.ExecuteActivity(ctx5, activityFn)

		ctx6 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          "Heartbeat",
			StartToCloseTimeout: 10 * time.Second,
			HeartbeatTimeout:    1 * time.Second,
			RetryPolicy:         noRetryPolicy,
		})
		f6 := workflow.ExecuteActivity(ctx6, activityFn)

		err1 = f1.Get(ctx1, nil)
		err2 = f2.Get(ctx2, nil)
		err3 = f3.Get(ctx3, nil)
		err4 = f4.Get(ctx4, nil)
		err5 = f5.Get(ctx5, nil)
		err6 = f6.Get(ctx6, nil)

		return nil
	}

	s.Worker().RegisterActivity(activityFn)
	s.Worker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 "functional-test-activity-timeouts",
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

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
	s.Equal("ScheduleToCloseNotSet", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())

	s.Error(err5)
	activityErr, ok = err5.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("StartToCloseNotSet", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())

	s.Error(err6)
	activityErr, ok = err6.(*temporal.ActivityError)
	s.True(ok)
	s.Equal("Heartbeat", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, timeoutErr.TimeoutType())
	s.True(timeoutErr.HasLastHeartbeatDetails())
	var v int
	s.NoError(timeoutErr.LastHeartbeatDetails(&v))
	s.Equal(2, v)
}

func (s *ActivityTestSuite) TestActivityHeartBeatWorkflow_Success() {
	id := "functional-heartbeat-test"
	wt := "functional-heartbeat-test-type"
	tl := "functional-heartbeat-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if activityCounter < activityCount {
			activityCounter++

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeString("activity-input"),
					Header:                 header,
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(task.ActivityId), tag.Counter(i))
			_, err := s.FrontendClient().RecordActivityTaskHeartbeat(testcore.NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				Namespace: s.Namespace().String(),
				TaskToken: task.TaskToken,
				Details:   payloads.EncodeString("details"),
			})
			s.NoError(err)
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo
		}
		activityExecutedCount++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(1, activityExecutedCount)

	// go over history and verify that the activity task scheduled event has header on it
	events := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	})

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled {"Header":{"Fields":{"tracing":{"Data":"\"sample data\""}}} }
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, events)
}

func (s *ActivityTestSuite) TestActivityRetry() {
	tv := testvars.New(s.T())

	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false
	var activityAScheduled, activityAFailed, activityBScheduled, activityBTimeout *historypb.HistoryEvent

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "A",
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              tv.TaskQueue(),
						Input:                  payloads.EncodeString("1"),
						ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
						ScheduleToStartTimeout: durationpb.New(4 * time.Second),
						StartToCloseTimeout:    durationpb.New(4 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:        durationpb.New(1 * time.Second),
							MaximumAttempts:        3,
							MaximumInterval:        durationpb.New(1 * time.Second),
							NonRetryableErrorTypes: []string{"bad-bug"},
							BackoffCoefficient:     1,
						},
					}}},
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "B",
						ActivityType:           &commonpb.ActivityType{Name: timeoutActivityName},
						TaskQueue:              &taskqueuepb.TaskQueue{Name: "no_worker_taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:                  payloads.EncodeString("2"),
						ScheduleToCloseTimeout: durationpb.New(5 * time.Second),
						ScheduleToStartTimeout: durationpb.New(5 * time.Second),
						StartToCloseTimeout:    durationpb.New(5 * time.Second),
						HeartbeatTimeout:       durationpb.New(0 * time.Second),
					}}},
			}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				switch event.GetEventType() { // nolint:exhaustive
				case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
					switch event.GetActivityTaskScheduledEventAttributes().GetActivityId() {
					case "A":
						activityAScheduled = event
					case "B":
						activityBScheduled = event
					}

				case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
					if event.GetActivityTaskFailedEventAttributes().GetScheduledEventId() == activityAScheduled.GetEventId() {
						activityAFailed = event
					}

				case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
					if event.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId() == activityBScheduled.GetEventId() {
						activityBTimeout = event
					}
				}
			}
		}

		if activityAFailed != nil && activityBTimeout != nil {
			s.Logger.Info("Completing Workflow")
			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}

		return []*commandpb.Command{}, nil
	}

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(tv.WorkflowID(), task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		var err error
		if activityExecutedCount == 0 {
			err = errors.New("bad-luck-please-retry") //nolint:err113
		} else if activityExecutedCount == 1 {
			err = temporal.NewNonRetryableApplicationError("bad-bug", "", nil)
		}
		activityExecutedCount++
		return nil, false, err
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
				RunId:      we.RunId,
			},
		})
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	_, err = s.TaskPoller().PollAndHandleActivityTask(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.Equal(tv.WorkflowID(), task.WorkflowExecution.GetWorkflowId())
			s.Equal(activityName, task.ActivityType.GetName())
			return nil, errors.New("bad-luck-please-retry")
		})
	s.NoError(err)

	descResp, err := describeWorkflowExecution()
	s.NoError(err)
	for _, pendingActivity := range descResp.GetPendingActivities() {
		if pendingActivity.GetActivityId() == "A" {
			s.NotNil(pendingActivity.GetLastFailure().GetApplicationFailureInfo())
			expectedErrString := "bad-luck-please-retry"
			s.Equal(expectedErrString, pendingActivity.GetLastFailure().GetMessage())
			s.False(pendingActivity.GetLastFailure().GetApplicationFailureInfo().GetNonRetryable())
			s.Equal(tv.WorkerIdentity(), pendingActivity.GetLastWorkerIdentity())
		}
	}

	_, err = s.TaskPoller().PollAndHandleActivityTask(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.Equal(tv.WorkflowID(), task.WorkflowExecution.GetWorkflowId())
			s.Equal(activityName, task.ActivityType.GetName())
			return nil, temporal.NewNonRetryableApplicationError("bad-bug", "", nil)
		})
	s.NoError(err)

	descResp, err = describeWorkflowExecution()
	s.NoError(err)
	s.Len(descResp.GetPendingActivities(), 1)
	s.Equal(descResp.GetPendingActivities()[0].GetActivityId(), "B")

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := 0; i < 3; i++ {
		s.False(workflowComplete)

		s.Logger.Info("Processing workflow task:", tag.Counter(i))
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithRetries(1))
		if err != nil {
			s.PrintHistoryEvents(s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
				RunId:      we.GetRunId(),
			}))
		}
		s.NoError(err, "Poll for workflow task failed")

		if workflowComplete {
			break
		}
	}

	s.True(workflowComplete)
}

func (s *ActivityTestSuite) TestActivityRetry_Infinite() {
	id := "functional-activity-retry-test"
	wt := "functional-activity-retry-type"
	tl := "functional-activity-retry-taskqueue"
	identity := "worker1"
	activityName := "activity_retry"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activitiesScheduled := false

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activitiesScheduled {
			activitiesScheduled = true

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          "A",
						ActivityType:        &commonpb.ActivityType{Name: activityName},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
						Input:               payloads.EncodeString("1"),
						StartToCloseTimeout: durationpb.New(100 * time.Second),
						RetryPolicy: &commonpb.RetryPolicy{
							MaximumAttempts:    0,
							BackoffCoefficient: 1,
						},
					}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	activityExecutedCount := 0
	activityExecutedLimit := 4
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())

		var err error
		if activityExecutedCount < activityExecutedLimit {
			err = errors.New("retry-error") //nolint:err113
		} else if activityExecutedCount == activityExecutedLimit {
			err = nil
		}
		activityExecutedCount++
		return nil, false, err
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	for i := 0; i <= activityExecutedLimit; i++ {
		err = poller.PollAndProcessActivityTask(false)
		s.NoError(err)
	}

	_, err = poller.PollAndProcessWorkflowTask(testcore.WithRetries(1))
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *ActivityTestSuite) TestActivityHeartBeatWorkflow_Timeout() {
	id := "functional-heartbeat-timeout-test"
	wt := "functional-heartbeat-timeout-test-type"
	tl := "functional-heartbeat-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		s.Logger.Info("Calling WorkflowTask Handler", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(1 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityExecutedCount := 0
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		// Timing out more than HB time.
		time.Sleep(2 * time.Second) //nolint:forbidigo
		activityExecutedCount++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	err = poller.PollAndProcessActivityTask(false)
	// Not s.ErrorIs() because error goes through RPC.
	s.IsType(consts.ErrActivityTaskNotFound, err)
	s.Equal(consts.ErrActivityTaskNotFound.Error(), err.Error())

	s.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *ActivityTestSuite) TestTryActivityCancellationFromWorkflow() {
	id := "functional-activity-cancellation-test"
	wt := "functional-activity-cancellation-test-type"
	tl := "functional-activity-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))

			activityScheduledID = task.StartedEventId + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(0 * time.Second),
				}},
			}}, nil
		}

		if requestCancellation {
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: activityScheduledID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	activityCanceled := false
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Equal(id, task.WorkflowExecution.GetWorkflowId())
		s.Equal(activityName, task.ActivityType.GetName())
		for i := 0; i < 10; i++ {
			s.Logger.Info("Heartbeating for activity", tag.WorkflowActivityID(task.ActivityId), tag.Counter(i))
			response, err := s.FrontendClient().RecordActivityTaskHeartbeat(testcore.NewContext(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: s.Namespace().String(),
					TaskToken: task.TaskToken,
					Details:   payloads.EncodeString("details"),
				})
			if response != nil && response.CancelRequested {
				activityCanceled = true
				return payloads.EncodeString("Activity Cancelled"), true, nil
			}
			s.NoError(err)
			time.Sleep(10 * time.Millisecond) //nolint:forbidigo
		}
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	cancelCh := make(chan struct{})
	go func() {
		s.Logger.Info("Trying to cancel the task in a different thread")
		// Send signal so that worker can send an activity cancel
		_, err1 := s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
			SignalName: "my signal",
			Input:      nil,
			Identity:   identity,
		})
		s.NoError(err1)

		scheduleActivity = false
		requestCancellation = true
		_, err2 := poller.PollAndProcessWorkflowTask()
		s.NoError(err2)
		close(cancelCh)
	}()

	s.Logger.Info("Start activity.")
	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	s.Logger.Info("Waiting for cancel to complete.", tag.WorkflowRunID(we.RunId))
	<-cancelCh
	s.True(activityCanceled, "Activity was not cancelled.")
	s.Logger.Info("Activity cancelled.", tag.WorkflowRunID(we.RunId))
}

func (s *ActivityTestSuite) TestActivityCancellationNotStarted() {
	id := "functional-activity-notstarted-cancellation-test"
	wt := "functional-activity-notstarted-cancellation-test-type"
	tl := "functional-activity-notstarted-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_notstarted"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
			s.Logger.Info("Scheduling activity")
			activityScheduledID = task.StartedEventId + 2
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(2 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(0 * time.Second),
				}},
			}}, nil
		}

		if requestCancellation {
			s.Logger.Info("Requesting cancellation")
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: activityScheduledID,
				}},
			}}, nil
		}

		s.Logger.Info("Completing Workflow")
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// dummy activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		s.Fail("activity should not run")
		return nil, false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	// Send signal so that worker can send an activity cancel
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = s.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
		SignalName: signalName,
		Input:      signalInput,
		Identity:   identity,
	})
	s.NoError(err)

	// Process signal in workflow and send request cancellation
	scheduleActivity = false
	requestCancellation = true
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)

	scheduleActivity = false
	requestCancellation = false
	_, err = poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
}

func (s *ActivityClientTestSuite) TestActivityHeartbeatDetailsDuringRetry() {
	// Latest reported heartbeat on activity should be available throughout workflow execution or until activity succeeds.
	// 1. Start workflow with single activity
	// 2. First invocation of activity sets heartbeat details and times out.
	// 3. Second invocation triggers retriable error.
	// 4. Next invocations succeed.
	// 5. Test should start polling for heartbeat details once first heartbeat was reported.
	// 6. Once workflow completes -- we're done.

	activityTimeout := time.Second

	activityExecutedCount := 0
	heartbeatDetails := 7771 // any value
	heartbeatDetailsPayload, err := payloads.Encode(heartbeatDetails)
	s.NoError(err)
	activityFn := func(ctx context.Context) error {
		var err error
		if activityExecutedCount == 0 {
			activity.RecordHeartbeat(ctx, heartbeatDetails)
			time.Sleep(activityTimeout + time.Second) //nolint:forbidigo
		} else if activityExecutedCount == 1 {
			time.Sleep(activityTimeout / 2)     //nolint:forbidigo
			err = errors.New("retryable-error") //nolint:err113
		}

		if activityExecutedCount > 0 {
			s.True(activity.HasHeartbeatDetails(ctx))
			var details int
			s.NoError(activity.GetHeartbeatDetails(ctx, &details))
			s.Equal(details, heartbeatDetails)
		}

		activityExecutedCount++
		return err
	}

	var err1 error

	activityId := "heartbeat_retry"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)

		err1 = f1.Get(ctx1, nil)

		return nil
	}

	s.Worker().RegisterActivity(activityFn)
	s.Worker().RegisterWorkflow(workflowFn)

	wfId := "functional-test-heartbeat-details-during-retry"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	runId := workflowRun.GetRunID()

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wfId,
				RunId:      runId,
			},
		})
	}

	//nolint:forbidigo
	time.Sleep(time.Second) // wait for the timeout to trigger

	for dweResult, dweErr := describeWorkflowExecution(); dweResult.GetWorkflowExecutionInfo().GetCloseTime() == nil; dweResult, dweErr = describeWorkflowExecution() {
		s.NoError(dweErr)
		s.NotNil(dweResult.GetWorkflowExecutionInfo())
		s.LessOrEqual(len(dweResult.PendingActivities), 1)

		if dweResult.PendingActivities != nil && len(dweResult.PendingActivities) == 1 {
			details := dweResult.PendingActivities[0].GetHeartbeatDetails()
			s.Equal(heartbeatDetailsPayload, details)
		}

		time.Sleep(time.Millisecond * 100) //nolint:forbidigo
	}

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	s.NoError(err1)
}

// TestActivityHeartBeat_RecordIdentity verifies that the identity of the worker sending the heartbeat
// is recorded in pending activity info and returned in describe workflow API response. This happens
// only when the worker identity is not sent when a poller picks the task.
func (s *ActivityTestSuite) TestActivityHeartBeat_RecordIdentity() {
	id := "functional-heartbeat-identity-record"
	workerIdentity := "70df788a-b0b2-4113-a0d5-130f13889e35"
	activityName := "activity_timer"

	taskQueue := &taskqueuepb.TaskQueue{Name: "functional-heartbeat-identity-record-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "functional-heartbeat-identity-record-type"},
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            workerIdentity,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err)

	workflowComplete := false
	workflowNextCmd := enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		switch workflowNextCmd { // nolint:exhaustive
		case enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
			workflowNextCmd = enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.IntToString(rand.Int()),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeString("activity-input"),
					Header:                 header,
					ScheduleToCloseTimeout: durationpb.New(15 * time.Second),
					ScheduleToStartTimeout: durationpb.New(60 * time.Second),
					StartToCloseTimeout:    durationpb.New(15 * time.Second),
					HeartbeatTimeout:       durationpb.New(60 * time.Second),
				}},
			}}, nil
		case enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION:
			workflowComplete = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
			}}, nil
		}
		panic("Unexpected workflow state")
	}

	activityStartedSignal := make(chan bool) // Used by activity channel to signal the start so that the test can verify empty identity.
	heartbeatSignalChan := make(chan bool)   // Activity task sends heartbeat when signaled on this chan. It also signals back on the same chan after sending the heartbeat.
	endActivityTask := make(chan bool)       // Activity task completes when signaled on this chan. This is to force the task to be in pending state.
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		activityStartedSignal <- true // signal the start of activity task.
		<-heartbeatSignalChan         // wait for signal before sending heartbeat.
		_, err := s.FrontendClient().RecordActivityTaskHeartbeat(testcore.NewContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: s.Namespace().String(),
			TaskToken: task.TaskToken,
			Details:   payloads.EncodeString("details"),
			Identity:  workerIdentity, // explicitly set the worker identity in the heartbeat request
		})
		s.NoError(err)
		heartbeatSignalChan <- true // signal that the heartbeat is sent.

		<-endActivityTask // wait for signal before completing the task
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            "", // Do not send the worker identity.
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// execute workflow task so that an activity can be enqueued.
	_, err = poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	// execute activity task which waits for signal before sending heartbeat.
	go func() {
		err := poller.PollAndProcessActivityTask(false)
		s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))
	}()

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
	}
	<-activityStartedSignal // wait for the activity to start

	// Verify that the worker identity is empty.
	descRespBeforeHeartbeat, err := describeWorkflowExecution()
	s.NoError(err)
	s.Empty(descRespBeforeHeartbeat.PendingActivities[0].LastWorkerIdentity)

	heartbeatSignalChan <- true // ask the activity to send a heartbeat.
	<-heartbeatSignalChan       // wait for the heartbeat to be sent (to prevent the test from racing to describe the workflow before the heartbeat is sent)

	// Verify that the worker identity is set now.
	descRespAfterHeartbeat, err := describeWorkflowExecution()
	s.NoError(err)
	s.Equal(workerIdentity, descRespAfterHeartbeat.PendingActivities[0].LastWorkerIdentity)

	// unblock the activity task
	endActivityTask <- true

	// ensure that the workflow is complete.
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

func (s *ActivityTestSuite) TestActivityTaskCompleteForceCompletion() {
	activityInfo := make(chan activity.Info, 1)
	taskQueue := testcore.RandomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, s.SdkClient(), taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	ctx := testcore.NewContext()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.NewString(),
		TaskQueue: taskQueue,
	}
	run, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(description.PendingActivities))
		require.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = s.SdkClient().CompleteActivityByID(ctx, s.Namespace().String(), run.GetID(), run.GetRunID(), ai.ActivityID, nil, nil)
	s.NoError(err)

	// Ensure the activity is completed and the workflow is unblcked.
	s.NoError(run.Get(ctx, nil))
}

func (s *ActivityTestSuite) TestActivityTaskCompleteRejectCompletion() {
	activityInfo := make(chan activity.Info, 1)
	taskQueue := testcore.RandomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, s.SdkClient(), taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	ctx := testcore.NewContext()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.NewString(),
		TaskQueue: taskQueue,
	}
	run, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(description.PendingActivities))
		require.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = s.SdkClient().CompleteActivity(ctx, ai.TaskToken, nil, nil)
	var svcErr *serviceerror.NotFound
	s.ErrorAs(err, &svcErr, "invalid activityID or activity already timed out or invoking workflow is completed")
}

func (s *ActivityTestSuite) mockWorkflowWithErrorActivity(activityInfo chan<- activity.Info, sdkClient sdkclient.Client, taskQueue string) (worker.Worker, func(ctx workflow.Context) error) {
	mockErrorActivity := func(ctx context.Context) error {
		ai := activity.GetInfo(ctx)
		activityInfo <- ai
		return errors.New("mock error of an activity") //nolint:err113
	}
	wf := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 3 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				// Add long initial interval to make sure the next attempt is not scheduled
				// before the test gets a chance to complete the activity via API call.
				InitialInterval: 2 * time.Minute,
			},
		}
		ctx2 := workflow.WithActivityOptions(ctx, ao)
		var mockErrorResult error
		err := workflow.ExecuteActivity(ctx2, mockErrorActivity).Get(ctx2, &mockErrorResult)
		if err != nil {
			return err
		}
		return mockErrorResult
	}

	workflowType := "test"
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	w.RegisterActivity(mockErrorActivity)
	return w, wf
}
