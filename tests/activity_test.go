package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ActivityTestSuite struct {
	parallelsuite.Suite[*ActivityTestSuite]
}

type ActivityClientTestSuite struct {
	parallelsuite.Suite[*ActivityClientTestSuite]
}

func TestActivityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityTestSuite{})
}

func TestActivityClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityClientTestSuite{})
}

func (s *ActivityClientTestSuite) TestActivityScheduleToClose_FiredDuringBackoff() {
	env := testcore.NewEnv(s.T())
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

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfId := "functional-test-gethistoryreverse"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	var out string
	err = workflowRun.Get(s.Context(), &out)

	s.Error(err)
	var wfExecutionError *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wfExecutionError)
	var activityError *temporal.ActivityError
	s.ErrorAs(wfExecutionError, &activityError)
	s.Equal(enumspb.RETRY_STATE_TIMEOUT, activityError.RetryState())

	s.Equal(int32(2), activityCompleted.Load())

}

func (s *ActivityClientTestSuite) TestActivityScheduleToClose_FiredDuringActivityRun() {
	env := testcore.NewEnv(s.T())
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

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        s.T().Name(),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	var out string
	err = workflowRun.Get(s.Context(), &out)
	var activityError *temporal.ActivityError
	s.ErrorAs(err, &activityError)
	s.Equal(enumspb.RETRY_STATE_TIMEOUT, activityError.RetryState())
	var timeoutError *temporal.TimeoutError
	s.ErrorAs(activityError, &timeoutError)
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
	env := testcore.NewEnv(s.T())
	activityFn := func(ctx context.Context) error {
		info := activity.GetInfo(ctx)
		if strings.HasPrefix(info.ActivityID, "Heartbeat") {
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
				for i := range 3 {
					activity.RecordHeartbeat(ctx, i)
					time.Sleep(200 * time.Millisecond) //nolint:forbidigo
				}
			}()
		}

		time.Sleep(5 * time.Second) //nolint:forbidigo
		return nil
	}

	var err1, err2, err3, err4, err5, err6, err7, err8 error
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

		ctx7 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "HeartbeatWithScheduleToClose",
			ScheduleToCloseTimeout: 2 * time.Second,
			HeartbeatTimeout:       1 * time.Second,
		})
		f7 := workflow.ExecuteActivity(ctx7, activityFn)

		ctx8 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "StartToCloseWithScheduleToClose",
			ScheduleToCloseTimeout: 3 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: 2 * time.Second,
			},
		})
		f8 := workflow.ExecuteActivity(ctx8, activityFn)

		err1 = f1.Get(ctx1, nil)
		err2 = f2.Get(ctx2, nil)
		err3 = f3.Get(ctx3, nil)
		err4 = f4.Get(ctx4, nil)
		err5 = f5.Get(ctx5, nil)
		err6 = f6.Get(ctx6, nil)
		err7 = f7.Get(ctx7, nil)
		err8 = f8.Get(ctx8, nil)
		return nil
	}

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 "functional-test-activity-timeouts",
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())
	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err)

	// verify activity timeout type
	var activityErr *temporal.ActivityError
	s.ErrorAs(err1, &activityErr)
	s.Equal("ScheduleToStart", activityErr.ActivityID())
	timeoutErr, ok := activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, timeoutErr.TimeoutType())

	s.ErrorAs(err2, &activityErr)
	s.Equal("StartToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())

	s.ErrorAs(err3, &activityErr)
	s.Equal("ScheduleToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())

	s.ErrorAs(err4, &activityErr)
	s.Equal("ScheduleToCloseNotSet", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())

	s.ErrorAs(err5, &activityErr)
	s.Equal("StartToCloseNotSet", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())

	s.ErrorAs(err6, &activityErr)
	s.Equal("Heartbeat", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_HEARTBEAT, timeoutErr.TimeoutType())
	s.True(timeoutErr.HasLastHeartbeatDetails())
	var v int
	s.NoError(timeoutErr.LastHeartbeatDetails(&v))
	s.Equal(2, v)

	s.ErrorAs(err7, &activityErr)
	s.Equal("HeartbeatWithScheduleToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())
	s.Equal("Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying (type: ScheduleToClose)", timeoutErr.Error())
	s.True(timeoutErr.HasLastHeartbeatDetails())
	v = 0
	s.NoError(timeoutErr.LastHeartbeatDetails(&v))
	s.Equal(2, v)

	s.ErrorAs(err8, &activityErr)
	s.Equal("StartToCloseWithScheduleToClose", activityErr.ActivityID())
	timeoutErr, ok = activityErr.Unwrap().(*temporal.TimeoutError)
	s.True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutErr.TimeoutType())
	s.Equal("Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying (type: ScheduleToClose)", timeoutErr.Error())
}

// TestWFASAAStartToCloseTimeout ports a slice of Test_ActivityTimeouts above: a started attempt
// exceeds its StartToClose timeout and, with no retries left, the activity ends TIMED_OUT. Both
// subtests must reach the same terminal status AND the same TimeoutType. WorkflowActivity is the
// oracle.
//
// Fidelity vs the original: covered — terminal status and the StartToClose TimeoutType (the semantic
// contract). Not covered: the other three timeout types (each is an additional scenario, not more
// fidelity here) and the failure *message* (SAA carries a proto message; WFA's SDK TimeoutError
// formats its own, so the strings differ by construction, not by behavior — the TimeoutType is the
// stable cross-surface discriminant).
func (s *standaloneActivityTestSuite) TestWFASAAStartToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.StartToCloseElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts
// (fired-during-run): the activity is started, then its ScheduleToClose deadline elapses while it
// runs, so it ends TIMED_OUT with the ScheduleToClose TimeoutType. Both subtests must reach the same
// status and type. (A never-started activity that hits the deadline times out as ScheduleToStart
// instead — on both surfaces — which is why the port polls first.)
func (s *standaloneActivityTestSuite) TestWFASAAScheduleToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.ScheduleToCloseElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1, HasScheduleToClose: true}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAATimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when retries are exhausted by a StartToClose timeout, or a schedule-to-close deadline closes a
// backing-off activity, the terminal TimedOut failure chains the application failure that drove the
// retries as its Cause (mutable_state_impl.go AddActivityTaskTimedOutEvent, temporalio/temporal#3667).
// Both surfaces must agree; WorkflowActivity is the oracle.
func (s *standaloneActivityTestSuite) TestWFASAATimeoutPreservesUnderlyingFailureCause() {
	env := s.newTestEnv()

	// assertCausePreserved drives the trace on both surfaces and asserts each ends TIMED_OUT with the
	// given timeout type, chaining the "drive" application failure (from attempt 1) as its cause.
	assertCausePreserved := func(t *testing.T, maxAttempts int32, trace []model.Event, timeoutType enumspb.TimeoutType) {
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String()}
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: maxAttempts, retryInterval: 200 * time.Millisecond, shortTimeout: saaTimeoutIn(trace)}
			a := h.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, "drive", a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			cfg := model.Config{MaxAttempts: maxAttempts, HasScheduleToClose: timeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE}
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: cfg, shortTimeout: saaTimeoutIn(trace)}
			a := h.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, "drive", a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	s.T().Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, 2, []model.Event{saaPoll, saaFailRetryably, saaPoll, {Kind: model.StartToCloseElapses}}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry — a distinct server
	// code path that must also chain the cause.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, 0, []model.Event{saaPoll, saaFailRetryably, {Kind: model.ScheduleToCloseElapses}}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	})
}

func (s *ActivityTestSuite) TestActivityHeartBeatWorkflow_Success() {
	env := testcore.NewEnv(s.T())
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
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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

		env.Logger.Info("Completing Workflow")

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
		for i := range 10 {
			env.Logger.Info("Heartbeating for activity", tag.ActivityID(task.ActivityId), tag.Counter(i))
			_, err := env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
				Namespace: env.Namespace().String(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	env.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
	s.Equal(1, activityExecutedCount)

	// go over history and verify that the activity task scheduled event has header on it
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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

// TestActivityHeartBeat ports the core of TestActivityHeartBeatWorkflow_Success above: a worker polls
// the activity and heartbeats a checkpoint payload; the checkpoint round-trips (readable while
// running); then the worker completes it and the activity ends COMPLETED. WorkflowActivity is the
// oracle; both subtests must observe the same heartbeat detail and the same terminal status. (The
// original also asserts the exact workflow history-event shape, which is not part of the shared
// contract.)
var heartbeatWant = []byte(`"hb"`) // == saaHeartbeatDetails

func (s *standaloneActivityTestSuite) TestWFASAAHeartBeat() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.Heartbeat}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		a := h.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Event{Kind: model.RespondCompleted})
		require.Equal(t, want, a.terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		a := h.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Event{Kind: model.RespondCompleted})
		require.Equal(t, want, a.terminal(t))
	})
}

func (s *ActivityTestSuite) TestActivityRetry() {
	env := testcore.NewEnv(s.T())
	tv := env.Tv()

	activityName := "activity_retry"
	timeoutActivityName := "timeout_activity"
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
			env.Logger.Info("Completing Workflow")
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
				RunId:      we.RunId,
			},
		})
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	_, err = env.TaskPoller().PollAndHandleActivityTask(tv,
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

	_, err = env.TaskPoller().PollAndHandleActivityTask(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.Equal(tv.WorkflowID(), task.WorkflowExecution.GetWorkflowId())
			s.Equal(activityName, task.ActivityType.GetName())
			return nil, temporal.NewNonRetryableApplicationError("bad-bug", "", nil)
		})
	s.NoError(err)

	descResp, err = describeWorkflowExecution()
	s.NoError(err)
	s.Len(descResp.GetPendingActivities(), 1)
	s.Equal("B", descResp.GetPendingActivities()[0].GetActivityId())

	env.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))
	for i := range 3 {
		s.False(workflowComplete)

		env.Logger.Info("Processing workflow task:", tag.Counter(i))
		_, err := poller.PollAndProcessWorkflowTask(testcore.WithRetries(1))
		if err != nil {
			s.PrintHistoryEvents(env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
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

// TestWFASAARetry ports the core of TestWFASAARetry (functional test) above to the equivalence
// framework: an attempt fails retryably, the backoff elapses, the next attempt fails non-retryably, and
// the activity ends FAILED with the application failure type. Both subtests must reach the same
// terminal status AND the same failure type. WorkflowActivity is the oracle.
//
// Fidelity vs the original: covered — the retryable-then-non-retryable -> FAILED path and the terminal
// application failure type. Not covered: the original's second activity (a schedule-to-start timeout on
// a no-worker queue — a separate scenario) and its assertions on workflow history-event shape, which is
// not part of the shared cross-surface contract.
func (s *standaloneActivityTestSuite) TestWFASAARetry() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll, saaFailNonRetryably}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "drive"}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

func (s *ActivityTestSuite) TestActivityRetry_Infinite() {
	env := testcore.NewEnv(s.T())
	id := "functional-activity-retry-test"
	wt := "functional-activity-retry-type"
	tl := "functional-activity-retry-taskqueue"
	identity := "worker1"
	activityName := "activity_retry"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
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
	env := testcore.NewEnv(s.T())
	id := "functional-heartbeat-timeout-test"
	wt := "functional-heartbeat-timeout-test-type"
	tl := "functional-heartbeat-timeout-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	activityCount := int32(1)
	activityCounter := int32(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		env.Logger.Info("Calling WorkflowTask Handler", tag.Counter(int(activityCounter)), tag.Number(int64(activityCount)))

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	err = poller.PollAndProcessActivityTask(false)
	// Not s.ErrorIs() because error goes through RPC.
	s.IsType(consts.ErrActivityTaskNotFound, err)
	s.Equal(consts.ErrActivityTaskNotFound.Error(), err.Error())

	env.Logger.Info("Waiting for workflow to complete", tag.WorkflowRunID(we.RunId))

	s.False(workflowComplete)
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
}

// TestWFASAAHeartbeatTimeout ports the core of TestActivityHeartBeatWorkflow_Timeout above: a started attempt
// heartbeats nothing within its HeartbeatTimeout and, with no retries left, the activity ends TIMED_OUT
// with the Heartbeat TimeoutType. Both subtests must reach the same status and type.
func (s *standaloneActivityTestSuite) TestWFASAAHeartbeatTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.HeartbeatElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1, HasHeartbeat: true}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

func (s *ActivityTestSuite) TestTryActivityCancellationFromWorkflow() {
	env := testcore.NewEnv(s.T())

	id := "functional-activity-cancellation-test"
	wt := "functional-activity-cancellation-test-type"
	tl := "functional-activity-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_timer"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))

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

		env.Logger.Info("Completing Workflow")

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
		for i := range 10 {
			env.Logger.Info("Heartbeating for activity", tag.ActivityID(task.ActivityId), tag.Counter(i))
			response, err := env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(),
				&workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: env.Namespace().String(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	cancelCh := make(chan struct{})
	go func() {
		env.Logger.Info("Trying to cancel the task in a different thread")
		// Send signal so that worker can send an activity cancel
		_, err1 := env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
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

	env.Logger.Info("Start activity.")
	err = poller.PollAndProcessActivityTask(false)
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	env.Logger.Info("Waiting for cancel to complete.", tag.WorkflowRunID(we.RunId))
	select {
	case <-cancelCh:
	case <-s.Context().Done():
		s.Fail("Test timed out for activity cancellation", s.Context().Err())
		return
	}
	s.True(activityCanceled, "Activity was not cancelled.")
	env.Logger.Info("Activity cancelled.", tag.WorkflowRunID(we.RunId))
}

// TestWFASAACancel ports the core of TestTryActivityCancellationFromWorkflow above: a running
// activity is cancel-requested, the worker acknowledges (RespondActivityTaskCanceled), and the activity
// ends CANCELED. WorkflowActivity is the oracle; both subtests must reach CANCELED. The RequestCancel
// event realizes differently per surface — SAA's direct RequestCancelActivityExecution RPC vs WFA's
// workflow-driven cancel (signal -> RequestCancelActivity) — which is exactly the driver's job to hide.
//
// Fidelity vs the original: covered — the cancel-then-acknowledge -> CANCELED path. Not covered: the
// original also asserts the workflow observed the cancellation (workflow-level, not the activity's
// cross-surface contract).
func (s *standaloneActivityTestSuite) TestWFASAACancel() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.RequestCancel}, {Kind: model.RespondCanceled}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1}}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

func (s *ActivityTestSuite) TestActivityCancellationNotStarted() {
	env := testcore.NewEnv(s.T())
	id := "functional-activity-notstarted-cancellation-test"
	wt := "functional-activity-notstarted-cancellation-test-type"
	tl := "functional-activity-notstarted-cancellation-test-taskqueue"
	identity := "worker1"
	activityName := "activity_notstarted"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecutionn", tag.WorkflowRunID(we.GetRunId()))

	activityCounter := int32(0)
	scheduleActivity := true
	requestCancellation := false
	activityScheduledID := int64(0)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if scheduleActivity {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))
			env.Logger.Info("Scheduling activity")
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
			env.Logger.Info("Requesting cancellation")
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
				Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
					ScheduledEventId: activityScheduledID,
				}},
			}}, nil
		}

		env.Logger.Info("Completing Workflow")
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err := poller.PollAndProcessWorkflowTask()
	s.True(err == nil || errors.Is(err, testcore.ErrNoTasks))

	// Send signal so that worker can send an activity cancel
	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	_, err = env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
	env := testcore.NewEnv(s.T())
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

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	wfId := "functional-test-heartbeat-details-during-retry"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	runId := workflowRun.GetRunID()

	describeWorkflowExecution := func() (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
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

	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err)

	s.NoError(err1)
}

// TestActivityHeartBeat_RecordIdentity verifies that the identity of the worker sending the heartbeat
// is recorded in pending activity info and returned in describe workflow API response. This happens
// only when the worker identity is not sent when a poller picks the task.
func (s *ActivityTestSuite) TestActivityHeartBeat_RecordIdentity() {
	env := testcore.NewEnv(s.T())
	id := "functional-heartbeat-identity-record"
	workerIdentity := "70df788a-b0b2-4113-a0d5-130f13889e35"
	activityName := "activity_timer"

	taskQueue := &taskqueuepb.TaskQueue{Name: "functional-heartbeat-identity-record-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample data")},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "functional-heartbeat-identity-record-type"},
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(60 * time.Second),
		Identity:            workerIdentity,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
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
		_, err := env.FrontendClient().RecordActivityTaskHeartbeat(s.Context(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
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
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            "", // Do not send the worker identity.
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              env.Logger,
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
		return env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
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
	env := testcore.NewEnv(s.T())
	activityInfo := make(chan activity.Info, 1)
	taskQueue := testcore.RandomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, env.SdkClient(), taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.NewString(),
		TaskQueue: taskQueue,
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotNil(t, description.PendingActivities[0].LastFailure)
		require.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = env.SdkClient().CompleteActivityByID(s.Context(), env.Namespace().String(), run.GetID(), run.GetRunID(), ai.ActivityID, nil, nil)
	s.NoError(err)

	// Ensure the activity is completed and the workflow is unblcked.
	s.NoError(run.Get(s.Context(), nil))
}

func (s *ActivityTestSuite) TestActivityTaskCompleteRejectCompletion() {
	env := testcore.NewEnv(s.T())
	activityInfo := make(chan activity.Info, 1)
	taskQueue := testcore.RandomizeStr(s.T().Name())
	w, wf := s.mockWorkflowWithErrorActivity(activityInfo, env.SdkClient(), taskQueue)
	s.NoError(w.Start())
	defer w.Stop()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        uuid.NewString(),
		TaskQueue: taskQueue,
	}
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, wf)
	s.NoError(err)
	ai := <-activityInfo
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotNil(t, description.PendingActivities[0].LastFailure)
		require.Equal(t, "mock error of an activity", description.PendingActivities[0].LastFailure.Message)
	},
		10*time.Second,
		500*time.Millisecond)

	err = env.SdkClient().CompleteActivity(s.Context(), ai.TaskToken, nil, nil)
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

func (s *ActivityClientTestSuite) TestActivity_AttemptsExceeded() {
	env := testcore.NewEnv(s.T())
	activityFunction := func(ctx context.Context) error {
		return errors.New("non-retryable-error") //nolint:err113
	}

	workflowFn := func(ctx workflow.Context) error {
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 1,
			},
		}), activityFunction).Get(ctx, nil)
		return err
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfID := testcore.RandomizeStr(s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)

	err = workflowRun.Get(s.Context(), nil)
	var wfExecutionError *temporal.WorkflowExecutionError
	s.ErrorAs(err, &wfExecutionError)
	var activityError *temporal.ActivityError
	s.ErrorAs(wfExecutionError, &activityError)
	s.Equal(enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, activityError.RetryState())
	var applicationErr *temporal.ApplicationError
	s.ErrorAs(activityError, &applicationErr)
	s.Equal("non-retryable-error", applicationErr.Message())

	history := env.GetHistory(string(env.Namespace()), &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()})
	s.ContainsHistory(`ActivityTaskFailed`, &historypb.History{Events: history})
}

// --- CHASM-activity (SAA) vs workflow-activity (WFA) equivalence ----------------------------
//
// For each behavior at the intersection of the two products, a test drives the same trace through both
// drivers as subtests: "WorkflowActivity" uses the workflow-activity driver (activity_utils.go) and
// "StandaloneActivity" uses the standalone-activity driver (activity_standalone_utils.go). Both assert
// the same expected public activity info (activityInfoProjection). WFA is the oracle: the
// WorkflowActivity subtest passing blesses the expectation, and the StandaloneActivity subtest passing
// proves the CHASM activity matches it. These live on standaloneActivityTestSuite because its env
// enables the standalone activity (WFA needs nothing special); driving both in one SAA-enabled env is
// why they sit here rather than on ActivityTestSuite.

// retryAfterFail: attempt 1 fails retryably, the backoff elapses, attempt 2 starts. The activity is
// then running its second attempt — no pending retry — so there is no current retry interval and no
// next-attempt schedule time.

func (s *standaloneActivityTestSuite) TestWFASAARetryAfterFail() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                2,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// backingOff: attempt 1 fails retryably, and we observe during the backoff window (before it elapses,
// so the next dispatch is still in the future). The retry is genuinely pending, so both the current
// retry interval and the next-attempt schedule time are populated — the case where C5 says the two
// products agree. The long interval keeps the window open across the describe. Expected fully green.

func (s *standaloneActivityTestSuite) TestWFASAABackingOff() {
	env := s.newTestEnv()
	backingOffInterval := 30 * time.Second
	trace := []model.Event{saaPoll, saaFailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   backingOffInterval,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: backingOffInterval}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: backingOffInterval}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// nextRetryDelayOverride: the worker fails with a next_retry_delay that overrides the policy backoff
// (policy 5s, override 30s); observed during the override-length window. Both products must honor the
// override identically — the resulting current retry interval is 30s, not the policy's 5s. Expected
// fully green.
func (s *standaloneActivityTestSuite) TestWFASAANextRetryDelayOverride() {
	env := s.newTestEnv()
	nextRetryDelayOverride := 30 * time.Second
	trace := []model.Event{saaPoll, saaFailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   nextRetryDelayOverride,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 5 * time.Second, nextRetryDelay: nextRetryDelayOverride}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 5 * time.Second, nextRetryDelay: nextRetryDelayOverride}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// firstAttemptStarted: a worker polls the first attempt, which is now running. No attempt has failed,
// so there is no current retry interval and no next-attempt schedule time. The baseline running-state
// equivalence. Expected fully green.
func (s *standaloneActivityTestSuite) TestWFASAAFirstAttemptStarted() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                1,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval sweeps NextAttemptScheduleTime and
// CurrentRetryInterval across the activity lifecycle, comparing SAA against WFA (the oracle) at each
// point. Each scenario drives the same trace through both surfaces and asserts the same public info.
// The running-state scenarios are the C5 divergence: WFA reports no pending retry while an attempt
// runs, whereas SAA leaks the preceding backoff's retry-scheduling metadata — so those SAA subtests
// are expected red until C5 is fixed. StartDelayPending and PausedDuringBackoff are standalone-only
// (WFA has no per-activity start delay, and the WFA driver has no operator pause).
func (s *standaloneActivityTestSuite) TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval() {
	env := s.newTestEnv()
	t := s.T()

	// both drives a trace through the WFA oracle and the SAA surface, asserting each reports want.
	both := func(t *testing.T, maxAttempts int32, retryInterval time.Duration, trace []model.Event, want activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: maxAttempts, retryInterval: retryInterval}
			require.Equal(t, want, h.driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: maxAttempts}, retryInterval: retryInterval}
			require.Equal(t, want, h.driveTrace(t, trace).projection(t))
		})
	}

	// First attempt within its start delay: the dispatch is pending in the future and is not a retry.
	t.Run("StartDelayPending", func(t *testing.T) {
		info := s.driveTrace(t, env, saaTrace{trace: []model.Event{}, startDelayed: true}).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no preceding backoff, so no retry interval.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1})
	})

	// Backing off before the retry dispatches: the retry is genuinely pending, so both the interval and
	// the next-attempt schedule time are populated. The case where the two products agree.
	t.Run("BackingOffBeforeRetry", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: saaDelayWindow, NextAttemptScheduleSet: true})
	})

	// Retry dispatched to matching but not yet polled: schedulable now, so no future dispatch time.
	t.Run("RetryQueuedNotStarted", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: saaDelayWindow})
	})

	// Retry attempt running with a further retry permitted: nothing pending (C5 — SAA leaks the backoff's
	// metadata here).
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Final attempt running with no retry remaining: nothing pending (C5 — SAA leaks metadata here too).
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, 2, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Completed after a retry: terminal, nothing pending.
	t.Run("Completed", func(t *testing.T) {
		trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll, saaComplete}
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: saaDelayWindow}
			require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: saaDelayWindow}
			require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
		})
	})

	// Paused while backing off. There is no next attempt set to occur, and it's not appropriate to
	// report the current retry interval since dispatch will not occur while paused.
	t.Run("PausedDuringBackoff", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaPause},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2})
	})
}
