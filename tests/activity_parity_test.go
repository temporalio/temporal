package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A StartToClose or Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes
// must fail the activity terminally (TimedOut) when it fires, rather than retrying.
func (s *standaloneActivityTestSuite) TestParityNonRetryableTimeout() {
	env := s.newTestEnv()
	t := s.T()

	both := func(t *testing.T, drive func(driver, *testing.T) enumspb.ActivityExecutionStatus) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, drive(&wfaDriver{s: s, env: env}, t),
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, drive(&saaDriver{s: s, env: env}, t),
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
	}

	t.Run("StartToClose", func(t *testing.T) {
		both(t, func(d driver, t *testing.T) enumspb.ActivityExecutionStatus {
			return d.start_Poll_StartToCloseTimeoutElapses(t)
		})
	})
	t.Run("Heartbeat", func(t *testing.T) {
		both(t, func(d driver, t *testing.T) enumspb.ActivityExecutionStatus {
			return d.start_Poll_HeartbeatTimeoutElapses(t)
		})
	})
}

// A retryable ServerFailure reported through RespondActivityTaskFailedById must be retried (a second
// attempt scheduled), not treated as terminal. WFA schedules attempt 2; SAA must do the same.
func (s *standaloneActivityTestSuite) TestParityByIDServerFailureIsRetried() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("WorkflowActivity", func(t *testing.T) {
		require.True(t, (&wfaDriver{s: s, env: env}).start_Poll_FailByIDWithRetryableServerFailure(t),
			"a retryable ServerFailure reported by ID must schedule another attempt, not fail terminally")
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.True(t, (&saaDriver{s: s, env: env}).start_Poll_FailByIDWithRetryableServerFailure(t),
			"a retryable ServerFailure reported by ID must schedule another attempt, not fail terminally")
	})
}

// retryableServerFailure builds a ServerFailure whose non_retryable flag is unset (i.e. retryable).
func retryableServerFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "transient server failure",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: false},
		},
	}
}

// pollForSecondAttempt reports whether a second activity task (attempt 2) is dispatched on the task
// queue, indicating the prior failure was retried rather than treated as terminal.
func pollForSecondAttempt(t *testing.T, s *standaloneActivityTestSuite, env *standaloneActivityEnv, taskQueue string) bool {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	if err != nil || resp.GetActivityId() == "" {
		return false
	}
	require.Equal(t, int32(2), resp.GetAttempt(), "dispatched task should be the second attempt")
	return true
}

// driver is the interface implemented by the WFA and SAA drivers.
type driver interface {
	start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_FailByIDWithRetryableServerFailure(t *testing.T) (retried bool)
}

// reproTimeout is the timeout under test, kept short so it fires within the test.
const reproTimeout = 2 * time.Second

// --- standalone-activity driver ------------------------------------------------------------

// saaDriver drives one standalone activity through the frontend RPCs.
type saaDriver struct {
	s   *standaloneActivityTestSuite
	env *standaloneActivityEnv

	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

func (d *saaDriver) start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus { //nolint:staticcheck // ST1003: underscores
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	d.pollTask(t)
	d.pollActivityExecution(t)
	return d.describeActivity(t).GetInfo().GetStatus()
}

func (d *saaDriver) start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus { //nolint:staticcheck // ST1003: underscores
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	d.pollTask(t)
	d.pollActivityExecution(t)
	return d.describeActivity(t).GetInfo().GetStatus()
}

func (d *saaDriver) start_Poll_FailByIDWithRetryableServerFailure(t *testing.T) bool { //nolint:staticcheck // ST1003: underscores
	d.startForRetryableFailure(t)
	d.pollTask(t)
	_, err := d.env.FrontendClient().RespondActivityTaskFailedById(d.s.Context(), &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  d.env.Namespace().String(),
		RunId:      d.runID,
		ActivityId: d.activityID,
		Failure:    retryableServerFailure(),
		Identity:   "worker",
	})
	require.NoError(t, err)
	return pollForSecondAttempt(t, d.s, d.env, d.taskQueue)
}

// startForRetryableFailure starts an activity with a long timeout and a retry budget so a retryable
// failure schedules another attempt.
func (d *saaDriver) startForRetryableFailure(t *testing.T) {
	id := testcore.RandomizeStr(t.Name())
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(200 * time.Millisecond),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(200 * time.Millisecond),
			MaximumAttempts:    3,
		},
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

// pollActivityExecution long-polls until the activity reaches a terminal outcome.
func (d *saaDriver) pollActivityExecution(t *testing.T) *workflowservice.PollActivityExecutionResponse {
	resp, err := d.env.FrontendClient().PollActivityExecution(d.s.Context(), &workflowservice.PollActivityExecutionRequest{
		Namespace:  d.env.Namespace().String(),
		ActivityId: d.activityID,
		RunId:      d.runID,
	})
	require.NoError(t, err)
	return resp
}

// startWithNonRetryableTimeout starts an activity with the given timeout short and listed in
// NonRetryableErrorTypes; every other timeout is long.
func (d *saaDriver) startWithNonRetryableTimeout(t *testing.T, timeoutType enumspb.TimeoutType) {
	id := testcore.RandomizeStr(t.Name())
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(200 * time.Millisecond),
			BackoffCoefficient:     1.0,
			MaximumInterval:        durationpb.New(200 * time.Millisecond),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()},
		},
		RequestId: uuid.NewString(),
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		req.StartToCloseTimeout = durationpb.New(reproTimeout)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		req.HeartbeatTimeout = durationpb.New(reproTimeout)
	default:
		t.Fatalf("unsupported timeout type %v", timeoutType)
	}
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), req)
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

// pollTask fetches a pending activity task, capturing its token.
func (d *saaDriver) pollTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(d.s.Context(), 10*time.Second)
	defer cancel()
	resp, err := d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: d.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
	d.token = resp.GetTaskToken()
}

// describeActivity returns the DescribeActivityExecution response.
func (d *saaDriver) describeActivity(t *testing.T) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := d.env.FrontendClient().DescribeActivityExecution(d.s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  d.env.Namespace().String(),
		ActivityId: d.activityID,
		RunId:      d.runID,
	})
	require.NoError(t, err)
	return resp
}

// --- workflow-activity driver --------------------------------------------------------------

// wfaDriver drives one activity scheduled by a helper workflow.
type wfaDriver struct {
	s   *standaloneActivityTestSuite
	env *standaloneActivityEnv

	run        sdkclient.WorkflowRun
	activityTQ string
	token      []byte
}

func (d *wfaDriver) start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus { //nolint:staticcheck // ST1003: underscores
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	d.pollTask(t)
	return d.awaitTerminalStatus(t)
}

func (d *wfaDriver) start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus { //nolint:staticcheck // ST1003: underscores
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	d.pollTask(t)
	return d.awaitTerminalStatus(t)
}

func (d *wfaDriver) start_Poll_FailByIDWithRetryableServerFailure(t *testing.T) bool { //nolint:staticcheck // ST1003: underscores
	d.startForRetryableFailure(t)
	d.pollTask(t)
	_, err := d.env.FrontendClient().RespondActivityTaskFailedById(d.s.Context(), &workflowservice.RespondActivityTaskFailedByIdRequest{
		Namespace:  d.env.Namespace().String(),
		WorkflowId: d.run.GetID(),
		RunId:      d.run.GetRunID(),
		ActivityId: "act",
		Failure:    retryableServerFailure(),
		Identity:   "worker",
	})
	require.NoError(t, err)
	return pollForSecondAttempt(t, d.s, d.env, d.activityTQ)
}

// startForRetryableFailure starts a workflow that schedules one activity with a long timeout and a retry
// budget so a retryable failure schedules another attempt.
func (d *wfaDriver) startForRetryableFailure(t *testing.T) {
	wfTQ := testcore.RandomizeStr("parity-wf")
	d.activityTQ = testcore.RandomizeStr("parity-act")

	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(singleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	run, err := d.env.SdkClient().ExecuteWorkflow(d.s.Context(),
		sdkclient.StartWorkflowOptions{ID: testcore.RandomizeStr("parity-run"), TaskQueue: wfTQ},
		singleActivityWorkflow, workflowActivityParams{
			TaskQueue:    d.activityTQ,
			StartToClose: time.Hour,
			MaxAttempts:  3,
		})
	require.NoError(t, err)
	d.run = run
}

// startWithNonRetryableTimeout starts a workflow that schedules one activity with the given timeout short
// and listed in NonRetryableErrorTypes.
func (d *wfaDriver) startWithNonRetryableTimeout(t *testing.T, timeoutType enumspb.TimeoutType) {
	wfTQ := testcore.RandomizeStr("parity-wf")
	d.activityTQ = testcore.RandomizeStr("parity-act")

	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(singleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	p := workflowActivityParams{
		TaskQueue:              d.activityTQ,
		StartToClose:           time.Hour,
		MaxAttempts:            3,
		NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()},
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		p.StartToClose = reproTimeout
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		p.Heartbeat = reproTimeout
	default:
		t.Fatalf("unsupported timeout type %v", timeoutType)
	}
	run, err := d.env.SdkClient().ExecuteWorkflow(d.s.Context(),
		sdkclient.StartWorkflowOptions{ID: testcore.RandomizeStr("parity-run"), TaskQueue: wfTQ},
		singleActivityWorkflow, p)
	require.NoError(t, err)
	d.run = run
}

// pollTask fetches a pending activity task, capturing its token.
func (d *wfaDriver) pollTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(d.s.Context(), 10*time.Second)
	defer cancel()
	resp, err := d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: d.activityTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
	d.token = resp.GetTaskToken()
}

// awaitTerminalStatus waits for the workflow to close and returns the activity's terminal status.
func (d *wfaDriver) awaitTerminalStatus(t *testing.T) enumspb.ActivityExecutionStatus {
	err := d.run.Get(d.s.Context(), nil)
	if err == nil {
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	}
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch actErr.Unwrap().(type) {
	case *temporal.TimeoutError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case *temporal.ApplicationError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	default:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	}
}

// workflowActivityParams configures the single activity the helper workflow schedules.
type workflowActivityParams struct {
	TaskQueue              string
	StartToClose           time.Duration
	Heartbeat              time.Duration // 0 = unset
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// singleActivityWorkflow is a workflow that schedules one activity with the given options and returns its result.
func singleActivityWorkflow(ctx workflow.Context, p workflowActivityParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           p.TaskQueue,
		ActivityID:          "act",
		StartToCloseTimeout: p.StartToClose,
		HeartbeatTimeout:    p.Heartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        200 * time.Millisecond,
			BackoffCoefficient:     1.0,
			MaximumInterval:        200 * time.Millisecond,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: p.NonRetryableErrorTypes,
		},
	})
	return workflow.ExecuteActivity(ctx, "noopActivity").Get(ctx, nil)
}
