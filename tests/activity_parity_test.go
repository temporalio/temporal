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
	"go.temporal.io/server/common/testing/await"
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

// A RespondActivityTaskFailed with an omitted Failure must be treated as retryable (parity with WFA),
// scheduling attempt 2 rather than closing the activity with no consumable outcome.
func (s *standaloneActivityTestSuite) TestParityNilFailureRetryable() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("WorkflowActivity", func(t *testing.T) {
		d := &wfaDriver{s: s, env: env}
		require.EqualValues(t, 2, d.start_Poll_FailNilFailure_SecondAttempt(t),
			"a nil failure must be retryable and schedule attempt 2, not close the activity")
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		d := &saaDriver{s: s, env: env}
		require.EqualValues(t, 2, d.start_Poll_FailNilFailure_SecondAttempt(t),
			"a nil failure must be retryable and schedule attempt 2, not close the activity")
	})
}

// driver is the interface implemented by the WFA and SAA drivers.
type driver interface {
	start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_FailNilFailure_SecondAttempt(t *testing.T) int32
}

// reproTimeout is the timeout under test, kept short so it fires within the test.
const reproTimeout = 2 * time.Second

// current_retry_interval and next_attempt_schedule_time are reported while a retry is backing off
// (before it is dispatched to Matching), and for next_attempt_schedule_time also during start delay
// (SAA only). Once the attempt is dispatched, or while the activity is paused, both are nil.
func (s *standaloneActivityTestSuite) TestParityCurrentRetryInterval() {
	env := s.newTestEnv()
	t := s.T()

	both := func(t *testing.T, want activityInfoProjection, drive func(retryDriver, *testing.T) activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, want, drive(&wfaDriver{s: s, env: env}, t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, want, drive(&saaDriver{s: s, env: env}, t))
		})
	}

	// First attempt within its start delay (SAA only): the pending dispatch is in the future and is
	// not a retry.
	t.Run("StartDelayPending", func(t *testing.T) {
		d := &saaDriver{s: s, env: env}
		d.startWithStartDelay(t, startDelay)
		info := d.describeActivity(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
			Attempt: 1,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_ObserveRunning(t)
		})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	t.Run("BackingOff", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			Attempt:                2,
			CurrentRetryInterval:   backingOffInterval,
			NextAttemptScheduleSet: true,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_ObserveBackingOff(t)
		})
	})

	// Backing off after a worker-supplied next_retry_delay: the reported interval is the worker's
	// override.
	t.Run("NextRetryDelayOverride", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			Attempt:                2,
			CurrentRetryInterval:   nextRetryDelayOverride,
			NextAttemptScheduleSet: true,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailWithNextRetryDelay_ObserveBackingOff(t, nextRetryDelayOverride)
		})
	})

	// Retry dispatched to Matching but not yet polled: both fields are nil.
	t.Run("RetryDispatched", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			Attempt: 2,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_RetryDispatched(t)
		})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
			Attempt: 2,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_BackoffElapses_Poll_ObserveRunning(t, 3)
		})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
			Attempt: 2,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_BackoffElapses_Poll_ObserveRunning(t, 2)
		})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	t.Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
			Attempt: 2,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_Paused(t)
		})
	})

	// Paused after the retry was dispatched: the dispatched code path already nils both fields, and the
	// pause preserves that.
	t.Run("PausedAfterDispatch", func(t *testing.T) {
		both(t, activityInfoProjection{
			State:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
			Attempt: 2,
		}, func(d retryDriver, t *testing.T) activityInfoProjection {
			return d.start_Poll_FailRetryably_BackoffElapses_Paused(t)
		})
	})
}

// retryDriver drives an activity to a retry-backoff state and reports its public retry-scheduling info.
type retryDriver interface {
	start_Poll_ObserveRunning(t *testing.T) activityInfoProjection
	start_Poll_FailRetryably_ObserveBackingOff(t *testing.T) activityInfoProjection
	start_Poll_FailWithNextRetryDelay_ObserveBackingOff(t *testing.T, nextRetryDelay time.Duration) activityInfoProjection
	start_Poll_FailRetryably_RetryDispatched(t *testing.T) activityInfoProjection
	start_Poll_FailRetryably_BackoffElapses_Poll_ObserveRunning(t *testing.T, maxAttempts int32) activityInfoProjection
	start_Poll_FailRetryably_BackoffElapses_Paused(t *testing.T) activityInfoProjection
	start_Poll_FailRetryably_Paused(t *testing.T) activityInfoProjection
}

// activityInfoProjection is the slice of an activity's public info this suite compares across the two
// surfaces: run state, attempt, and the retry-scheduling metadata. CurrentRetryInterval is rounded to
// the second (WFA derives it by subtracting two stored timestamps; SAA stores it exactly).
// NextAttemptScheduleTime is compared by set-ness, since its absolute value differs run to run.
type activityInfoProjection struct {
	State                  enumspb.PendingActivityState
	Attempt                int32
	CurrentRetryInterval   time.Duration
	NextAttemptScheduleSet bool
}

// retryDispatched reports that the retry has been dispatched to Matching (attempt 2, scheduled, with no
// pending future dispatch) — the state reached once the backoff elapses.
func retryDispatched(p activityInfoProjection) bool {
	return p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED && p.Attempt == 2 && !p.NextAttemptScheduleSet
}

const (
	// backingOffInterval is long enough to observe an activity while it is still backing off.
	backingOffInterval = 30 * time.Second
	// dispatchInterval is short enough that the backoff elapses and the retry dispatches within the test.
	dispatchInterval = 1 * time.Second
	// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from backingOffInterval so
	// the reported interval cannot be confused with the policy's.
	nextRetryDelayOverride = 10 * time.Second
	// startDelay keeps a first attempt pending dispatch for the whole test.
	startDelay = time.Hour
)

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

func (d *saaDriver) start_Poll_FailNilFailure_SecondAttempt(t *testing.T) int32 { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 2)
	d.pollTask(t) // attempt 1
	d.respondFailedNilFailure(t)
	return d.pollAttempt(t)
}

// respondFailedNilFailure fails the current attempt with an omitted Failure.
func (d *saaDriver) respondFailedNilFailure(t *testing.T) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(),
		TaskToken: d.token,
		Identity:  "worker",
		// Failure intentionally omitted.
	})
	require.NoError(t, err)
}

// pollAttempt polls for a redispatched task and returns its attempt number, or 0 if none is dispatched.
func (d *saaDriver) pollAttempt(t *testing.T) int32 {
	ctx, cancel := context.WithTimeout(d.s.Context(), 10*time.Second)
	defer cancel()
	resp, err := d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: d.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	if err != nil || resp.GetActivityId() == "" {
		return 0
	}
	return resp.GetAttempt()
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

// describeActivity returns the DescribeActivityExecution response. Takes require.TestingT so it can be
// driven either by the test's *testing.T or by an *await.T inside an await.Require poll.
func (d *saaDriver) describeActivity(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := d.env.FrontendClient().DescribeActivityExecution(d.s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  d.env.Namespace().String(),
		ActivityId: d.activityID,
		RunId:      d.runID,
	})
	require.NoError(t, err)
	return resp
}

func (d *saaDriver) start_Poll_ObserveRunning(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t) // returns only once the start is recorded, so a single describe already sees STARTED
	return d.observe(t)
}

func (d *saaDriver) start_Poll_FailRetryably_ObserveBackingOff(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t)
	d.failRetryably(t) // synchronous: the reschedule to backing off is committed before it returns
	return d.observe(t)
}

func (d *saaDriver) start_Poll_FailWithNextRetryDelay_ObserveBackingOff(t *testing.T, nextRetryDelay time.Duration) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t)
	d.failWithNextRetryDelay(t, nextRetryDelay) // synchronous: the reschedule is committed before it returns
	return d.observe(t)
}

func (d *saaDriver) start_Poll_FailRetryably_RetryDispatched(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 3)
	d.pollTask(t)
	d.failRetryably(t)
	return d.awaitObserve(t, retryDispatched)
}

func (d *saaDriver) start_Poll_FailRetryably_BackoffElapses_Poll_ObserveRunning(t *testing.T, maxAttempts int32) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, maxAttempts)
	d.pollTask(t)
	d.failRetryably(t)
	d.pollTask(t) // long-poll: blocks until the retry dispatches and the next attempt's start is recorded
	return d.observe(t)
}

func (d *saaDriver) start_Poll_FailRetryably_Paused(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3) // long, so the pause lands well before the retry dispatches
	d.pollTask(t)
	d.failRetryably(t)  // synchronous: rescheduled to backing off before it returns
	d.pauseActivity(t)  // synchronous: paused before it returns
	return d.observe(t) // so a single describe sees the paused, backing-off activity
}

func (d *saaDriver) start_Poll_FailRetryably_BackoffElapses_Paused(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 3)
	d.pollTask(t)
	d.failRetryably(t)
	d.awaitObserve(t, retryDispatched) // projection poll: the backoff elapsing bumps no version to long-poll on
	d.pauseActivity(t)                 // synchronous
	return d.observe(t)
}

// startRequest builds a start request for an activity whose failures are retryable, with a constant
// backoff of retryInterval.
func (d *saaDriver) startRequest(id string, retryInterval time.Duration, maxAttempts int32) *workflowservice.StartActivityExecutionRequest {
	return &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(retryInterval),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(retryInterval),
			MaximumAttempts:    maxAttempts,
		},
		RequestId: uuid.NewString(),
	}
}

func (d *saaDriver) startRetryable(t *testing.T, retryInterval time.Duration, maxAttempts int32) {
	id := testcore.RandomizeStr(t.Name())
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), d.startRequest(id, retryInterval, maxAttempts))
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

// startWithStartDelay starts an activity with a start delay so the first attempt stays pending dispatch.
func (d *saaDriver) startWithStartDelay(t *testing.T, startDelay time.Duration) {
	id := testcore.RandomizeStr(t.Name())
	req := d.startRequest(id, backingOffInterval, 3)
	req.StartDelay = durationpb.New(startDelay)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), req)
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

func (d *saaDriver) failRetryably(t *testing.T) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(), TaskToken: d.token, Identity: "worker", Failure: retryableFailure(),
	})
	require.NoError(t, err)
}

func (d *saaDriver) failWithNextRetryDelay(t *testing.T, nextRetryDelay time.Duration) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(), TaskToken: d.token, Identity: "worker",
		Failure: retryableFailureWithNextRetryDelay(nextRetryDelay),
	})
	require.NoError(t, err)
}

func (d *saaDriver) pauseActivity(t *testing.T) {
	_, err := d.env.FrontendClient().PauseActivityExecution(d.s.Context(), &workflowservice.PauseActivityExecutionRequest{
		Namespace: d.env.Namespace().String(), ActivityId: d.activityID, RunId: d.runID, Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
}

// observe reads the activity's public retry-scheduling info as the shared projection.
func (d *saaDriver) observe(t require.TestingT) activityInfoProjection {
	i := d.describeActivity(t).GetInfo()
	return activityInfoProjection{
		State:                  i.GetRunState(),
		Attempt:                i.GetAttempt(),
		CurrentRetryInterval:   i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// awaitObserve polls the public projection until pred holds, returning that projection. Reserved for a
// dispatch-delay elapse (start-delay / backoff), whose only effect is a read-time projection flip with no
// transition-history version advance, so a Describe long-poll would never wake. Effects committed by a
// synchronous RPC (or already recorded by the time a Poll returns) need no wait — describe directly.
func (d *saaDriver) awaitObserve(t *testing.T, pred func(activityInfoProjection) bool) activityInfoProjection {
	var p activityInfoProjection
	await.Require(d.s.Context(), t, func(at *await.T) {
		p = d.observe(at)
		at.Require().Truef(pred(p), "last observed: %+v", p)
	}, 15*time.Second, 100*time.Millisecond)
	return p
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
		RetryInterval:          200 * time.Millisecond,
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

func (d *wfaDriver) start_Poll_ObserveRunning(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t) // returns only once the start is recorded, so a single describe already sees STARTED
	return d.observe(t)
}

func (d *wfaDriver) start_Poll_FailRetryably_ObserveBackingOff(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t)
	d.failRetryably(t) // synchronous: the reschedule to backing off is committed before it returns
	return d.observe(t)
}

func (d *wfaDriver) start_Poll_FailWithNextRetryDelay_ObserveBackingOff(t *testing.T, nextRetryDelay time.Duration) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3)
	d.pollTask(t)
	d.failWithNextRetryDelay(t, nextRetryDelay) // synchronous: the reschedule is committed before it returns
	return d.observe(t)
}

func (d *wfaDriver) start_Poll_FailRetryably_RetryDispatched(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 3)
	d.pollTask(t)
	d.failRetryably(t)
	return d.awaitObserve(t, retryDispatched)
}

func (d *wfaDriver) start_Poll_FailRetryably_BackoffElapses_Poll_ObserveRunning(t *testing.T, maxAttempts int32) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, maxAttempts)
	d.pollTask(t)
	d.failRetryably(t)
	d.pollTask(t) // long-poll: blocks until the retry dispatches and the next attempt's start is recorded
	return d.observe(t)
}

func (d *wfaDriver) start_Poll_FailRetryably_Paused(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, backingOffInterval, 3) // long, so the pause lands well before the retry dispatches
	d.pollTask(t)
	d.failRetryably(t)  // synchronous: rescheduled to backing off before it returns
	d.pauseActivity(t)  // synchronous: paused before it returns
	return d.observe(t) // so a single describe sees the paused, backing-off activity
}

func (d *wfaDriver) start_Poll_FailRetryably_BackoffElapses_Paused(t *testing.T) activityInfoProjection { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 3)
	d.pollTask(t)
	d.failRetryably(t)
	d.awaitObserve(t, retryDispatched) // projection poll: the backoff elapsing bumps no version to long-poll on
	d.pauseActivity(t)                 // synchronous
	return d.observe(t)
}

func (d *wfaDriver) start_Poll_FailNilFailure_SecondAttempt(t *testing.T) int32 { //nolint:staticcheck // ST1003: underscores
	d.startRetryable(t, dispatchInterval, 2)
	d.pollTask(t) // attempt 1
	d.respondFailedNilFailure(t)
	return d.pollAttempt(t)
}

// startRetryable starts a workflow that schedules one activity whose failures are retryable, with a
// constant backoff of retryInterval.
func (d *wfaDriver) startRetryable(t *testing.T, retryInterval time.Duration, maxAttempts int32) {
	wfTQ := testcore.RandomizeStr("parity-wf")
	d.activityTQ = testcore.RandomizeStr("parity-act")

	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(singleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	run, err := d.env.SdkClient().ExecuteWorkflow(d.s.Context(),
		sdkclient.StartWorkflowOptions{ID: testcore.RandomizeStr("parity-run"), TaskQueue: wfTQ},
		singleActivityWorkflow, workflowActivityParams{
			TaskQueue: d.activityTQ, StartToClose: time.Hour, RetryInterval: retryInterval, MaxAttempts: maxAttempts,
		})
	require.NoError(t, err)
	d.run = run
}

func (d *wfaDriver) failRetryably(t *testing.T) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(), TaskToken: d.token, Identity: "worker", Failure: retryableFailure(),
	})
	require.NoError(t, err)
}

// respondFailedNilFailure fails the current attempt with an omitted Failure.
func (d *wfaDriver) respondFailedNilFailure(t *testing.T) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(),
		TaskToken: d.token,
		Identity:  "worker",
		// Failure intentionally omitted.
	})
	require.NoError(t, err)
}

func (d *wfaDriver) failWithNextRetryDelay(t *testing.T, nextRetryDelay time.Duration) {
	_, err := d.env.FrontendClient().RespondActivityTaskFailed(d.s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: d.env.Namespace().String(), TaskToken: d.token, Identity: "worker",
		Failure: retryableFailureWithNextRetryDelay(nextRetryDelay),
	})
	require.NoError(t, err)
}

func (d *wfaDriver) pauseActivity(t *testing.T) {
	_, err := d.env.FrontendClient().PauseActivityExecution(d.s.Context(), &workflowservice.PauseActivityExecutionRequest{
		Namespace: d.env.Namespace().String(), WorkflowId: d.run.GetID(), RunId: d.run.GetRunID(), ActivityId: wfaActivityID,
		Identity: "op", Reason: "drive", RequestId: uuid.NewString(),
	})
	require.NoError(t, err)
}

// observe reads the activity's public retry-scheduling info via DescribeWorkflowExecution, as the shared
// projection. Takes require.TestingT so it works under an await.Require poll (see awaitObserve).
func (d *wfaDriver) observe(t require.TestingT) activityInfoProjection {
	resp, err := d.env.SdkClient().DescribeWorkflowExecution(d.s.Context(), d.run.GetID(), d.run.GetRunID())
	require.NoError(t, err)
	for _, pa := range resp.GetPendingActivities() {
		if pa.GetActivityId() == wfaActivityID {
			return activityInfoProjection{
				State:                  pa.GetState(),
				Attempt:                pa.GetAttempt(),
				CurrentRetryInterval:   pa.GetCurrentRetryInterval().AsDuration().Round(time.Second),
				NextAttemptScheduleSet: pa.GetNextAttemptScheduleTime() != nil,
			}
		}
	}
	require.Fail(t, "no pending activity", "activity %q not pending; workflow may have closed", wfaActivityID)
	return activityInfoProjection{}
}

// awaitObserve polls the public projection until pred holds, returning that projection. Reserved for a
// dispatch-delay elapse (start-delay / backoff), whose only effect is a read-time projection flip with no
// transition-history version advance, so a Describe long-poll would never wake. Effects committed by a
// synchronous RPC (or already recorded by the time a Poll returns) need no wait — describe directly.
func (d *wfaDriver) awaitObserve(t *testing.T, pred func(activityInfoProjection) bool) activityInfoProjection {
	var p activityInfoProjection
	await.Require(d.s.Context(), t, func(at *await.T) {
		p = d.observe(at)
		at.Require().Truef(pred(p), "last observed: %+v", p)
	}, 15*time.Second, 100*time.Millisecond)
	return p
}

// wfaActivityID is the fixed ID the helper workflow assigns its activity.
const wfaActivityID = "act"

// pollAttempt polls for a redispatched task and returns its attempt number, or 0 if none is dispatched.
func (d *wfaDriver) pollAttempt(t *testing.T) int32 {
	ctx, cancel := context.WithTimeout(d.s.Context(), 10*time.Second)
	defer cancel()
	resp, err := d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: d.activityTQ, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	if err != nil || resp.GetActivityId() == "" {
		return 0
	}
	return resp.GetAttempt()
}

// workflowActivityParams configures the single activity the helper workflow schedules.
type workflowActivityParams struct {
	TaskQueue              string
	StartToClose           time.Duration
	Heartbeat              time.Duration // 0 = unset
	RetryInterval          time.Duration // InitialInterval == MaximumInterval (constant backoff)
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// singleActivityWorkflow is a workflow that schedules one activity with the given options and returns its result.
func singleActivityWorkflow(ctx workflow.Context, p workflowActivityParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           p.TaskQueue,
		ActivityID:          wfaActivityID,
		StartToCloseTimeout: p.StartToClose,
		HeartbeatTimeout:    p.Heartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        p.RetryInterval,
			BackoffCoefficient:     1.0,
			MaximumInterval:        p.RetryInterval,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: p.NonRetryableErrorTypes,
		},
	})
	return workflow.ExecuteActivity(ctx, "noopActivity").Get(ctx, nil)
}

// retryableFailure is the worker-reported failure the drivers use to trigger a retry.
func retryableFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive"},
		},
	}
}

// retryableFailureWithNextRetryDelay is a retryable failure carrying a worker-supplied next_retry_delay
// that overrides the policy backoff for the next attempt.
func retryableFailureWithNextRetryDelay(nextRetryDelay time.Duration) *failurepb.Failure {
	return &failurepb.Failure{
		Message: "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NextRetryDelay: durationpb.New(nextRetryDelay)},
		},
	}
}
