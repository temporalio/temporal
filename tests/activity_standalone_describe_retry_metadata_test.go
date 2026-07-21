package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// DescribeActivityExecution reports NextAttemptScheduleTime and CurrentRetryInterval straight from the
// persisted last-attempt fields, with no gating on the activity's status or wall-clock time. So once an
// activity has retried, NextAttemptScheduleTime reports a past timestamp while the attempt is running,
// and CurrentRetryInterval echoes the interval that led to the current attempt rather than the next one
// / null; during an initial start delay it reports null even though a dispatch is pending in the
// future. TestDescribeRetrySchedulingMetadata drives the activity to each lifecycle state and asserts
// the intended (contract-compliant, WFA-aligned) value of both fields, failing until the fix.
//
//   - NextAttemptScheduleTime: the awaiting attempt's pending dispatch time while it is still in the
//     future (schedule_time+start_delay for the first attempt, complete_time+interval for a retry);
//     null once that time has passed, once running, and while paused.
//   - CurrentRetryInterval: while running, the next interval (or null if no retry remains); while
//     backing off, the current interval; null when the attempt is not a retry.

// saaActivity addresses one driven activity for follow-up RPCs.
type saaActivity struct {
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

const (
	saaRetryInterval = 5 * time.Second // long enough to observe a backoff window before the retry dispatches
	saaBackoffSettle = 2 * time.Second // slack so a wait outlasts the backoff's firing instant
	saaStartDelay    = time.Hour       // keeps the first attempt pending dispatch for the whole test
)

func (s *standaloneActivityTestSuite) TestDescribeRetrySchedulingMetadata() {
	env := s.newTestEnv()
	t := s.T()

	// First attempt within its start delay: the dispatch is pending in the future, and this is not a retry.
	t.Run("StartDelayPending", func(t *testing.T) {
		info := s.startWithStartDelay(t, env).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch; the next interval applies if it fails.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		info := s.start_Poll(t, env, 3).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState())
		require.Nil(t, info.GetNextAttemptScheduleTime(), "null while running")
		require.Equal(t, saaRetryInterval, info.GetCurrentRetryInterval().AsDuration(), "while running, the next interval")
	})

	// Backing off before the retry dispatches: the next dispatch is in the future. Correct today; a
	// regression guard, not a repro.
	t.Run("BackingOffBeforeRetry", func(t *testing.T) {
		info := s.start_Poll_FailRetryably(t, env, 3).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.True(t, info.GetNextAttemptScheduleTime().AsTime().After(time.Now()), "future retry dispatch time")
		require.Equal(t, saaRetryInterval, info.GetCurrentRetryInterval().AsDuration(), "while backing off, the current interval")
	})

	// Retry attempt running with a further retry permitted.
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		info := s.start_Poll_FailRetryably_RetryBackoffElapse_Poll(t, env, 3).GetInfo()
		require.EqualValues(t, 2, info.GetAttempt())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState())
		require.Nil(t, info.GetNextAttemptScheduleTime(), "null while running")
		require.Equal(t, saaRetryInterval, info.GetCurrentRetryInterval().AsDuration(), "while running, the next interval")
	})

	// Final attempt running with no retry remaining.
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		info := s.start_Poll_FailRetryably_RetryBackoffElapse_Poll(t, env, 2).GetInfo()
		require.EqualValues(t, 2, info.GetAttempt())
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, info.GetRunState())
		require.Nil(t, info.GetNextAttemptScheduleTime(), "null while running")
		require.Nil(t, info.GetCurrentRetryInterval(), "null when no retry remains")
	})
}

// startWithStartDelay starts an activity with a long start delay and describes it while it is still
// SCHEDULED and pending its first dispatch.
func (s *standaloneActivityTestSuite) startWithStartDelay(t *testing.T, env *standaloneActivityEnv) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startActivity(t, env, 0, saaStartDelay)
	return s.describeActivity(t, env, a)
}

// start_Poll starts an activity, polls its first attempt into STARTED, and describes it.
func (s *standaloneActivityTestSuite) start_Poll(t *testing.T, env *standaloneActivityEnv, maxAttempts int32) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startActivity(t, env, maxAttempts, 0)
	s.pollTask(t, env, a)
	return s.describeActivity(t, env, a)
}

// start_Poll_FailRetryably starts an activity, fails its first attempt retryably, and describes it
// while it is backing off before the retry dispatches (the interval is long enough to observe).
func (s *standaloneActivityTestSuite) start_Poll_FailRetryably(t *testing.T, env *standaloneActivityEnv, maxAttempts int32) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startActivity(t, env, maxAttempts, 0)
	s.pollTask(t, env, a)
	s.respondFailedRetryably(t, env, a)
	return s.describeActivity(t, env, a)
}

// start_Poll_FailRetryably_RetryBackoffElapse_Poll drives the activity through a full retry: the first
// attempt fails, the backoff elapses, and the second attempt is polled into STARTED. Describes it there.
func (s *standaloneActivityTestSuite) start_Poll_FailRetryably_RetryBackoffElapse_Poll(t *testing.T, env *standaloneActivityEnv, maxAttempts int32) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startActivity(t, env, maxAttempts, 0)
	s.pollTask(t, env, a)
	s.respondFailedRetryably(t, env, a)
	time.Sleep(saaRetryInterval + saaBackoffSettle)
	s.pollTask(t, env, a)
	return s.describeActivity(t, env, a)
}

// startActivity starts one activity under the given retry cap (0 = unlimited) and optional start delay,
// with a long backoff interval so backoff windows are observable. Copied from the SAA driver's start.
func (s *standaloneActivityTestSuite) startActivity(t *testing.T, env *standaloneActivityEnv, maxAttempts int32, startDelay time.Duration) *saaActivity {
	id := testcore.RandomizeStr(t.Name())
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(saaRetryInterval),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(saaRetryInterval),
			MaximumAttempts:    maxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	if startDelay > 0 {
		req.StartDelay = durationpb.New(startDelay)
	}
	resp, err := env.FrontendClient().StartActivityExecution(s.Context(), req)
	require.NoError(t, err)
	return &saaActivity{activityID: id, taskQueue: id, runID: resp.RunId}
}

// pollTask dispatches the pending task to a poller, capturing its token (SCHEDULED -> STARTED).
// Copied from the SAA driver's pollForTask.
func (s *standaloneActivityTestSuite) pollTask(t *testing.T, env *standaloneActivityEnv, a *saaActivity) {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
	a.token = resp.GetTaskToken()
}

// respondFailedRetryably fails the current attempt with a retryable application error. Copied from the
// SAA driver's RespondFailed.
func (s *standaloneActivityTestSuite) respondFailedRetryably(t *testing.T, env *standaloneActivityEnv, a *saaActivity) {
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: a.token,
		Identity:  "worker",
		Failure: &failurepb.Failure{
			Message:     "drive",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: false}},
		},
	})
	require.NoError(t, err)
}

// describeActivity returns the activity's public DescribeActivityExecution response. Copied from the
// SAA driver's describe.
func (s *standaloneActivityTestSuite) describeActivity(t *testing.T, env *standaloneActivityEnv, a *saaActivity) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: a.activityID,
		RunId:      a.runID,
	})
	require.NoError(t, err)
	return resp
}
