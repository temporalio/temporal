package tests

// Driver for standalone-activity (SAA) tests: it starts an activity and drives it through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a wall-clock
// wait. The event vocabulary is in chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type saaDriver struct {
	env              *testcore.TestEnv
	ctx              context.Context
	cfg              activityConfig
	numStarted       int
	activityIDPrefix string
}

// newSAADriver builds a driver.
func newSAADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *saaDriver {
	return &saaDriver{
		env:              env,
		ctx:              testcontext.For(t),
		cfg:              cfg,
		activityIDPrefix: t.Name(),
	}
}

// saaHandle is a handle to an activity instance.
type saaHandle struct {
	d          *saaDriver
	cfg        activityConfig // d.cfg with the windows this trace needs; see activityConfig.forTrace
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

// driveTrace schedules an activity, and then advances that activity through a sequence of events (a
// 'trace'). Returns a handle to the activity at the reached state.
func (d *saaDriver) driveTrace(t require.TestingT, trace []model.Event) *saaHandle {
	validateTrace(t, trace)
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

// driveEvent advances the activity by one event.
func (a *saaHandle) driveEvent(t require.TestingT, e model.Event) {
	switch {
	case e.Type == model.PollType:
		// When a trace includes a poll event, the implication is that the activity should be
		// dispatchable and that the poll will yield an activity task, so finding no task is a
		// failure.
		resp := a.pollForTask(t, activityDriverPositivePollTimeout)
		require.NotNilf(t, resp, "%s: no task was dispatched within %s", e, activityDriverPositivePollTimeout)
		a.token = resp.GetTaskToken()
	case isWallClockEvent(e.Type):
		// A wall-clock event is realized by waiting out its configured window.
		a.awaitWallClock(t, e)
	default:
		require.NoError(t, a.rpc(e))
	}
}

// awaitWallClock blocks until a wall-clock event's effect is visible, and fails if it does not
// become visible within (window + settle).
func (a *saaHandle) awaitWallClock(t require.TestingT, e model.Event) {
	if isDispatchDelayEvent(e.Type) {
		a.awaitDispatchTimePassed(t, e)
		return
	}
	a.awaitStateTransition(t, e, time.Now().Add(a.cfg.window(e)+activityDriverWallClockSettle))
}

// awaitStateTransition long-polls DescribeActivityExecution until the transition-history version
// advances past the token's, and fails if none does by the deadline. An empty response means the
// server's long-poll window expired, so resubmit.
func (a *saaHandle) awaitStateTransition(t require.TestingT, e model.Event, deadline time.Time) {
	token := a.describe(t).GetLongPollToken()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.d.ctx, deadline)
		resp, err := a.d.env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:     a.d.env.Namespace().String(),
			ActivityId:    a.activityID,
			RunId:         a.runID,
			LongPollToken: token,
		})
		cancel()
		if err != nil {
			if time.Now().Before(deadline) {
				require.NoError(t, err)
			}
			break // the deadline cancelled the long poll
		}
		if resp.GetInfo() != nil {
			return // non-empty: the state advanced
		}
	}
	t.Errorf("%s: the activity did not transition within %s of driving the event, so the event did not take "+
		"effect. Last observed: %+v", e, a.cfg.window(e)+activityDriverWallClockSettle, a.activityInfo(t))
}

// awaitDispatchTimePassed polls the activity until the dispatch time has passed, and fails if it
// has not.
func (a *saaHandle) awaitDispatchTimePassed(t require.TestingT, e model.Event) {
	info := a.describe(t).GetInfo()
	next := info.GetNextAttemptScheduleTime()
	if next == nil {
		return // the dispatch time has already passed
	}
	deadline := next.AsTime().Add(activityDriverWallClockSettle)
	p := saaActivityInfo(info)
	if activityDriverPollUntil(deadline, func() bool { p = a.activityInfo(t); return !p.NextAttemptScheduleTimeSet }) {
		return
	}
	t.Errorf("%s: a dispatch is still pending %s after the time the server scheduled it for, so the "+
		"window did not elapse. Last observed: %+v", e, activityDriverWallClockSettle, p)
}

func (d *saaDriver) start(t require.TestingT, cfg activityConfig) *saaHandle {
	d.numStarted++
	id := fmt.Sprintf("%s-%d", d.activityIDPrefix, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(cfg, id, id))
	require.NoError(t, err)
	return &saaHandle{d: d, cfg: cfg, activityID: id, taskQueue: id, runID: resp.RunId}
}

func (d *saaDriver) startRequest(c activityConfig, activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	opt := func(v time.Duration) *durationpb.Duration {
		if v == 0 {
			return nil
		}
		return durationpb.New(v)
	}
	return &workflowservice.StartActivityExecutionRequest{
		Namespace:              d.env.Namespace().String(),
		ActivityId:             activityID,
		ActivityType:           d.env.Tv().ActivityType(),
		Identity:               d.env.Tv().ClientIdentity(),
		Input:                  activityParityDefaultInput,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:    durationpb.New(c.startToClose()),
		ScheduleToCloseTimeout: opt(c.ScheduleToClose),
		ScheduleToStartTimeout: opt(c.ScheduleToStart),
		HeartbeatTimeout:       opt(c.Heartbeat),
		StartDelay:             opt(c.StartDelay),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(c.retryInterval()),
			BackoffCoefficient:     cmp.Or(c.BackoffCoefficient, 1.0),
			MaximumInterval:        durationpb.New(cmp.Or(c.MaxRetryInterval, c.retryInterval())),
			MaximumAttempts:        c.MaxAttempts,
			NonRetryableErrorTypes: c.NonRetryableErrorTypes,
		},
		RequestId: uuid.NewString(),
	}
}

// describe returns the DescribeActivityExecution response, including the outcome, the last failure,
// and the heartbeat details.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.d.ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:               a.d.env.Namespace().String(),
		ActivityId:              a.activityID,
		RunId:                   a.runID,
		IncludeOutcome:          true,
		IncludeLastFailure:      true,
		IncludeHeartbeatDetails: true,
	})
	require.NoError(t, err)
	return resp
}

// activityInfo is the activity's ActivityExecutionInfo, projected down to a schema shared with
// workflow activity.
func (a *saaHandle) activityInfo(t require.TestingT) activityInfo {
	return saaActivityInfo(a.describe(t).GetInfo())
}

// terminalStatus waits for the activity to reach a terminal state and reports it.
// PollActivityExecution resolves once the activity is no longer running. An empty response means the
// server's long-poll window expired, so resubmit.
func (a *saaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	deadline := time.Now().Add(activityDriverTerminalTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.d.ctx, deadline)
		resp, err := a.d.env.FrontendClient().PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
			Namespace:  a.d.env.Namespace().String(),
			ActivityId: a.activityID,
			RunId:      a.runID,
		})
		cancel()
		if err != nil {
			if time.Now().Before(deadline) {
				require.NoError(t, err)
			}
			break // the deadline cancelled the long poll
		}
		if resp.GetRunId() != "" {
			return a.describe(t).GetInfo().GetStatus()
		}
	}
	t.Errorf("the activity did not reach a terminal status within %s of the trace finishing. Last observed: %+v",
		activityDriverTerminalTimeout, a.activityInfo(t))
	return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
}

func saaActivityInfo(i *apiactivitypb.ActivityExecutionInfo) activityInfo {
	return activityInfo{
		RunState:                   i.GetRunState(),
		Attempt:                    i.GetAttempt(),
		CurrentRetryInterval:       i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: i.GetNextAttemptScheduleTime() != nil,
	}
}

// rpc performs the frontend RPC for a non-Poll, non-wall-clock event and returns its error.
func (a *saaHandle) rpc(e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.RespondFailedType:
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: activityFailure(e.Retryable, a.cfg.NextRetryDelay),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	default:
		return fmt.Errorf("saaDriver: unhandled event type %v", e.Type)
	}
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	ctx, cancel := context.WithTimeout(a.d.ctx, timeout)
	defer cancel()
	resp, err := a.d.env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: a.d.env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue},
		Identity:  a.d.env.Tv().WorkerIdentity(),
	})
	// Matching signals "waited, found nothing" with an empty response and a nil error, so any error
	// means the poll did not complete cleanly.
	if err != nil {
		if a.d.ctx.Err() != nil {
			return nil // teardown
		}
		if deadline, ok := a.d.ctx.Deadline(); ok && time.Until(deadline) < common.MinLongPollTimeout {
			t.Errorf("saaDriver: test context budget exhausted before the poll could run (%.1fs left, need >= %s). "+
				"Raise TEMPORAL_TEST_TIMEOUT and `go test -timeout`.\n  %v",
				time.Until(deadline).Seconds(), common.MinLongPollTimeout, err)
			return nil
		}
		t.Errorf("saaDriver bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // no task available
	}
	return resp
}
