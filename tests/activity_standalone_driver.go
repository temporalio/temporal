package tests

// Driver for standalone-activity (SAA) tests: it starts an activity and drives it through a
// sequence of events (a 'trace'). Each event is either a frontend RPC, a poll, or a timer
// wait. The event vocabulary is in chasm/lib/activity/model.

import (
	"cmp"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	activityDriverState
	d          *saaDriver
	activityID string
	runID      string
	taskQueue  string
}

// driveTrace schedules an activity, and then advances that activity through a sequence of events (a
// 'trace'). Returns a handle to the activity at the reached state.
func (d *saaDriver) driveTrace(t testing.TB, trace []model.Event) *saaHandle {
	validateTrace(t, trace)
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

func (a *saaHandle) driveEvent(t testing.TB, e model.Event) {
	driveActivityEvent(t, a, e)
}

func (a *saaHandle) awaitTimeout(t testing.TB, e model.Event, deadline time.Time) {
	awaitActivityTimeout(t, a, e, deadline)
}

// timeoutInfo is the most recent timeout the activity reports.
func (a *saaHandle) timeoutInfo(t require.TestingT) activityTimeoutInfo {
	response := a.describe(t)
	info := response.GetInfo()
	timeout := info.GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType()
	if timeout == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		// Per-attempt timeouts are reported as LastFailure. Schedule timeouts close the activity
		// directly and are reported only in the terminal Outcome.
		timeout = response.GetOutcome().GetFailure().GetTimeoutFailureInfo().GetTimeoutType()
	}
	return activityTimeoutInfo{
		timeout:  timeout,
		attempt:  info.GetAttempt(),
		terminal: info.GetStatus() != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
	}
}

// awaitDispatchDelay waits for the public dispatch deadline to become due. A following Poll is what
// proves that the task actually reached Matching.
func (a *saaHandle) awaitDispatchDelay(t testing.TB, e model.Event) {
	awaitActivityDispatchDelay(a.d.ctx, t, e, func(t require.TestingT) (bool, enumspb.PendingActivityState, *timestamppb.Timestamp, any) {
		info := a.describe(t).GetInfo()
		return info.GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			info.GetRunState(),
			info.GetNextAttemptScheduleTime(),
			saaActivityInfo(info)
	})
}

func (d *saaDriver) start(t require.TestingT, cfg activityConfig) *saaHandle {
	d.numStarted++
	id := fmt.Sprintf("%s-%d", d.activityIDPrefix, d.numStarted)
	resp, err := d.env.FrontendClient().StartActivityExecution(d.ctx, d.startRequest(cfg, id, id))
	require.NoError(t, err)
	return &saaHandle{
		activityDriverState: activityDriverState{ctx: d.ctx, cfg: cfg},
		d:                   d,
		activityID:          id,
		runID:               resp.RunId,
		taskQueue:           id,
	}
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
		Input:                  payloads.EncodeString(activityInput),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:    durationpb.New(c.startToClose()),
		ScheduleToCloseTimeout: opt(c.ScheduleToClose),
		ScheduleToStartTimeout: opt(c.ScheduleToStart),
		HeartbeatTimeout:       opt(c.HeartbeatTimeout),
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

// terminalOutcome waits for the activity to reach a terminal state and reports it.
// PollActivityExecution resolves once the activity is no longer running. An empty response means the
// server's long-poll window expired, so resubmit.
func (a *saaHandle) terminalOutcome(t require.TestingT) activityTerminalOutcome {
	deadline := time.Now().Add(activityDriverTimeout)
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
			describeResponse := a.describe(t)
			return activityTerminalOutcome{
				status:     describeResponse.GetInfo().GetStatus(),
				retryState: describeResponse.GetOutcome().GetRetryState(),
			}
		}
	}
	require.FailNow(
		t,
		"activity did not reach a terminal status",
		"within %s of the trace finishing; last observed: %+v",
		activityDriverTimeout,
		a.activityInfo(t),
	)
	return activityTerminalOutcome{}
}

func saaActivityInfo(i *activitypb.ActivityExecutionInfo) activityInfo {
	return activityInfo{
		RunState:                   i.GetRunState(),
		Attempt:                    i.GetAttempt(),
		CurrentRetryInterval:       i.GetCurrentRetryInterval().AsDuration().Round(time.Second),
		NextAttemptScheduleTimeSet: i.GetNextAttemptScheduleTime() != nil,
		LastHeartbeatDetails:       activityMarshalPayloads(i.GetHeartbeatDetails()),
	}
}

func (a *saaHandle) respondCanceledByID() error {
	_, err := a.d.env.FrontendClient().RespondActivityTaskCanceledById(
		a.d.ctx,
		&workflowservice.RespondActivityTaskCanceledByIdRequest{
			Namespace:  a.d.env.Namespace().String(),
			ActivityId: a.activityID,
			RunId:      a.runID,
			Identity:   a.d.env.Tv().WorkerIdentity(),
		},
	)
	return err
}

// rpc performs the frontend RPC for a non-Poll, non-timer event and returns its error.
func (a *saaHandle) rpc(_ testing.TB, e model.Event) error {
	fc := a.d.env.FrontendClient()
	ns := a.d.env.Namespace().String()
	switch e.Type {
	case model.HeartbeatType:
		_, err := fc.RecordActivityTaskHeartbeat(a.d.ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityRecordedHeartbeatDetails,
		})
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.d.ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondCompletedByIDType:
		_, err := fc.RespondActivityTaskCompletedById(a.d.ctx, &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace: ns, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondFailedType:
		req := &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(), Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailed(a.d.ctx, req)
		return err
	case model.RespondFailedByIDType:
		req := &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace: ns, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailedById(a.d.ctx, req)
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.d.ctx, &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		_, err := fc.RequestCancelActivityExecution(a.d.ctx, &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.TerminateType:
		_, err := fc.TerminateActivityExecution(a.d.ctx, &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.d.ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: uuid.NewString(),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.d.ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.d.ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), KeepPaused: e.KeepPaused, ResetHeartbeat: e.ResetHeartbeat,
		})
		return err
	case model.UpdateOptionsType:
		_, err := fc.UpdateActivityExecutionOptions(a.d.ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			ActivityOptions: &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)},
			UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
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
		//nolint:testifylint // t is a require.TestingT, which has only Errorf
		t.Errorf("saaDriver bug: PollActivityTaskQueue did not complete cleanly (poll timeout must be >= "+
			"MinLongPollTimeout; only an empty response with a nil error means \"no task\"): %v", err)
		return nil
	}
	if resp.GetActivityId() == "" {
		return nil // no task available
	}
	return resp
}
