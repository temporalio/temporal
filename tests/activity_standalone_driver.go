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
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- the activity under test -----------------------------------------------------------------

// --- driver --------------------------------------------------------------------------------

type saaDriver struct {
	env              *testcore.TestEnv
	t                *testing.T
	chasmCtx         context.Context // memoized by chasmContext
	cfg              activityConfig
	cfgIdx           int // labels this driver's config in the conformance explorer's logs
	numStarted       int
	activityIDPrefix string // activity-id prefix

	positivePollTimeout time.Duration // bounds a "must dispatch" poll; 0 => activityDriverTimeout

	// customizeStart mutates the StartActivityExecutionRequest before it is sent.
	customizeStart func(*workflowservice.StartActivityExecutionRequest)
}

// newSAADriver builds a driver.
func newSAADriver(t *testing.T, env *testcore.TestEnv, cfg activityConfig) *saaDriver {
	return &saaDriver{
		env:              env,
		t:                t,
		cfg:              cfg,
		activityIDPrefix: t.Name(),
	}
}

// testContext returns the driver's current test context, deliberately not cached;
// see [testcontext.EnsureRemaining].
func (d *saaDriver) testContext() context.Context {
	return testcontext.For(d.t)
}

func (a *saaHandle) testContext() context.Context {
	return a.d.testContext()
}

// saaHandle is a handle to an activity instance.
type saaHandle struct {
	activityDriverState
	model         *activityModel // the model state reached, so driveEvent can check each event
	d             *saaDriver
	activityID    string
	runID         string
	taskQueue     string
	lastHeartbeat *workflowservice.RecordActivityTaskHeartbeatResponse

	// Raw stamps, shifted cur->prev by each observed() read; see checkTaskInvalidation.
	prevStamp, curStamp       int32
	prevSTCStamp, curSTCStamp int32
}

// driveTrace schedules an activity, and then advances that activity through a sequence of events (a
// 'trace'). Returns a handle to the activity at the reached state.
func (d *saaDriver) driveTrace(t testing.TB, trace []model.Event) *saaHandle {
	a := d.start(t, d.cfg.forTrace(trace))
	for _, e := range trace {
		a.driveEvent(t, e)
	}
	return a
}

func (a *saaHandle) driveEvent(t testing.TB, e model.Event) {
	driveActivityEvent(t, a, e, a.model)
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
	awaitActivityDispatchDelay(a.testContext(), t, e, func(t require.TestingT) (bool, enumspb.PendingActivityState, *timestamppb.Timestamp, any) {
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
	resp, err := d.env.FrontendClient().StartActivityExecution(d.testContext(), d.startRequest(cfg, id, id))
	require.NoError(t, err)
	return &saaHandle{
		activityDriverState: activityDriverState{
			cfg:                 cfg,
			positivePollTimeout: d.positivePollTimeout,
			establishedReqID:    map[model.EventType]string{},
		},
		model:      newActivityModel(cfg),
		d:          d,
		activityID: id,
		runID:      resp.RunId,
		taskQueue:  id,
	}
}

func (d *saaDriver) startRequest(c activityConfig, activityID, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	opt := func(v time.Duration) *durationpb.Duration {
		if v == 0 {
			return nil
		}
		return durationpb.New(v)
	}
	req := &workflowservice.StartActivityExecutionRequest{
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
	if d.customizeStart != nil {
		d.customizeStart(req)
	}
	return req
}

// describe returns the DescribeActivityExecution response, including the outcome, the last failure,
// and the heartbeat details.
func (a *saaHandle) describe(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := a.d.env.FrontendClient().DescribeActivityExecution(a.testContext(), &workflowservice.DescribeActivityExecutionRequest{
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

// terminal is the terminal status from Info plus the failure discriminant and retry state from the
// Outcome.
func (a *saaHandle) terminal(t require.TestingT) activityTerminalProjection {
	resp := a.awaitTerminal(t)
	return activityTerminalProjection{
		Status:      resp.GetInfo().GetStatus(),
		FailureType: saaFailureType(resp.GetOutcome().GetFailure()),
		RetryState:  resp.GetOutcome().GetRetryState(),
	}
}

// terminalStatus waits for the activity to reach a terminal state and reports it.
func (a *saaHandle) terminalStatus(t require.TestingT) enumspb.ActivityExecutionStatus {
	return a.terminal(t).Status
}

// terminalCause is the failure the terminal outcome chains as its Cause, empty if there is none.
func (a *saaHandle) terminalCause(t require.TestingT) failureCause {
	cause := a.awaitTerminal(t).GetOutcome().GetFailure().GetCause()
	return failureCause{Type: saaFailureType(cause), Message: cause.GetMessage()}
}

// awaitTerminal waits for the activity to stop running and then describes it. Neither the terminal
// status nor the Outcome is settled before then, so reading either without waiting reports whatever the
// activity happens to be doing. PollActivityExecution is the long poll that resolves once it is no
// longer running; it returns an empty response when its window expires, so resubmit. Each poll is
// bounded by the deadline.
func (a *saaHandle) awaitTerminal(t require.TestingT) *workflowservice.DescribeActivityExecutionResponse {
	deadline := time.Now().Add(activityDriverTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(a.testContext(), deadline)
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
			return a.describe(t)
		}
	}
	require.FailNow(
		t,
		"activity did not reach a terminal status",
		"within %s of the trace finishing; last observed: %+v",
		activityDriverTimeout,
		a.activityInfo(t),
	)
	return nil
}

// heartbeatDetails is the last heartbeat checkpoint, as the first payload's raw bytes.
func (a *saaHandle) heartbeatDetails(t require.TestingT) []byte {
	return firstPayloadData(a.describe(t).GetInfo().GetHeartbeatDetails())
}

// saaFailureType is the application failure Type, the TimeoutType string, or "" for neither.
func saaFailureType(f *failurepb.Failure) string {
	if app := f.GetApplicationFailureInfo(); app != nil {
		return app.GetType()
	}
	if to := f.GetTimeoutFailureInfo(); to != nil {
		return to.GetTimeoutType().String()
	}
	return ""
}

// observedState is the activity's state as DescribeActivityExecution reports it.
func (a *saaHandle) observedState(t require.TestingT) activityState {
	info := a.describe(t).GetInfo()
	switch info.GetStatus() {
	case enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return activityState{runState: info.GetRunState(), attempt: info.GetAttempt()}
	default:
		return activityState{closed: true}
	}
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
		a.testContext(),
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
		resp, err := fc.RecordActivityTaskHeartbeat(a.testContext(), &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: ns, TaskToken: a.token, Details: activityRecordedHeartbeatDetails,
		})
		a.lastHeartbeat = resp
		return err
	case model.RespondCompletedType:
		_, err := fc.RespondActivityTaskCompleted(a.testContext(), &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
			Result: payloads.EncodeString("result"),
		})
		return err
	case model.RespondCompletedByIDType:
		_, err := fc.RespondActivityTaskCompletedById(a.testContext(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
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
		_, err := fc.RespondActivityTaskFailed(a.testContext(), req)
		return err
	case model.RespondFailedByIDType:
		req := &workflowservice.RespondActivityTaskFailedByIdRequest{
			Namespace: ns, RunId: a.runID, ActivityId: a.activityID, Identity: a.d.env.Tv().WorkerIdentity(),
			Failure: respondFailedFailure(e, a.cfg.NextRetryDelay),
		}
		if e.HasHeartbeatDetails {
			req.LastHeartbeatDetails = activityHeartbeatDetails
		}
		_, err := fc.RespondActivityTaskFailedById(a.testContext(), req)
		return err
	case model.RespondCanceledType:
		_, err := fc.RespondActivityTaskCanceled(a.testContext(), &workflowservice.RespondActivityTaskCanceledRequest{
			Namespace: ns, TaskToken: a.token, Identity: a.d.env.Tv().WorkerIdentity(),
		})
		return err
	case model.RequestCancelType:
		_, err := fc.RequestCancelActivityExecution(a.testContext(), &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.TerminateType:
		_, err := fc.TerminateActivityExecution(a.testContext(), &workflowservice.TerminateActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.PauseType:
		_, err := fc.PauseActivityExecution(a.testContext(), &workflowservice.PauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(), Reason: "drive", RequestId: a.reqID(e),
		})
		return err
	case model.UnpauseType:
		_, err := fc.UnpauseActivityExecution(a.testContext(), &workflowservice.UnpauseActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
		})
		return err
	case model.ResetType:
		_, err := fc.ResetActivityExecution(a.testContext(), &workflowservice.ResetActivityExecutionRequest{
			Namespace: ns, ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
			KeepPaused: e.KeepPaused, ResetHeartbeat: e.ResetHeartbeat, RestoreOriginalOptions: e.RestoreOriginal,
		})
		return err
	case model.UpdateOptionsType:
		return a.updateOptions(e)
	default:
		return fmt.Errorf("saaDriver: unhandled event type %v", e.Type)
	}
}

func (a *saaHandle) updateOptions(e model.Event) error {
	req := &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace: a.d.env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, Identity: a.d.env.Tv().ClientIdentity(),
	}
	switch {
	case e.RestoreOriginal:
		req.RestoreOriginal = true
	case e.SetsStartDelay:
		req.ActivityOptions = &activitypb.ActivityOptions{StartDelay: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"start_delay"}}
	default:
		// A minimal, always-valid update: re-set the heartbeat timeout.
		req.ActivityOptions = &activitypb.ActivityOptions{HeartbeatTimeout: durationpb.New(time.Hour)}
		req.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}}
	}
	_, err := a.d.env.FrontendClient().UpdateActivityExecutionOptions(a.testContext(), req)
	return err
}

func (a *saaHandle) pollForTask(t require.TestingT, timeout time.Duration) *workflowservice.PollActivityTaskQueueResponse {
	return activityPollForTask(a.testContext(), t, "saaDriver", a.d.env, a.taskQueue, timeout)
}
