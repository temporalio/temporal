package tests

// Workflow activity's (WFA) half of the model-conformance machinery. The shared engine is in
// activity_conformance.go.
//
// WFA is not a CHASM archetype, so unlike SAA there is no component to read: everything this surface can
// see comes from DescribeWorkflowExecution's pending-activity entry while the activity is in progress,
// and from the workflow's history once it has closed. That is exactly the public projection
// model.ExpectedDescribe predicts, so it is what WFA checks — the internal fields (the reset intent, the
// per-attempt and schedule-to-close stamps) are not observable here and are not asserted.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/await"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- the surface seam ------------------------------------------------------------------------

func (d *wfaDriver) surfaceName() string { return "WFA" }
func (d *wfaDriver) configIndex() int    { return d.cfgIdx }

func (d *wfaDriver) startForConformance(t testing.TB) activityConformanceTarget {
	return d.start(t, d.cfg)
}

// candidateEvents is WFA's event alphabet.
//
// It omits two of SAA's. TerminateActivityExecution has no workflow-activity form at all. And
// RequestCancelActivityExecutionRequest carries no workflow id, so a workflow activity cannot be
// cancelled by that RPC: cancellation comes from the workflow, which the driver reaches by signal — an
// occurrence with no accept/reject outcome for the model to predict. CancelRequested is therefore
// unreachable on this surface, and RespondCanceled only ever exercises its rejection.
func (d *wfaDriver) candidateEvents() []model.Event {
	var out []model.Event
	simple := []model.EventType{
		model.PollType, model.HeartbeatType, model.RespondCompletedType, model.RespondCanceledType, model.UpdateOptionsType,
	}
	for _, k := range simple {
		out = append(out, model.Event{Type: k})
	}
	out = append(out, model.Event{Type: model.UpdateOptionsType, SetsStartDelay: true})
	for _, r := range []bool{false, true} {
		out = append(out, model.Event{Type: model.RespondFailedType, Failure: &model.Failure{NonRetryable: !r}})
	}
	for _, sr := range []bool{false, true} {
		out = append(out, model.Event{Type: model.PauseType, SameRequestID: sr})
	}
	out = append(out, model.Event{Type: model.UnpauseType})
	for _, kp := range []bool{false, true} {
		for _, ro := range []bool{false, true} {
			out = append(out, model.Event{Type: model.ResetType, KeepPaused: kp, RestoreOriginal: ro})
		}
	}
	return out
}

// --- observation -----------------------------------------------------------------------------

// wfaObservation is everything WFA can see of an activity's state: its pending-activity entry while it
// is in progress, and the status its terminal history event records once it is not.
type wfaObservation struct {
	inProgress     bool
	runState       enumspb.PendingActivityState    // in-progress only
	attempt        int32                           // in-progress only
	terminalStatus enumspb.ActivityExecutionStatus // terminal only
}

// observe reads the activity's state. The workflow is held open past the activity precisely so the
// terminal half stays readable.
func (a *wfaHandle) observe(t require.TestingT) wfaObservation {
	if pa := a.pendingActivityInfo(t); pa != nil {
		return wfaObservation{inProgress: true, runState: pa.GetState(), attempt: pa.GetAttempt()}
	}
	return wfaObservation{terminalStatus: a.terminalStatusFromHistory(t)}
}

// conformsTo compares what the surface can see with the projection the model predicts.
//
// Only half of model.ExpectedDescribe applies at a time, because the two halves come from different
// places here. While the activity is in progress there is no ActivityExecutionStatus to read — a
// workflow activity has only a PendingActivityState — so the run state and attempt are what is checked.
// Once it has closed its pending entry is gone, taking the attempt count with it, and the history event
// gives the terminal status.
func (a *wfaHandle) conformsTo(t require.TestingT, expected model.AbstractState) (bool, string) {
	wantTerminalStatus, wantRunState := model.ExpectedDescribe(expected)
	wantInProgress := !expected.Status.Terminal()
	obs := a.observe(t)
	fields := [][3]string{
		{"InProgress", fmt.Sprint(obs.inProgress), fmt.Sprint(wantInProgress)},
	}
	switch {
	case obs.inProgress && wantInProgress:
		fields = append(fields,
			[3]string{"RunState", obs.runState.String(), wantRunState.String()},
			[3]string{"Attempt", fmt.Sprint(obs.attempt), fmt.Sprint(expected.AttemptCount)})
	case !obs.inProgress && !wantInProgress:
		fields = append(fields,
			[3]string{"Status", obs.terminalStatus.String(), wantTerminalStatus.String()})
	default: // the two disagree on whether the activity is open, which the InProgress row already says
	}
	rows, agree := activitySplitFields(fields)
	if len(rows) == 0 {
		return true, ""
	}
	return false, activityDiffBlock(rows, agree)
}

// awaitConformsTo polls the observation until it agrees with expected, or the deadline passes.
func (a *wfaHandle) awaitConformsTo(t testing.TB, expected model.AbstractState, deadline time.Time) {
	await.Require(a.testContext(), t, func(t *await.T) {
		ok, diff := a.conformsTo(t, expected)
		t.Require().Truef(ok, "activity does not match the model state\n%s", diff)
	}, max(0, time.Until(deadline)), activityDriverPollInterval)
}

// checkFinalEdge does nothing: the projection conformsTo compares is already everything this surface
// exposes. SAA's extra reads — the internal component state and the task stamps — have no WFA analogue.
func (a *wfaHandle) checkFinalEdge(require.TestingT, model.Event, model.AbstractState, model.Outcome) {
}

func (a *wfaHandle) heartbeatFlags() model.HeartbeatFlags {
	return model.HeartbeatFlags{
		CancelRequested: a.lastHeartbeat.GetCancelRequested(),
		ActivityPaused:  a.lastHeartbeat.GetActivityPaused(),
		ActivityReset:   a.lastHeartbeat.GetActivityReset(),
	}
}

func (a *wfaHandle) nextAttemptScheduleTime(t require.TestingT) *timestamppb.Timestamp {
	return a.pendingActivityInfo(t).GetNextAttemptScheduleTime()
}

// terminalStatusFromHistory reads the outcome of the workflow's single activity from its history. The
// wrapper workflow schedules exactly one activity, so the last terminal activity event is its outcome.
// TERMINATED has no counterpart here: a workflow activity has no terminate path.
func (a *wfaHandle) terminalStatusFromHistory(t require.TestingT) enumspb.ActivityExecutionStatus {
	status := enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	iter := a.d.env.SdkClient().GetWorkflowHistory(
		a.testContext(), a.workflowID, a.runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		if s, ok := wfaTerminalActivityStatus(event); ok {
			status = s
		}
	}
	return status
}

func wfaTerminalActivityStatus(event *historypb.HistoryEvent) (enumspb.ActivityExecutionStatus, bool) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, true
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, true
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, true
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, true
	default:
		return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED, false
	}
}
