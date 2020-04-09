package history

import (
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
)

type workflowContext interface {
	getContext() workflowExecutionContext
	getMutableState() mutableState
	reloadMutableState() (mutableState, error)
	getReleaseFn() releaseWorkflowExecutionFunc
	getWorkflowID() string
	getRunID() string
}

type workflowContextImpl struct {
	context      workflowExecutionContext
	mutableState mutableState
	releaseFn    releaseWorkflowExecutionFunc
}

type updateWorkflowAction struct {
	noop           bool
	createDecision bool
}

var (
	updateWorkflowWithNewDecision = &updateWorkflowAction{
		createDecision: true,
	}
	updateWorkflowWithoutDecision = &updateWorkflowAction{
		createDecision: false,
	}
)

type updateWorkflowActionFunc func(workflowExecutionContext, mutableState) (*updateWorkflowAction, error)

func (w *workflowContextImpl) getContext() workflowExecutionContext {
	return w.context
}

func (w *workflowContextImpl) getMutableState() mutableState {
	return w.mutableState
}

func (w *workflowContextImpl) reloadMutableState() (mutableState, error) {
	mutableState, err := w.getContext().loadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	w.mutableState = mutableState
	return mutableState, nil
}

func (w *workflowContextImpl) getReleaseFn() releaseWorkflowExecutionFunc {
	return w.releaseFn
}

func (w *workflowContextImpl) getWorkflowID() string {
	return w.getContext().getExecution().GetWorkflowId()
}

func (w *workflowContextImpl) getRunID() string {
	return w.getContext().getExecution().GetRunId()
}

func newWorkflowContext(
	context workflowExecutionContext,
	releaseFn releaseWorkflowExecutionFunc,
	mutableState mutableState,
) *workflowContextImpl {

	return &workflowContextImpl{
		context:      context,
		releaseFn:    releaseFn,
		mutableState: mutableState,
	}
}

func failDecision(
	mutableState mutableState,
	decision *decisionInfo,
	decisionFailureCause eventpb.DecisionTaskFailedCause,
) error {

	if _, err := mutableState.AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		decisionFailureCause,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	return mutableState.FlushBufferedEvents()
}

func scheduleDecision(
	mutableState mutableState,
) error {

	if mutableState.HasPendingDecision() {
		return nil
	}

	_, err := mutableState.AddDecisionTaskScheduledEvent(false)
	if err != nil {
		return serviceerror.NewInternal("Failed to add decision scheduled event.")
	}
	return nil
}

func retryWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
	parentNamespace string,
	continueAsNewAttributes *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes,
) (mutableState, error) {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			eventpb.DecisionTaskFailedCause_ForceCloseDecision,
		); err != nil {
			return nil, err
		}
	}

	_, newMutableState, err := mutableState.AddContinueAsNewEvent(
		eventBatchFirstEventID,
		common.EmptyEventID,
		parentNamespace,
		continueAsNewAttributes,
	)
	if err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func timeoutWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			eventpb.DecisionTaskFailedCause_ForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddTimeoutWorkflowEvent(
		eventBatchFirstEventID,
	)
	return err
}

func terminateWorkflow(
	mutableState mutableState,
	eventBatchFirstEventID int64,
	terminateReason string,
	terminateDetails []byte,
	terminateIdentity string,
) error {

	if decision, ok := mutableState.GetInFlightDecision(); ok {
		if err := failDecision(
			mutableState,
			decision,
			eventpb.DecisionTaskFailedCause_ForceCloseDecision,
		); err != nil {
			return err
		}
	}

	_, err := mutableState.AddWorkflowExecutionTerminatedEvent(
		eventBatchFirstEventID,
		terminateReason,
		terminateDetails,
		terminateIdentity,
	)
	return err
}
