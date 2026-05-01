package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Compile-time assertion: *Workflow must implement activity.ActivityStore.
var _ activity.ActivityStore = (*Workflow)(nil)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, workflow state is managed by mutable_state_impl, not CHASM engine, leaving it empty as CHASM expects a
	// state object.
	*emptypb.Empty

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]

	// Operations map is used to store the Nexus operations for the workflow, keyed by scheduled event ID.
	Operations chasm.Map[int64, *nexusoperation.Operation]

	// Activities map stores embedded CHASM activity sub-components, keyed by SDK-provided activity ID.
	Activities chasm.Map[string, *activity.Activity]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer: msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}

func (w *Workflow) ContextMetadata(_ chasm.Context) map[string]string {
	// TODO: Export workflow metadata from the CHASM workflow root instead of CloseTransaction().
	return nil
}

func (w *Workflow) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewInternal("workflow root Terminate should not be called")
}

// OnActivityCompleted implements ActivityStore for workflow-embedded activities.
// Writes ActivityTaskStarted + ActivityTaskCompleted history events; Apply() handles cleanup.
func (w *Workflow) OnActivityCompleted(ctx chasm.MutableContext, act *activity.Activity) error {
	scheduledEventID := act.ActivityState.GetScheduledEventId()
	attempt := act.LastAttempt.Get(ctx)
	startedEvent, err := addAndApplyHistoryEvent[ActivityTaskStartedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				Attempt:          attempt.GetCount(),
				RequestId:        attempt.GetStartRequestId(),
				Identity:         attempt.GetLastWorkerIdentity(),
			},
		}
	})
	if err != nil {
		return err
	}
	_, err = addAndApplyHistoryEvent[ActivityTaskCompletedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
			ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEvent.GetEventId(),
				Identity:         attempt.GetLastWorkerIdentity(),
				Result:           act.Outcome.Get(ctx).GetSuccessful().GetOutput(),
			},
		}
	})
	return err
}

// OnActivityFailed implements ActivityStore for workflow-embedded activities.
// Writes ActivityTaskStarted + ActivityTaskFailed history events; Apply() handles cleanup.
func (w *Workflow) OnActivityFailed(ctx chasm.MutableContext, act *activity.Activity) error {
	scheduledEventID := act.ActivityState.GetScheduledEventId()
	attempt := act.LastAttempt.Get(ctx)
	startedEvent, err := addAndApplyHistoryEvent[ActivityTaskStartedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
				ScheduledEventId: scheduledEventID,
				Attempt:          attempt.GetCount(),
				RequestId:        attempt.GetStartRequestId(),
				Identity:         attempt.GetLastWorkerIdentity(),
			},
		}
	})
	if err != nil {
		return err
	}
	_, err = addAndApplyHistoryEvent[ActivityTaskFailedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
			ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEvent.GetEventId(),
				Failure:          act.Outcome.Get(ctx).GetFailed().GetFailure(),
				RetryState:       enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
				Identity:         attempt.GetLastWorkerIdentity(),
			},
		}
	})
	return err
}

// OnActivityTimedOut implements ActivityStore for workflow-embedded activities.
// Writes ActivityTaskTimedOut (and optionally ActivityTaskStarted) history events; Apply() handles cleanup.
func (w *Workflow) OnActivityTimedOut(ctx chasm.MutableContext, act *activity.Activity, timeoutFailure *failurepb.Failure, needsStartedEvent bool) error {
	scheduledEventID := act.ActivityState.GetScheduledEventId()
	if timeoutFailure == nil {
		return nil
	}

	var startedEventID int64
	if needsStartedEvent {
		attempt := act.LastAttempt.Get(ctx)
		startedEvent, err := addAndApplyHistoryEvent[ActivityTaskStartedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: scheduledEventID,
					Attempt:          attempt.GetCount(),
					RequestId:        attempt.GetStartRequestId(),
					Identity:         attempt.GetLastWorkerIdentity(),
				},
			}
		})
		if err != nil {
			return err
		}
		startedEventID = startedEvent.GetEventId()
	}
	_, err := addAndApplyHistoryEvent[ActivityTaskTimedOutEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
			ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
				ScheduledEventId: scheduledEventID,
				StartedEventId:   startedEventID,
				Failure:          timeoutFailure,
				RetryState:       enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			},
		}
	})
	return err
}

// OnActivityCanceled implements ActivityStore for workflow-embedded activities.
// No ActivityTaskCanceled history event is written on the forward path in this prototype.
func (w *Workflow) OnActivityCanceled(ctx chasm.MutableContext, act *activity.Activity) error {
	activityID := act.ActivityState.GetActivityId()
	delete(w.Activities, activityID)
	return w.ScheduleWorkflowTask()
}

// OnActivityTerminated implements ActivityStore for workflow-embedded activities.
func (w *Workflow) OnActivityTerminated(ctx chasm.MutableContext, act *activity.Activity) error {
	activityID := act.ActivityState.GetActivityId()
	delete(w.Activities, activityID)
	return w.ScheduleWorkflowTask()
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
) error {
	// Check CHASM max callbacks limit
	currentCallbackCount := len(w.Callbacks)
	if len(completionCallbacks)+currentCallbackCount > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			currentCallbackCount,
		)
	}

	// Initialize map if needed
	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	// Add each callback
	for idx, cb := range completionCallbacks {
		chasmCB := &callbackspb.Callback{
			Links: cb.GetLinks(),
		}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			chasmCB.Variant = &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
		}

		// requestID (unique per API call) + idx (position within the request) ensures unique, idempotent callback IDs.
		// Unlike HSM callbacks, CHASM replicates entire trees rather than replaying events, so deterministic
		// cross-cluster IDs based on event version are not needed.
		id := fmt.Sprintf("%s-%d", requestID, idx)

		// Create and add callback
		callbackObj := callback.NewCallback(requestID, eventTime, &callbackspb.CallbackState{}, chasmCB)
		w.Callbacks[id] = chasm.NewComponentField(ctx, callbackObj)
	}
	return nil
}

// AddEmbeddedActivity adds a CHASM activity sub-component to the workflow, keyed by SDK-provided activity ID.
func (w *Workflow) AddEmbeddedActivity(
	ctx chasm.MutableContext,
	activityID string,
	act *activity.Activity,
) {
	if w.Activities == nil {
		w.Activities = make(chasm.Map[string, *activity.Activity])
	}
	w.Activities[activityID] = chasm.NewComponentField(ctx, act)
}

// AddNexusOperation adds a Nexus operation component to the workflow.
func (w *Workflow) AddNexusOperation(
	ctx chasm.MutableContext,
	key int64,
	op *nexusoperation.Operation,
) {
	if w.Operations == nil {
		w.Operations = make(chasm.Map[int64, *nexusoperation.Operation])
	}
	w.Operations[key] = chasm.NewComponentField(ctx, op)
}

// addAndApplyHistoryEvent adds a history event to the workflow and applies the corresponding event definition,
// addAndApplyHistoryEvent adds a history event to the workflow and applies the corresponding event definition,
// looked up by Go type. This is the preferred way to add and apply events as it provides go-to-definition navigation.
func addAndApplyHistoryEvent[D EventDefinition](
	w *Workflow,
	ctx chasm.MutableContext,
	setAttributes func(*historypb.HistoryEvent),
) (*historypb.HistoryEvent, error) {
	def, ok := eventDefinitionByGoType[D](workflowContextFromChasm(ctx).registry)
	if !ok {
		return nil, serviceerror.NewInternalf("no event definition registered for Go type %T", (*D)(nil))
	}
	event := w.AddHistoryEvent(def.Type(), setAttributes)
	return event, def.Apply(ctx, w, event)
}

// HasAnyBufferedEvent returns true if the workflow has any buffered event matching the given filter.
func (w *Workflow) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
	return w.MSPointer.HasAnyBufferedEvent(filter)
}

func (w *Workflow) WorkflowTypeName() string {
	return w.GetWorkflowTypeName()
}

// BuildPendingActivityInfos reads CHASM-embedded activities and converts them to API format
// for DescribeWorkflowExecution. Only non-terminal activities are included.
func (w *Workflow) BuildPendingActivityInfos(ctx chasm.Context) ([]*workflowpb.PendingActivityInfo, error) {
	var result []*workflowpb.PendingActivityInfo
	for _, field := range w.Activities {
		act := field.Get(ctx)
		info := act.BuildPendingActivityInfo(ctx)
		if info != nil {
			result = append(result, info)
		}
	}
	return result, nil
}

// FindActivityByScheduledEventID finds an embedded activity by its scheduled history event ID.
// Returns nil if not found.
func (w *Workflow) FindActivityByScheduledEventID(ctx chasm.Context, scheduledEventID int64) *activity.Activity {
	for _, field := range w.Activities {
		act := field.Get(ctx)
		if act.ActivityState.GetScheduledEventId() == scheduledEventID {
			return act
		}
	}
	return nil
}
