package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

	// IncomingSignals map is used to track incoming signals, keyed by request ID,
	// to allow DescribeWorkflow to resolve RequestIDRef signal backlinks.
	IncomingSignals chasm.Map[string, *chasmworkflowpb.IncomingSignalData]
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

// AddIncomingSignalEvent adds an entry for the signal requestID -> eventID mapping to
// track all signals that have been received by the workflow.
// Returns error if this requestID already exists in the map -- use UpdateIncomingSignalEvent to update existing entries.
func (w *Workflow) AddIncomingSignalEvent(
	ctx chasm.MutableContext,
	requestID string,
	eventID int64,
) error {
	if w.IncomingSignals == nil {
		w.IncomingSignals = make(chasm.Map[string, *chasmworkflowpb.IncomingSignalData])
	}
	if w.HasIncomingSignalEvent(ctx, requestID) {
		return serviceerror.NewInvalidArgumentf(
			"signal request ID %s already exists, use UpdateIncomingSignalEvent to update it instead",
			requestID,
		)
	}
	w.IncomingSignals[requestID] = chasm.NewDataField(ctx, &chasmworkflowpb.IncomingSignalData{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventId:   eventID,
	})
	return nil
}

// UpdateIncomingSignalEvent updates the eventID for an existing signal requestID in the map.
// If the requestID is not in the map, this is a no-op (e.g. when called for non-signal request IDs
// during buffer flush).
func (w *Workflow) UpdateIncomingSignalEvent(
	ctx chasm.MutableContext,
	requestID string,
	eventID int64,
) error {
	if !w.HasIncomingSignalEvent(ctx, requestID) {
		return nil
	}
	w.IncomingSignals[requestID] = chasm.NewDataField(ctx, &chasmworkflowpb.IncomingSignalData{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		EventId:   eventID,
	})
	return nil
}

func (w *Workflow) HasIncomingSignalEvent(_ chasm.Context, requestID string) bool {
	_, exists := w.IncomingSignals[requestID]
	return exists
}

// HasAnyBufferedEvent returns true if the workflow has any buffered event matching the given filter.
func (w *Workflow) HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool {
	return w.MSPointer.HasAnyBufferedEvent(filter)
}

func (w *Workflow) WorkflowTypeName() string {
	return w.GetWorkflowTypeName()
}
