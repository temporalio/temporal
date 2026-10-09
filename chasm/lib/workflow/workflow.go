package workflow

import (
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Both Workflow and WorkflowUpdate components have completion callbacks attached to them.
// However, the [callbackpb.CallbackMetadata] is only on Workflow. (It contains the aggregated
// information about all callbacks in the CHASM tree, spanning all of the child WorkflowUpdates.)
//
// So while Workflow implements the [callback.Host] interface, WorkflowUpdate is only a [Holder].
// And attaching completion callbacks to a WorkflowUpdate is done through an [updateCallbackHost].
var (
	_ callback.Host   = (*Workflow)(nil)
	_ callback.Holder = (*WorkflowUpdate)(nil)
)

type Workflow struct {
	chasm.UnimplementedComponent

	// For now, the workflow's execution state is managed by mutable_state_impl, not the CHASM
	// engine. WorkflowState only carries the bookkeeping that the CHASM tree itself owns.
	*chasmworkflowpb.WorkflowState

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the workflow.
	Callbacks chasm.Map[string, *callback.Callback]

	// Operations map is used to store the Nexus operations for the workflow, keyed by scheduled event ID.
	Operations chasm.Map[int64, *nexusoperation.Operation]

	// IncomingSignals map is used to track incoming signals, keyed by request ID,
	// to allow DescribeWorkflow to resolve RequestIDRef signal backlinks.
	IncomingSignals chasm.Map[string, *chasmworkflowpb.IncomingSignalData]

	// Updates indexed by update ID, used to store the update components.
	Updates chasm.Map[string, *WorkflowUpdate]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer:     msPointer,
		WorkflowState: &chasmworkflowpb.WorkflowState{},
	}
}

// LifecycleState reports the workflow's lifecycle as derived from mutable state.
//
// NOTE: For legacy reasons this is the reverse of other archetypes. Elsewhere the root component's
// LifecycleState drives the execution state in mutable state (see closeTransactionHandleRootLifecycleChange,
// which is bypassed for workflows). Here mutableStateImpl updates the execution state directly and this method
// only reflects it, so it is meant for the read path (e.g. task validation), not for driving state transitions.
func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	return w.MSPointer.LifecycleState()
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

// ProcessCloseCallbacks triggers "WorkflowClosed" callbacks using the CHASM implementation.
// It schedules all workflow-level and update-level callbacks that are in STANDBY state.
func (w *Workflow) ProcessCloseCallbacks(ctx chasm.MutableContext) error {
	if err := callback.ScheduleCompletionCallbacks(ctx, w); err != nil {
		return err
	}
	return w.ProcessAllUpdateCloseCallbacks(ctx)
}

// ProcessAllUpdateCloseCallbacks triggers callbacks for all updates without touching
// workflow-level callbacks. This is used when the workflow is continuing to a new run
// (ContinueAsNew, retry, cron): workflow-level callbacks are inherited by the new run,
// but update callbacks must fire now because the update was aborted on the old run.
func (w *Workflow) ProcessAllUpdateCloseCallbacks(ctx chasm.MutableContext) error {
	for _, updateField := range w.Updates {
		err := callback.ScheduleCompletionCallbacks(ctx, updateField.Get(ctx))
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessUpdateCallbacks triggers callbacks for a single updateID if exists.
func (w *Workflow) ProcessUpdateCallbacks(ctx chasm.MutableContext, updateID string) error {
	update, exists := w.Updates[updateID]
	if !exists {
		return serviceerror.NewNotFoundf("update with ID %s not found", updateID)
	}
	return callback.ScheduleCompletionCallbacks(ctx, update.Get(ctx))
}

// RejectUpdate stores the rejection failure on the WorkflowUpdate component and
// fires any pending callbacks. This is used when a reapplied update (after reset)
// is rejected by the worker's validator - the callbacks need to deliver the
// rejection failure to the caller.
func (w *Workflow) RejectUpdate(ctx chasm.MutableContext, updateID string, rejectionFailure *failurepb.Failure) error {
	updateField, exists := w.Updates[updateID]
	if !exists {
		return nil // no callbacks registered for this update
	}

	upd := updateField.Get(ctx)
	upd.RejectionFailure = rejectionFailure

	return callback.ScheduleCompletionCallbacks(ctx, upd)
}

// CallbackAddition is a set of completion callbacks that a single request is about to attach
// to an execution. UpdateID is empty when the callbacks target the workflow itself rather
// than one of its updates.
type CallbackAddition struct {
	UpdateID  string
	RequestID string
	Callbacks []*commonpb.Callback
}

// callbackHolder returns the [callback.Holder] implementation for the given updateID. Or if
// empty, the workflow's own callbacks. It is nil for an update with no callbacks attached yet.
func (w *Workflow) callbackHolder(ctx chasm.Context, updateID string) callback.Holder {
	if updateID == "" {
		return w
	}
	if updateField, ok := w.Updates[updateID]; ok {
		if upd, ok := updateField.TryGet(ctx); ok {
			return upd
		}
	}
	return nil
}

// ValidateCallbackAddition checks that addition can be attached to this execution without
// breaching aggregate callback validation checks, e.g. callback count or total size.
//
// IMPORTANT: This should ONLY be called on initial request paths, and NEVER for replays.
//
// Like any other user input, callbacks are validated by the request handler that receives them,
// and not where they are attached. They are attached while applying history events, which also
// happens when replaying events that were accepted long ago (NDC replication, reset, history
// import) or when carrying callbacks over to a new run (continue-as-new, retry). Rejecting them
// there would stall the task rather than protect anything, and lowering a limit would
// retroactively wedge every execution already above it. Callbacks are validated once, when a
// request introduces them, and then kept as-is.
//
// inFlight lists callbacks the execution will attach if their Updates are accepted: those of
// Updates admitted but not yet accepted, which the history service tracks in memory in its update
// registry and which are therefore not yet reflected in WorkflowState. They are reserved against
// the limits but not themselves validated, and an addition re-offering one of them (same UpdateID
// and RequestID) is not counted a second time.
func (w *Workflow) ValidateCallbackAddition(
	ctx chasm.Context,
	inFlight []CallbackAddition,
	addition CallbackAddition,
	validator callbacks.Validator,
) error {
	if len(addition.Callbacks) == 0 {
		return nil
	}

	// A request that already attached its callbacks is a no-op at attach time, so counting it
	// again here would reject retries that are actually within the limits. This has to precede
	// the limit checks: target already holds the callbacks being re-offered.
	target := w.callbackHolder(ctx, addition.UpdateID)
	if callback.HasCallbacksForRequest(ctx, target, addition.RequestID) {
		return nil
	}

	usage := callback.UsageOf(w.GetCallbackMetadata())
	seen := make(map[callbackRequestKey]struct{}, len(inFlight))
	for _, held := range inFlight {
		key := callbackRequestKey{updateID: held.UpdateID, requestID: held.RequestID}
		// A retry of the request that admitted an Update is buffered alongside it, so the same
		// request can be reported twice.
		if _, ok := seen[key]; ok || len(held.Callbacks) == 0 {
			continue
		}
		seen[key] = struct{}{}
		// Already attached, and so already in the totals: a retry of a request persisted with an
		// UpdateAdmitted event can be buffered again.
		if callback.HasCallbacksForRequest(ctx, w.callbackHolder(ctx, held.UpdateID), held.RequestID) {
			continue
		}
		usage.Reserve(held.Callbacks)
	}
	// The same request is already held in flight, and is counted above.
	if _, ok := seen[callbackRequestKey{updateID: addition.UpdateID, requestID: addition.RequestID}]; ok {
		return nil
	}

	if ctx.NamespaceEntry() == nil {
		return serviceerror.NewInternal("chasm context missing namespace entry")
	}
	namespaceName := ctx.NamespaceEntry().Name().String()
	return callback.ValidateAdditions(namespaceName, usage, addition.Callbacks, validator)
}

type callbackRequestKey struct {
	updateID  string
	requestID string
}

// AddCompletionCallbacks attaches completion callbacks to the workflow. Re-attaching a request
// that is already present is a no-op.
//
// NOTE: Aggregate limits are NOT checked here. See [Workflow.ValidateCallbackAddition].
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	return callback.Attach(ctx, w, requestID, eventTime, completionCallbacks, callback.WithReusedRequestID())
}

// AddUpdateCompletionCallbacks attaches completion callbacks to the given update, creating its
// WorkflowUpdate component if needed. Re-attaching a request that is already present is a no-op,
// which keeps the totals accurate when the same callbacks arrive on more than one event (an
// update's admitted and accepted events, say).
//
// NOTE: Aggregate limits are NOT checked here. See [Workflow.ValidateCallbackAddition].
func (w *Workflow) AddUpdateCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	updateID string,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	// Don't create a WorkflowUpdate component for an update without callbacks.
	if len(completionCallbacks) == 0 {
		return nil
	}

	// Create the WorkflowUpdate component if needed.
	if w.Updates == nil {
		w.Updates = make(chasm.Map[string, *WorkflowUpdate], 1)
	}
	if _, ok := w.Updates[updateID]; !ok {
		w.Updates[updateID] = chasm.NewComponentField(ctx, NewWorkflowUpdate(ctx, updateID, w.MSPointer))
	}

	// Wrap the WorkflowUpdate so it can be used as a [callback.Host].
	wfUpdateHost := updateCallbackHost{
		workflow: w,
		update:   w.Updates[updateID].Get(ctx),
	}
	return callback.Attach(ctx, wfUpdateHost, requestID, eventTime, completionCallbacks, callback.WithReusedRequestID())
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
// Note that since signals are buffered, the eventID may the common.BufferedEventID, which
// will be updated to a concrete eventID once this signal is flushed to the DB.
// If caller tries to add an already-existing eventID, this function will ignore and silently return
// instead of overwriting -- use UpdateIncomingSignalEvent to update existing entries.
func (w *Workflow) AddIncomingSignalEvent(
	ctx chasm.MutableContext,
	requestID string,
	eventID int64,
) error {
	if w.IncomingSignals == nil {
		w.IncomingSignals = make(chasm.Map[string, *chasmworkflowpb.IncomingSignalData])
	}
	if w.HasIncomingSignalEvent(ctx, requestID) {
		return nil
	}
	w.IncomingSignals[requestID] = chasm.NewDataField(ctx, &chasmworkflowpb.IncomingSignalData{
		// This might be common.BufferedEventID, which will be updated via UpdateIncomingSignalEvent
		// once this signal is flushed to DB.
		EventId: eventID,
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
	if w.HasIncomingSignalEvent(ctx, requestID) {
		w.IncomingSignals[requestID].Get(ctx).EventId = eventID
	}

	return nil
}

// HasIncomingSignalEvent returns true if a signal with this requestID is already persisted
// in this CHASM tree.
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
