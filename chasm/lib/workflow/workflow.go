package workflow

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/service/history/historybuilder"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Workflow struct {
	chasm.UnimplementedComponent

	// Workflow execution state itself is still managed by mutable_state_impl, not the CHASM
	// engine. This carries only the aggregate callback accounting, which cannot be derived
	// cheaply from the tree. It is left nil until something needs it, so a workflow with no
	// callbacks still persists a zero-byte blob exactly as it did when this was an
	// emptypb.Empty. Use state() to write; the generated getters are nil-safe for reads.
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
		MSPointer: msPointer,
	}
}

// state returns the workflow's persisted state, allocating it on first write. Reads should use
// the generated getters, which tolerate a nil receiver.
func (w *Workflow) state() *chasmworkflowpb.WorkflowState {
	if w.WorkflowState == nil {
		w.WorkflowState = &chasmworkflowpb.WorkflowState{}
	}
	return w.WorkflowState
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

// ProcessCloseCallbacks triggers "WorkflowClosed" callbacks using the CHASM implementation.
// It schedules all workflow-level and update-level callbacks that are in STANDBY state.
func (w *Workflow) ProcessCloseCallbacks(ctx chasm.MutableContext) error {
	if err := callback.ScheduleStandbyCallbacks(ctx, w.Callbacks); err != nil {
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
		if err := callback.ScheduleStandbyCallbacks(ctx, updateField.Get(ctx).Callbacks); err != nil {
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
	return callback.ScheduleStandbyCallbacks(ctx, update.Get(ctx).Callbacks)
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

	return callback.ScheduleStandbyCallbacks(ctx, upd.Callbacks)
}

// CallbackTotals returns the count and summed size of every callback on the workflow, across
// both the workflow-level map and all update-level maps.
//
// The totals are denormalized onto WorkflowState because deriving them walks and deserializes
// every WorkflowUpdate node, and under a MutableContext that also marks each one dirty. Sizes
// could not be derived at all mid-transaction, since child blobs are only serialized when the
// transaction closes.
func (w *Workflow) CallbackTotals(ctx chasm.Context) (count int, size int64) {
	if w.GetTotalCallbacksSize() != 0 || (len(w.Callbacks) == 0 && len(w.Updates) == 0) {
		return int(w.GetTotalCallbacksCount()), w.GetTotalCallbacksSize()
	}
	// A stored size of zero alongside a non-empty callback set can only mean the workflow was
	// persisted before WorkflowState existed, because a validated callback always serializes to
	// more than zero bytes. Recompute once; the caller writes the result back.
	return w.recomputeCallbackTotals(ctx)
}

// recomputeCallbackTotals walks the tree to rebuild the aggregate accounting. Only the callback
// specification is measured, never the surrounding CallbackState, whose delivery bookkeeping
// mutates after attach and would make the total drift on its own.
func (w *Workflow) recomputeCallbackTotals(ctx chasm.Context) (count int, size int64) {
	sum := func(callbacks chasm.Map[string, *callback.Callback]) {
		for _, field := range callbacks {
			count++
			size += int64(field.Get(ctx).GetCallback().Size())
		}
	}
	sum(w.Callbacks)
	for _, updateField := range w.Updates {
		sum(updateField.Get(ctx).Callbacks)
	}
	return count, size
}

// UpdateCallbackCount returns how many completion callbacks are attached to the given update,
// or zero if the update has none yet. Used by the write path to enforce the per-update cap
// before an event is created.
func (w *Workflow) UpdateCallbackCount(ctx chasm.Context, updateID string) int {
	updateField, ok := w.Updates[updateID]
	if !ok {
		return 0
	}
	return len(updateField.Get(ctx).Callbacks)
}

// pendingCallback is a converted callback together with the key it will occupy.
type pendingCallback struct {
	id      string
	chasmCB *callbackspb.Callback
}

// planCallbackInsertions converts the request's callbacks and drops any whose key is already
// present in target.
//
// Planning before validating means the limits are charged against what the call would actually
// persist rather than against the request as sent: a retry re-derives keys it has already
// written, and rejecting it for exceeding a cap it does not move would be wrong. Conversion
// happens before the skip so a malformed callback is still rejected on a retry, and all
// conversion happens up front so target is never partially mutated.
func planCallbackInsertions(
	target chasm.Map[string, *callback.Callback],
	requestID string,
	completionCallbacks []*commonpb.Callback,
) ([]pendingCallback, error) {
	var pending []pendingCallback
	for idx, cb := range completionCallbacks {
		chasmCB, err := callback.FromAPICallback(cb)
		if err != nil {
			return nil, err
		}
		// requestID (unique per API call) + idx (position within the request) ensures unique, idempotent callback IDs.
		// Unlike HSM callbacks, CHASM replicates entire trees rather than replaying events, so deterministic
		// cross-cluster IDs based on event version are not needed.
		id := fmt.Sprintf("%s-%d", requestID, idx)
		if _, exists := target[id]; exists {
			// Already registered, skip to avoid overwriting.
			continue
		}
		pending = append(pending, pendingCallback{id: id, chasmCB: chasmCB})
	}
	return pending, nil
}

// applyCallbackInsertions writes the planned callbacks into target and reports their summed
// size, which the caller folds into the workflow's aggregate accounting.
func applyCallbackInsertions(
	ctx chasm.MutableContext,
	target chasm.Map[string, *callback.Callback],
	requestID string,
	eventTime *timestamppb.Timestamp,
	pending []pendingCallback,
) (insertedSize int64) {
	for _, p := range pending {
		target[p.id] = chasm.NewComponentField(ctx, callback.NewCallback(requestID, eventTime, p.chasmCB))
		insertedSize += int64(p.chasmCB.Size())
	}
	return insertedSize
}

// AddCompletionCallbacks creates completion callbacks using the CHASM implementation.
//
// This is reached from ApplyWorkflowExecutionStartedEvent and friends, which also run under
// MutableStateRebuilder during NDC replication, history import, and reset. It therefore only
// applies the event and maintains the aggregate accounting; the cumulative limits are enforced
// on the write path, where rejecting a request is still meaningful. See
// MutableStateImpl.validateChasmCallbackAttachments.
func (w *Workflow) AddCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	pending, err := planCallbackInsertions(w.Callbacks, requestID, completionCallbacks)
	if err != nil {
		return err
	}
	currentCount, currentSize := w.CallbackTotals(ctx)

	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(pending))
	}

	insertedSize := applyCallbackInsertions(ctx, w.Callbacks, requestID, eventTime, pending)
	w.recordCallbackTotals(ctx, currentCount+len(pending), currentSize+insertedSize)
	return nil
}

// recordCallbackTotals persists the workflow's aggregate callback accounting. It is written
// even when nothing was inserted, so that a total recomputed for a workflow that predates the
// counters is not recomputed again on the next attach.
func (w *Workflow) recordCallbackTotals(ctx chasm.Context, count int, size int64) {
	w.state().TotalCallbacksCount = int32(count)
	w.state().TotalCallbacksSize = size
	callback.RecordTotalSizePerExecution(ctx, size)
}

// AddUpdateCompletionCallbacks creates completion callbacks using the CHASM implementation.
//
// Like AddCompletionCallbacks, this only applies the event; the limits live on the write path.
func (w *Workflow) AddUpdateCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	updateID string,
	requestID string,
	completionCallbacks []*commonpb.Callback,
) error {
	// Plan against the update's existing callbacks, so a retry that re-derives keys it already
	// wrote contributes nothing to the accounting.
	var existing chasm.Map[string, *callback.Callback]
	if updateField, ok := w.Updates[updateID]; ok {
		existing = updateField.Get(ctx).Callbacks
	}
	pending, err := planCallbackInsertions(existing, requestID, completionCallbacks)
	if err != nil {
		return err
	}
	currentCount, currentSize := w.CallbackTotals(ctx)

	if w.Updates == nil {
		w.Updates = make(chasm.Map[string, *WorkflowUpdate], 1)
	}
	if _, ok := w.Updates[updateID]; !ok {
		workflowUpdateObj := NewWorkflowUpdate(ctx, updateID, w.MSPointer)
		workflowUpdateObj.Callbacks = make(chasm.Map[string, *callback.Callback], len(pending))
		w.Updates[updateID] = chasm.NewComponentField(ctx, workflowUpdateObj)
	}
	update := w.Updates[updateID].Get(ctx)

	insertedSize := applyCallbackInsertions(ctx, update.Callbacks, requestID, eventTime, pending)
	w.recordCallbackTotals(ctx, currentCount+len(pending), currentSize+insertedSize)
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
