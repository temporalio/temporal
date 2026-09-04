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

// totalCallbackCount returns the total number of callbacks across workflow-level
// and all update-level callback maps.
func (w *Workflow) totalCallbackCount(ctx chasm.Context) int {
	count := len(w.Callbacks)
	for _, updateField := range w.Updates {
		count += len(updateField.Get(ctx).Callbacks)
	}
	return count
}

// checkWorkflowCallbackLimit returns an error if adding newCount callbacks would
// exceed the per-workflow maximum.
func (w *Workflow) checkWorkflowCallbackLimit(ctx chasm.Context, newCount, maxCallbacksPerWorkflow int) error {
	current := w.totalCallbackCount(ctx)
	if newCount+current > maxCallbacksPerWorkflow {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to a workflow (%d callbacks already attached)",
			maxCallbacksPerWorkflow,
			current,
		)
	}
	return nil
}

// callbackID returns a stable key for idempotent attachment. CHASM replicates
// the tree, so the key does not need an event version.
func callbackID(requestID string, index int) string {
	return fmt.Sprintf("%s-%d", requestID, index)
}

// countNewCallbacks excludes callbacks stored by an earlier request attempt.
func countNewCallbacks(
	target chasm.Map[string, *callback.Callback],
	requestID string,
	callbackCount int,
) int {
	count := 0
	for index := range callbackCount {
		if _, exists := target[callbackID(requestID, index)]; !exists {
			count++
		}
	}
	return count
}

// convertCallbacks validates all callbacks before its caller changes workflow state.
func convertCallbacks(completionCallbacks []*commonpb.Callback) ([]*callbackspb.Callback, error) {
	chasmCBs := make([]*callbackspb.Callback, len(completionCallbacks))
	for i, cb := range completionCallbacks {
		chasmCB := &callbackspb.Callback{Links: cb.GetLinks()}
		switch variant := cb.Variant.(type) {
		case *commonpb.Callback_Nexus_:
			chasmCB.Variant = &callbackspb.Callback_Nexus_{
				Nexus: &callbackspb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			return nil, serviceerror.NewInvalidArgumentf("unsupported callback variant: %T", variant)
		}
		chasmCBs[i] = chasmCB
	}
	return chasmCBs, nil
}

func addCallbacksToMap(
	ctx chasm.MutableContext,
	target chasm.Map[string, *callback.Callback],
	requestID string,
	eventTime *timestamppb.Timestamp,
	callbacks []*callbackspb.Callback,
) {
	for index, chasmCB := range callbacks {
		id := callbackID(requestID, index)
		if _, exists := target[id]; exists {
			continue
		}
		callbackObj := callback.NewCallback(requestID, eventTime, &callbackspb.CallbackState{}, chasmCB)
		target[id] = chasm.NewComponentField(ctx, callbackObj)
	}
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
	callbacks, err := convertCallbacks(completionCallbacks)
	if err != nil {
		return err
	}
	newCallbackCount := countNewCallbacks(w.Callbacks, requestID, len(callbacks))
	if err := w.checkWorkflowCallbackLimit(ctx, newCallbackCount, maxCallbacksPerWorkflow); err != nil {
		return err
	}

	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback], len(completionCallbacks))
	}

	addCallbacksToMap(ctx, w.Callbacks, requestID, eventTime, callbacks)
	return nil
}

// AddUpdateCompletionCallbacks creates completion callbacks using the CHASM implementation.
// maxCallbacksPerWorkflow is the configured maximum number of callbacks allowed per workflow.
// maxCallbacksPerUpdateID is the configured maximum number of callbacks allowed per update ID.
func (w *Workflow) AddUpdateCompletionCallbacks(
	ctx chasm.MutableContext,
	eventTime *timestamppb.Timestamp,
	updateID string,
	requestID string,
	completionCallbacks []*commonpb.Callback,
	maxCallbacksPerWorkflow int,
	maxCallbacksPerUpdateID int,
) error {
	callbacks, err := convertCallbacks(completionCallbacks)
	if err != nil {
		return err
	}

	var update *WorkflowUpdate
	if updateField, exists := w.Updates[updateID]; exists {
		update = updateField.Get(ctx)
	}

	currentCallbackCount := 0
	var updateCallbacks chasm.Map[string, *callback.Callback]
	if update != nil {
		updateCallbacks = update.Callbacks
		currentCallbackCount = len(updateCallbacks)
	}
	newCallbackCount := countNewCallbacks(updateCallbacks, requestID, len(callbacks))
	if err := w.checkWorkflowCallbackLimit(ctx, newCallbackCount, maxCallbacksPerWorkflow); err != nil {
		return err
	}
	if newCallbackCount+currentCallbackCount > maxCallbacksPerUpdateID {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d callbacks to update %q (%d callbacks already attached)",
			maxCallbacksPerUpdateID,
			updateID,
			currentCallbackCount,
		)
	}

	if update == nil {
		if w.Updates == nil {
			w.Updates = make(chasm.Map[string, *WorkflowUpdate], 1)
		}
		update = NewWorkflowUpdate(ctx, updateID, w.MSPointer)
		update.Callbacks = make(chasm.Map[string, *callback.Callback], len(callbacks))
		w.Updates[updateID] = chasm.NewComponentField(ctx, update)
	} else if update.Callbacks == nil {
		update.Callbacks = make(chasm.Map[string, *callback.Callback], len(callbacks))
	}

	addCallbacksToMap(ctx, update.Callbacks, requestID, eventTime, callbacks)
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
