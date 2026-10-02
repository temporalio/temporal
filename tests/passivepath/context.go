package passivepath

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const applyTimeout = 30 * time.Second

type replicationApplyContextKey struct{}

type replicationApplyContext struct{}

var _ testhooks.HistoryPassiveReplicationTestHook = (*Harness)(nil)

// InterceptUpdate diverts supported active updates into a replication artifact and
// applies it synchronously while the caller's workflow cache lease remains held.
func (h *Harness) InterceptUpdate(
	ctx context.Context,
	payload any,
	next func() error,
) (retErr error) {
	h.recordIntercepted()

	request, ok := payload.(*workflow.TestHookUpdateExecutionRequest)
	if !ok {
		return fmt.Errorf("passivepath: unexpected update hook payload %T", payload)
	}
	defer func() {
		if retErr != nil {
			request.ExecutionContext.Clear()
			if request.NewContext != nil {
				request.NewContext.Clear()
			}
		}
	}()
	delegate := func(reason BailReason) error {
		h.recordBailout(reason)
		return next()
	}

	if request.UpdateExecutionTransactionPolicy != historyi.TransactionPolicyActive {
		h.recordBailout(BailPassivePolicy)
		if err := request.PrepareMutableStateTransaction(); err != nil {
			return err
		}
		transactionPayload, err := request.CloseMutableStateTransaction()
		if err != nil {
			return err
		}
		if request.ExecutionContext != nil {
			if err := h.comparePassiveTasks(
				request.ExecutionContext.GetWorkflowKey(),
				request.ExecutionContext.MutableState,
				transactionPayload.ExecutionMutation.Tasks,
			); err != nil {
				return err
			}
		}
		if request.NewMutableState != nil && transactionPayload.NewExecutionSnapshot != nil {
			if err := h.comparePassiveTasks(
				request.NewMutableState.GetWorkflowKey(),
				request.NewMutableState,
				transactionPayload.NewExecutionSnapshot.Tasks,
			); err != nil {
				return err
			}
		}
		return request.ExecuteExecutionTransaction(transactionPayload)
	}
	newRun := request.NewContext != nil || request.NewMutableState != nil || request.NewExecutionTransactionPolicy != nil
	if newRun && (request.NewContext == nil || request.NewMutableState == nil ||
		request.NewExecutionTransactionPolicy == nil ||
		*request.NewExecutionTransactionPolicy != historyi.TransactionPolicyActive) {
		return delegate(BailNewRun)
	}
	if request.UpdateMode != persistence.UpdateWorkflowModeUpdateCurrent {
		return delegate(BailUpdateMode)
	}

	mutableState := request.ExecutionContext.MutableState
	if mutableState == nil {
		return delegate(BailNoMutableState)
	}
	if len(mutableState.GetExecutionInfo().TransitionHistory) == 0 {
		return delegate(BailNoTransitionHistory)
	}
	if mutableState.HasBufferedEvents() {
		return delegate(BailBufferedEvents)
	}

	exclusiveStart := transitionhistory.CopyVersionedTransition(mutableState.CurrentVersionedTransition())
	if exclusiveStart == nil {
		return delegate(BailNoTransitionHistory)
	}

	if err := request.PrepareMutableStateTransaction(); err != nil {
		return err
	}
	transactionPayload, err := request.CloseMutableStateTransaction()
	if err != nil {
		return err
	}
	activeMutation := transactionPayload.ExecutionMutation
	eventsSeq := transactionPayload.ExecutionEvents
	if mutableState.HasBufferedEvents() {
		return errors.New("passivepath: mutable state has buffered events after close")
	}
	if activeMutation.ClearBufferedEvents {
		h.recordBailout(BailClearBufferedEvents)
		return request.ExecuteExecutionTransaction(transactionPayload)
	}

	artifact, err := h.buildArtifact(
		ctx,
		request.ShardContext,
		request.ExecutionContext.GetWorkflowKey(),
		mutableState,
		exclusiveStart,
		eventsSeq,
	)
	if err != nil {
		return err
	}
	if newRun {
		newRunEventBatches, err := h.serializeEvents(transactionPayload.NewExecutionEvents)
		if err != nil {
			return err
		}
		if len(newRunEventBatches) == 0 {
			return errors.New("passivepath: new run has no initial event batch")
		}
		artifact.NewRunInfo = &replicationspb.NewRunInfo{
			RunId:      request.NewMutableState.GetExecutionState().GetRunId(),
			EventBatch: newRunEventBatches[0],
		}
		h.expectPassiveTasks(request.NewMutableState.GetWorkflowKey(), transactionPayload.NewExecutionSnapshot.Tasks)
	}
	h.expectPassiveTasks(request.ExecutionContext.GetWorkflowKey(), activeMutation.Tasks)

	workflowKeys := []definition.WorkflowKey{request.ExecutionContext.GetWorkflowKey()}
	if newRun {
		workflowKeys = append(workflowKeys, definition.NewWorkflowKey(
			request.ExecutionContext.GetWorkflowKey().NamespaceID,
			request.ExecutionContext.GetWorkflowKey().WorkflowID,
			request.NewMutableState.GetExecutionState().GetRunId(),
		))
	}
	h.recordDiverted(workflowKeys...)
	err = h.apply(
		ctx,
		request.ShardContext,
		request.ExecutionContext.GetArchetypeID(),
		artifact,
	)
	if err != nil {
		h.recordApplyError(err)
		return err
	}
	h.recordApplied()
	return nil
}

func (h *Harness) buildArtifact(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	mutableState historyi.MutableState,
	exclusiveStart *persistencespb.VersionedTransition,
	eventsSeq []*persistence.WorkflowEvents,
) (*replicationspb.VersionedTransitionArtifact, error) {
	// SyncStateRetriever normally loads the already-persisted successor run when this
	// field is set. In this test hook the successor is still only in memory, and its
	// first event batch is supplied by InterceptUpdate below instead. Hide the ID from
	// that lookup, then restore it on both mutable state and the generated mutation.
	successorRunID := mutableState.GetExecutionInfo().GetSuccessorRunId()
	mutableState.GetExecutionInfo().SuccessorRunId = ""
	defer func() {
		mutableState.GetExecutionInfo().SuccessorRunId = successorRunID
	}()
	result, err := h.newRetriever(shardContext).GetSyncWorkflowStateArtifactFromMutableState(
		ctx,
		workflowKey.NamespaceID,
		&commonpb.WorkflowExecution{WorkflowId: workflowKey.WorkflowID, RunId: workflowKey.RunID},
		mutableState,
		exclusiveStart,
		nil,
		wcache.NoopReleaseFn,
	)
	if err != nil {
		return nil, err
	}
	artifact := result.VersionedTransitionArtifact
	if artifact.GetSyncWorkflowStateMutationAttributes() == nil {
		return nil, fmt.Errorf("passivepath: expected mutation artifact for %s, got snapshot", workflowKey.String())
	}
	if successorRunID != "" {
		mutation := artifact.GetSyncWorkflowStateMutationAttributes().GetStateMutation()
		if mutation.GetExecutionInfo() == nil {
			return nil, fmt.Errorf("passivepath: mutation for %s has no execution info", workflowKey.String())
		}
		mutation.ExecutionInfo.SuccessorRunId = successorRunID
	}
	eventBatches, err := h.serializeEvents(eventsSeq)
	if err != nil {
		return nil, err
	}
	artifact.EventBatches = eventBatches

	var firstID, lastID int64
	var count int
	var eventTypes []string
	for _, batch := range eventsSeq {
		for _, event := range batch.Events {
			if firstID == 0 || event.GetEventId() < firstID {
				firstID = event.GetEventId()
			}
			if event.GetEventId() > lastID {
				lastID = event.GetEventId()
			}
			count++
			eventTypes = append(eventTypes, event.GetEventType().String())
		}
	}
	h.logArtifactEvents(workflowKey, count, firstID, lastID, mutableState.GetNextEventID(), eventTypes)
	return artifact, nil
}

func (h *Harness) serializeEvents(eventsSeq []*persistence.WorkflowEvents) ([]*commonpb.DataBlob, error) {
	var blobs []*commonpb.DataBlob
	for _, events := range eventsSeq {
		if len(events.Events) == 0 {
			continue
		}
		blob, err := h.serializer.SerializeEvents(events.Events)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, blob)
	}
	return blobs, nil
}

// UseTransientWorkflowContextForReplication scopes the uncached passive context to
// the synchronous replication call made by this harness.
func (h *Harness) UseTransientWorkflowContextForReplication(ctx context.Context) bool {
	_, ok := ctx.Value(replicationApplyContextKey{}).(replicationApplyContext)
	return ok
}

func (h *Harness) apply(
	ctx context.Context,
	shardContext historyi.ShardContext,
	archetypeID chasm.ArchetypeID,
	artifact *replicationspb.VersionedTransitionArtifact,
) error {
	ctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, replicationApplyContextKey{}, replicationApplyContext{})

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	err = engine.ReplicateVersionedTransition(
		ctx,
		archetypeID,
		artifact,
		shardContext.GetClusterMetadata().GetCurrentClusterName(),
	)
	if errors.Is(err, consts.ErrDuplicate) {
		return nil
	}
	return err
}
