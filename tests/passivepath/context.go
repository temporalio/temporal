package passivepath

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
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
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
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
			if err := h.comparePassiveState(
				request.ExecutionContext.GetWorkflowKey(),
				request.ExecutionContext.MutableState.CloneToProto(),
			); err != nil {
				return err
			}
			if err := h.comparePassiveTasks(
				request.ExecutionContext.GetWorkflowKey(),
				request.ExecutionContext.MutableState,
				transactionPayload.ExecutionMutation.Tasks,
			); err != nil {
				return err
			}
		}
		if request.NewMutableState != nil && transactionPayload.NewExecutionSnapshot != nil {
			if err := h.comparePassiveState(
				request.NewMutableState.GetWorkflowKey(),
				request.NewMutableState.CloneToProto(),
			); err != nil {
				return err
			}
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
	mutableState := request.ExecutionContext.MutableState
	if mutableState == nil {
		return delegate(BailNoMutableState)
	}
	if len(mutableState.GetExecutionInfo().TransitionHistory) == 0 {
		return delegate(BailNoTransitionHistory)
	}
	hasBufferedEvents := mutableState.HasBufferedEvents()
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
	expectedState := mutableState.CloneToProto()
	replicationTask, err := syncVersionedTransitionTask(activeMutation.Tasks)
	if err != nil {
		return err
	}
	if transitionhistory.Compare(
		replicationTask.VersionedTransition,
		mutableState.CurrentVersionedTransition(),
	) != 0 {
		return fmt.Errorf(
			"passivepath: replication task transition %v does not match mutable state transition %v",
			replicationTask.VersionedTransition,
			mutableState.CurrentVersionedTransition(),
		)
	}
	if hasBufferedEvents || mutableState.HasBufferedEvents() {
		h.recordBailout(BailBufferedEvents)
		return request.ExecuteExecutionTransaction(transactionPayload)
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
		replicationTask.VersionedTransition,
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
		h.expectPassiveState(request.NewMutableState.GetWorkflowKey(), request.NewMutableState.CloneToProto(), true)
		h.expectPassiveTasks(
			request.NewMutableState.GetWorkflowKey(),
			tasksWithoutReplication(transactionPayload.NewExecutionSnapshot.Tasks),
		)
	}
	h.expectPassiveState(request.ExecutionContext.GetWorkflowKey(), expectedState, false)
	h.expectPassiveTasks(
		request.ExecutionContext.GetWorkflowKey(),
		tasksWithoutReplication(activeMutation.Tasks),
	)

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
	if err := h.comparePersistedMutableState(
		ctx,
		request.ShardContext,
		request.ExecutionContext.GetArchetypeID(),
		request.ExecutionContext.GetWorkflowKey(),
		expectedState,
	); err != nil {
		h.recordApplyError(err)
		return err
	}
	h.recordApplied()
	return nil
}

func syncVersionedTransitionTask(
	tasksByCategory map[historytasks.Category][]historytasks.Task,
) (*historytasks.SyncVersionedTransitionTask, error) {
	var result *historytasks.SyncVersionedTransitionTask
	for _, task := range tasksByCategory[historytasks.CategoryReplication] {
		syncTask, ok := task.(*historytasks.SyncVersionedTransitionTask)
		if !ok {
			continue
		}
		if result != nil {
			return nil, errors.New("passivepath: active transaction generated multiple sync versioned transition tasks")
		}
		result = syncTask
	}
	if result == nil {
		return nil, errors.New("passivepath: active transaction generated no sync versioned transition task")
	}
	return result, nil
}

// tasksWithoutReplication removes the active-only replication envelope before task
// parity is checked. The passive close must regenerate every other task category.
func tasksWithoutReplication(
	tasksByCategory map[historytasks.Category][]historytasks.Task,
) map[historytasks.Category][]historytasks.Task {
	result := maps.Clone(tasksByCategory)
	delete(result, historytasks.CategoryReplication)
	return result
}

func (h *Harness) comparePersistedMutableState(
	ctx context.Context,
	shardContext historyi.ShardContext,
	archetypeID chasm.ArchetypeID,
	workflowKey definition.WorkflowKey,
	expected *persistencespb.WorkflowMutableState,
) error {
	response, err := shardContext.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardContext.GetShardID(),
		NamespaceID: workflowKey.NamespaceID,
		WorkflowID:  workflowKey.WorkflowID,
		RunID:       workflowKey.RunID,
		ArchetypeID: archetypeID,
	})
	if err != nil {
		return fmt.Errorf("passivepath: load passively persisted mutable state for %s: %w", workflowKey.String(), err)
	}

	if diff := mutableStateDiff(expected, response.State); diff != "" {
		return fmt.Errorf(
			"passivepath: active/passive mutable state differs for %s (-active +passive):\n%s",
			workflowKey.String(), diff,
		)
	}
	return nil
}

func mutableStateDiff(expected, actual *persistencespb.WorkflowMutableState) string {
	return mutableStateDiffWithOptions(expected, actual, false)
}

func mutableStateDiffWithOptions(
	expected *persistencespb.WorkflowMutableState,
	actual *persistencespb.WorkflowMutableState,
	rebuiltFromEvents bool,
) string {
	expected = proto.Clone(expected).(*persistencespb.WorkflowMutableState)
	actual = proto.Clone(actual).(*persistencespb.WorkflowMutableState)
	normalizeMutableStateForComparison(expected)
	normalizeMutableStateForComparison(actual)
	if rebuiltFromEvents {
		normalizeEventRebuiltMutableStateForComparison(expected)
		normalizeEventRebuiltMutableStateForComparison(actual)
	}
	return cmp.Diff(expected, actual, protocmp.Transform())
}

func normalizeEventRebuiltMutableStateForComparison(state *persistencespb.WorkflowMutableState) {
	info := state.ExecutionInfo
	if info != nil {
		for _, versionHistory := range info.GetVersionHistories().GetHistories() {
			versionHistory.BranchToken = nil
		}
		info.SubStateMachineTombstoneBatches = slices.DeleteFunc(
			info.SubStateMachineTombstoneBatches,
			func(batch *persistencespb.StateMachineTombstoneBatch) bool {
				return len(batch.GetStateMachineTombstones()) == 0
			},
		)
		// Rebuilding the first event initializes this task-refresh watermark locally.
		info.VisibilityLastUpdateVersionedTransition = nil
		// The active close and event rebuild derive this timestamp independently and can
		// differ at sub-millisecond precision without changing workflow semantics.
		info.WorkflowTaskOriginalScheduledTime = nil
	}
	if executionState := state.ExecutionState; executionState != nil {
		delete(executionState.RequestIds, executionState.CreateRequestId)
		executionState.CreateRequestId = ""
	}
}

func normalizeMutableStateForComparison(state *persistencespb.WorkflowMutableState) {
	// SignalRequestedIds represents a set even though persistence encodes it as a list.
	slices.Sort(state.SignalRequestedIds)

	info := state.ExecutionInfo
	if info == nil {
		return
	}
	// Reuse the production definition of cluster- and shard-local mutable state.
	workflow.SanitizeMutableState(state)
	for _, activityInfo := range state.ActivityInfos {
		activityInfo.TimerTaskStatus = 0
	}
	for _, timerInfo := range state.TimerInfos {
		timerInfo.TaskStatus = 0
	}
	// These values describe the local persistence transaction rather than replicated
	// workflow state. Closing the passive apply necessarily assigns them again.
	info.LastUpdateTime = nil
	info.StateTransitionCount = 0
	// Sticky queues are local worker routing state and are deliberately cleared when
	// mutable state is synchronized to another cluster.
	info.StickyTaskQueue = ""
	info.StickyScheduleToStartTimeout = nil
	if info.WorkflowTaskStartedEventId == 0 {
		info.WorkflowTaskStartedTime = nil
	}
	if info.ExecutionStats != nil {
		info.ExecutionStats.HistorySize = 0
	}
	if len(info.AutoResetPoints.GetPoints()) == 0 {
		info.AutoResetPoints = nil
	}
}

func (h *Harness) buildArtifact(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	mutableState historyi.MutableState,
	exclusiveStart *persistencespb.VersionedTransition,
	replicationTaskTransition *persistencespb.VersionedTransition,
	eventsSeq []*persistence.WorkflowEvents,
) (*replicationspb.VersionedTransitionArtifact, error) {
	// SyncStateRetriever normally loads the already-persisted successor run when this
	// field is set. In this test hook the successor is still only in memory, and its
	// first event batch is supplied by InterceptUpdate below instead. Hide the ID from
	// that lookup, then restore it on both mutable state and the generated artifact.
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
		h.artifactStartTransition(exclusiveStart),
		nil,
		wcache.NoopReleaseFn,
	)
	if err != nil {
		return nil, err
	}
	if artifactTransition := transitionhistory.LastVersionedTransition(result.VersionedTransitionHistory); transitionhistory.Compare(
		artifactTransition,
		replicationTaskTransition,
	) != 0 {
		return nil, fmt.Errorf(
			"passivepath: artifact transition %v does not match replication task transition %v",
			artifactTransition,
			replicationTaskTransition,
		)
	}
	artifact := result.VersionedTransitionArtifact
	if successorRunID != "" {
		switch {
		case artifact.GetSyncWorkflowStateMutationAttributes() != nil:
			mutation := artifact.GetSyncWorkflowStateMutationAttributes().GetStateMutation()
			if mutation.GetExecutionInfo() == nil {
				return nil, fmt.Errorf("passivepath: mutation for %s has no execution info", workflowKey.String())
			}
			mutation.ExecutionInfo.SuccessorRunId = successorRunID
		case artifact.GetSyncWorkflowStateSnapshotAttributes() != nil:
			snapshot := artifact.GetSyncWorkflowStateSnapshotAttributes().GetState()
			if snapshot.GetExecutionInfo() == nil {
				return nil, fmt.Errorf("passivepath: snapshot for %s has no execution info", workflowKey.String())
			}
			snapshot.ExecutionInfo.SuccessorRunId = successorRunID
		default:
			return nil, fmt.Errorf("passivepath: artifact for %s has no state", workflowKey.String())
		}
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
