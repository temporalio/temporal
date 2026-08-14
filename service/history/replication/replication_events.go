package replication

import (
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/tasks"
)

// emitReplicationExecuting emits a best-effort "executing" ReplicationLifecycle event when an
// executable replication task is picked up to execute on the target cluster. It never affects
// control flow: a nil logger / unresolved shard is a no-op and namespace resolution failures
// fall back to "".
//
// The event logger and shard id are taken from the target shard (resolved by namespace+workflow),
// because the replication ProcessToolBox does not carry the event logger in all wirings.
func emitReplicationExecuting(
	toolBox ProcessToolBox,
	task *replicationspb.ReplicationTask,
	key definition.WorkflowKey,
	taskType string,
	attempt int32,
	sourceCluster string,
	sourceShard int32,
) {
	shardContext, err := toolBox.ShardController.GetShardByNamespaceWorkflow(namespace.ID(key.NamespaceID), key.WorkflowID)
	if err != nil {
		return
	}
	logger := shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	var nsName string
	if name, err := toolBox.NamespaceCache.GetNamespaceName(namespace.ID(key.NamespaceID)); err == nil {
		nsName = name.String()
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:         wideevents.ReplicationExecuting,
		TaskType:      taskType,
		Shard:         shardContext.GetShardID(),
		Namespace:     nsName,
		NamespaceID:   key.NamespaceID,
		WorkflowID:    key.WorkflowID,
		RunID:         key.RunID,
		Attempt:       attempt,
		SourceCluster: sourceCluster,
		SourceShard:   sourceShard,
		SourceTaskID:  task.GetSourceTaskId(),
	}
	// Record what this attempt will try to apply, taken from the task itself: its target versioned
	// transition and, for verify, the expected event version history. This lets a reader correlate
	// the executing attempt with the eventual applied outcome and history branch.
	if vt := task.GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}
	if items := task.GetVerifyVersionedTransitionTaskAttributes().GetEventVersionHistory(); len(items) > 0 {
		payload.EventVersionHistory = versionHistoryEntries(items)
	}
	wideevents.Emit(logger, payload)
}

// emitReplicationSent emits a best-effort "sent" ReplicationLifecycle event for the supported
// replication task types. It never affects control flow.
func (s *StreamSenderImpl) emitReplicationSent(
	task *replicationspb.ReplicationTask,
	item tasks.Task,
) {
	logger := s.shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	var taskType string
	switch task.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		taskType = wideevents.ReplTaskSyncWorkflowState
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		taskType = wideevents.ReplTaskSyncVersionedTransition
	case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
		taskType = wideevents.ReplTaskVerifyVersionedTransition
	case enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK:
		// Carries no state or events, so only the identity below applies.
		taskType = wideevents.ReplTaskDeleteExecution
	default:
		return
	}

	nsID := item.GetNamespaceID()
	nsName, err := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if err != nil {
		nsName = namespace.EmptyName
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:         wideevents.ReplicationSent,
		TaskType:      taskType,
		Shard:         s.serverShardKey.ShardID,
		Namespace:     nsName.String(),
		NamespaceID:   nsID,
		WorkflowID:    item.GetWorkflowID(),
		RunID:         item.GetRunID(),
		SourceCluster: s.shardContext.GetClusterMetadata().GetCurrentClusterName(),
		SourceShard:   s.serverShardKey.ShardID,
		SourceTaskID:  item.GetTaskID(),
		TargetCluster: s.clientClusterName,
		Priority:      task.GetPriority().String(),
	}
	if vt := task.GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}

	if attr := task.GetVerifyVersionedTransitionTaskAttributes(); attr != nil {
		// verify ships no history batch, so use the task's target event id as next_event_id.
		payload.NextEventID = attr.GetNextEventId()
		payload.NewRunID = attr.GetNewRunId()
	}

	if rawTaskInfo := task.GetRawTaskInfo(); rawTaskInfo != nil {
		payload.FirstEventID = rawTaskInfo.GetFirstEventId()
		payload.NextEventID = rawTaskInfo.GetNextEventId()
	} else if svtTask, ok := item.(*tasks.SyncVersionedTransitionTask); ok {
		payload.FirstEventID = svtTask.FirstEventID
		payload.NextEventID = svtTask.NextEventID
	}
	if attr := task.GetSyncVersionedTransitionTaskAttributes(); attr != nil {
		payload.IsFirstSync = attr.GetVersionedTransitionArtifact().GetIsFirstSync()
	}

	// artifact detail, extracted from the mutable state carried in the task payload: the child->parent
	// identity plus what the task actually carried. The verify task ships no mutable state, so it
	// contributes nothing here.
	populateSentArtifactInfo(&payload, task, s.shardContext.GetPayloadSerializer(), s.logger)

	wideevents.Emit(logger, payload)
}

// emitReplicationSkipped emits a best-effort terminal "skipped" ReplicationLifecycle event when the
// sender gives up building ("converting") a task and skips it. It mirrors emitReplicationSent but is
// driven by the raw source task: conversion failed, so there is no built replication task to read
// task-type/versioned-transition/parent detail from. It records the source task type, the
// source_cluster/source_shard/source_task_id join key, the target_cluster now missing the state, the
// priority tier, the exhausted attempt count, and the last convert error. It never affects control
// flow.
func (s *StreamSenderImpl) emitReplicationSkipped(
	item tasks.Task,
	attempt int64,
	priority enumsspb.TaskPriority,
	err error,
) {
	logger := s.shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	nsID := item.GetNamespaceID()
	nsName, nsErr := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if nsErr != nil {
		nsName = namespace.EmptyName
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:    wideevents.ReplicationSkipped,
		TaskType: replicationTaskTypeName(item),
		Shard:    s.serverShardKey.ShardID,
		// SourceCluster/SourceShard/SourceTaskID are the join key back to this cluster's replication
		// queue. TargetCluster identifies the passive this stream feeds; it is recorded because a
		// skipped task emits no downstream event there, so this is the only place the destination is
		// captured — it tells an operator which passive is now missing this task's state.
		SourceCluster: s.shardContext.GetClusterMetadata().GetCurrentClusterName(),
		SourceShard:   s.serverShardKey.ShardID,
		SourceTaskID:  item.GetTaskID(),
		TargetCluster: s.clientClusterName,
		Priority:      priority.String(),
		Namespace:     nsName.String(),
		NamespaceID:   nsID,
		WorkflowID:    item.GetWorkflowID(),
		RunID:         item.GetRunID(),
		Attempt:       int32(attempt),
	}
	if err != nil {
		payload.Error = err.Error()
	}
	wideevents.Emit(logger, payload)
}

// replicationTaskTypeName maps a raw replication-queue task to the ReplicationLifecycle task_type
// vocabulary, so a "skipped" event groups with the same task's sent/executing/applied events under
// one task_type value. It reads the source task's own type because convert failed and produced no
// built replication task to switch on. verify_versioned_transition is intentionally absent: it is
// synthesized during convert and is never a raw queue task, so it cannot be skipped here.
func replicationTaskTypeName(item tasks.Task) string {
	switch item.GetType() {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return wideevents.ReplTaskSyncWorkflowState
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION:
		return wideevents.ReplTaskSyncVersionedTransition
	case enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION:
		return wideevents.ReplTaskDeleteExecution
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return wideevents.ReplTaskHistory
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return wideevents.ReplTaskSyncActivity
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM:
		return wideevents.ReplTaskSyncHSM
	default:
		// A type with no vocabulary mapping still gets an event (a skip is data loss); fall back to
		// the raw enum name rather than dropping it.
		return item.GetType().String()
	}
}

// populateSentArtifactInfo records the child->parent identity and what the task carried, from the
// mutable state in the task payload. Task types that ship no mutable state (e.g. verify) are a no-op.
//
// These bounds are recorded because they cannot be recovered later: LastUpdateVersionedTransition
// stamps are overwritten by subsequent transitions, and the replication progress cache that supplies
// the lower bound of both the state diff and the event range is in-memory and per (run, target).
func populateSentArtifactInfo(
	payload *wideevents.ReplicationLifecyclePayload,
	task *replicationspb.ReplicationTask,
	serializer serialization.Serializer,
	logger log.Logger,
) {
	switch task.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		if ms := task.GetSyncWorkflowStateTaskAttributes().GetWorkflowState(); ms != nil {
			// This task type always carries whole mutable state, so there is no lower bound.
			payload.ArtifactKind = wideevents.ArtifactKindSnapshot
			payload.PopulateParentInfo(ms.GetExecutionInfo())
			payload.HydratedVT = hydratedVersionedTransition(ms.GetExecutionInfo())
		}
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		art := task.GetSyncVersionedTransitionTaskAttributes().GetVersionedTransitionArtifact()
		if art == nil {
			return
		}
		var info *persistencespb.WorkflowExecutionInfo
		if snap := art.GetSyncWorkflowStateSnapshotAttributes(); snap != nil {
			payload.ArtifactKind = wideevents.ArtifactKindSnapshot
			if ms := snap.GetState(); ms != nil {
				info = ms.GetExecutionInfo()
			}
		} else if mut := art.GetSyncWorkflowStateMutationAttributes(); mut != nil {
			payload.ArtifactKind = wideevents.ArtifactKindMutation
			// Exclusive lower bound of the state diff: the target's last-synced versioned transition
			// per the sender's progress cache. Only a mutation carries one.
			if vt := mut.GetExclusiveStartVersionedTransition(); vt != nil {
				payload.ExclusiveStartVT = &wideevents.VersionedTransitionEntry{
					FailoverVersion: vt.GetNamespaceFailoverVersion(),
					TransitionCount: vt.GetTransitionCount(),
				}
			}
			if m := mut.GetStateMutation(); m != nil {
				info = m.GetExecutionInfo()
			}
		}
		if info != nil {
			payload.PopulateParentInfo(info)
			payload.HydratedVT = hydratedVersionedTransition(info)
		}
		payload.ShippedFirstEventID, payload.ShippedFirstEventVersion,
			payload.ShippedLastEventID, payload.ShippedLastEventVersion =
			shippedEventRange(serializer, art.GetEventBatches(), logger)
		// The successor's event 1 rides outside the bounds above; the run id is enough to find it.
		payload.NewRunID = art.GetNewRunInfo().GetRunId()
	default:
	}
}

// hydratedVersionedTransition returns the versioned transition of the mutable state serialized into
// the artifact, i.e. the last entry of its transition history. Nil when transition history is absent
// (state-based replication disabled, or the workflow is in an unknown versioned transition).
func hydratedVersionedTransition(info *persistencespb.WorkflowExecutionInfo) *wideevents.VersionedTransitionEntry {
	vt := transitionhistory.LastVersionedTransition(info.GetTransitionHistory())
	if vt == nil {
		return nil
	}
	return &wideevents.VersionedTransitionEntry{
		FailoverVersion: vt.GetNamespaceFailoverVersion(),
		TransitionCount: vt.GetTransitionCount(),
	}
}

// shippedEventRange reads the inclusive (event id, version) bounds actually carried by the artifact
// from its first and last batch. The versions identify the history branch the events sit on, which
// event ids alone cannot: the same ids exist on both sides of a split brain. Deserialization
// failures yield zeroes rather than an error: this is best-effort instrumentation and must never
// affect the send path.
//
// Decoding is stripped (event_id + version, unknown fields discarded) so the cost does not scale
// with payload size: a full decode of a large batch would copy every payload byte to read two ints.
func shippedEventRange(
	serializer serialization.Serializer,
	batches []*commonpb.DataBlob,
	logger log.Logger,
) (firstEventID int64, firstEventVersion int64, lastEventID int64, lastEventVersion int64) {
	if len(batches) == 0 || serializer == nil {
		return 0, 0, 0, 0
	}
	firstBatch, err := serializer.DeserializeStrippedEvents(batches[0])
	if err != nil || len(firstBatch) == 0 {
		if err != nil && logger != nil {
			logger.Error("replication lifecycle: unable to deserialize first event batch", tag.Error(err))
		}
		return 0, 0, 0, 0
	}
	firstEventID = firstBatch[0].GetEventId()
	firstEventVersion = firstBatch[0].GetVersion()

	lastBatch := firstBatch
	if len(batches) > 1 {
		lastBatch, err = serializer.DeserializeStrippedEvents(batches[len(batches)-1])
		if err != nil || len(lastBatch) == 0 {
			if err != nil && logger != nil {
				logger.Error("replication lifecycle: unable to deserialize last event batch", tag.Error(err))
			}
			// The lower bound is still worth reporting on its own.
			return firstEventID, firstEventVersion, 0, 0
		}
	}
	last := lastBatch[len(lastBatch)-1]
	return firstEventID, firstEventVersion, last.GetEventId(), last.GetVersion()
}

// emitReplicationVerifyApplied emits a best-effort "applied" ReplicationLifecycle event for a
// verify task. Outcome is "verified" when verification passed (no error) and "resend_needed" when
// the task requested a state resend; any other error is reported with its message. ms may be nil
// (e.g. the workflow was not found) in which case only identity fields are populated.
func (e *ExecutableVerifyVersionedTransitionTask) emitReplicationVerifyApplied(
	ms *persistencespb.WorkflowMutableState,
	retErr error,
) {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(namespace.ID(e.NamespaceID), e.WorkflowID)
	if err != nil {
		return
	}
	logger := shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	outcome := "verified"
	errStr := ""
	if retErr != nil {
		var syncStateErr *serviceerrors.SyncState
		if errors.As(retErr, &syncStateErr) {
			outcome = "resend_needed"
		} else {
			outcome = "error"
			errStr = retErr.Error()
		}
	}

	var nsName string
	if name, nsErr := e.NamespaceCache.GetNamespaceName(namespace.ID(e.NamespaceID)); nsErr == nil {
		nsName = name.String()
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:         wideevents.ReplicationApplied,
		TaskType:      wideevents.ReplTaskVerifyVersionedTransition,
		Shard:         shardContext.GetShardID(),
		Namespace:     nsName,
		NamespaceID:   e.NamespaceID,
		WorkflowID:    e.WorkflowID,
		RunID:         e.RunID,
		Outcome:       outcome,
		Error:         errStr,
		SourceCluster: e.SourceClusterName(),
		SourceShard:   e.SourceShardKey().ShardID,
		SourceTaskID:  e.TaskID(),
	}
	if vt := e.ReplicationTask().GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}

	// For verify, record an expected-vs-actual comparison in Details rather than a mutable-state
	// summary. When verify triggers a resend, the state replicator emits its own applied event with
	// the full summary, so repeating it here would only duplicate. "expected" is what the task
	// requires the passive to have; "actual" is what the inspected mutable state currently has.
	details := map[string]any{"expected": e.expectedState()}
	if ms != nil {
		details["actual"] = actualState(ms)
	}
	payload.Details = details

	wideevents.Emit(logger, payload)
}

// expectedState describes what the verify task requires the passive to have: the target versioned
// transition, next event id, and expected event version history.
func (e *ExecutableVerifyVersionedTransitionTask) expectedState() map[string]any {
	expected := map[string]any{"next_event_id": e.taskAttr.GetNextEventId()}
	if vt := e.ReplicationTask().GetVersionedTransition(); vt != nil {
		expected["versioned_transition"] = versionedTransitionEntry(vt)
	}
	if items := e.taskAttr.GetEventVersionHistory(); len(items) > 0 {
		expected["event_version_history"] = versionHistoryEntries(items)
	}
	return expected
}

// actualState describes what the passive's inspected mutable state currently has.
func actualState(ms *persistencespb.WorkflowMutableState) map[string]any {
	info := ms.GetExecutionInfo()
	actual := map[string]any{"next_event_id": ms.GetNextEventId()}
	if th := info.GetTransitionHistory(); len(th) > 0 {
		actual["versioned_transition"] = versionedTransitionEntry(th[len(th)-1])
	}
	if currentHistory, err := versionhistory.GetCurrentVersionHistory(info.GetVersionHistories()); err == nil {
		actual["event_version_history"] = versionHistoryEntries(currentHistory.GetItems())
	}
	return actual
}

func versionedTransitionEntry(vt *persistencespb.VersionedTransition) wideevents.VersionedTransitionEntry {
	return wideevents.VersionedTransitionEntry{
		FailoverVersion: vt.GetNamespaceFailoverVersion(),
		TransitionCount: vt.GetTransitionCount(),
	}
}

func versionHistoryEntries(items []*historyspb.VersionHistoryItem) []wideevents.VersionHistoryEntry {
	out := make([]wideevents.VersionHistoryEntry, 0, len(items))
	for _, item := range items {
		out = append(out, wideevents.VersionHistoryEntry{EventID: item.GetEventId(), Version: item.GetVersion()})
	}
	return out
}
