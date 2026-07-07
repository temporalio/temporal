package replication

import (
	"errors"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	commonevents "go.temporal.io/server/common/events"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/tasks"
)

// emitReplicationExecuting emits a best-effort "executing" ReplicationLifecycle event when an
// executable replication task is picked up to execute on the target cluster. It never affects
// control flow: a nil handler / unresolved shard is a no-op and namespace resolution failures
// fall back to "".
//
// The event handler and shard id are taken from the target shard (resolved by namespace+workflow),
// because the replication ProcessToolBox does not carry the event handler in all wirings.
func emitReplicationExecuting(
	toolBox ProcessToolBox,
	task *replicationspb.ReplicationTask,
	key definition.WorkflowKey,
	taskType string,
	attempt int32,
) {
	if !toolBox.Config.EmitReplicationLifecycleEvents() {
		return
	}
	shardContext, err := toolBox.ShardController.GetShardByNamespaceWorkflow(namespace.ID(key.NamespaceID), key.WorkflowID)
	if err != nil {
		return
	}
	handler := shardContext.GetEventHandler()
	if handler == nil {
		return
	}

	var nsName string
	if name, err := toolBox.NamespaceCache.GetNamespaceName(namespace.ID(key.NamespaceID)); err == nil {
		nsName = name.String()
	}

	payload := commonevents.ReplicationLifecyclePayload{
		Phase:       commonevents.ReplicationExecuting,
		TaskType:    taskType,
		Shard:       shardContext.GetShardID(),
		Namespace:   nsName,
		NamespaceID: key.NamespaceID,
		WorkflowID:  key.WorkflowID,
		RunID:       key.RunID,
		Attempt:     attempt,
	}
	// Record what this attempt will try to apply, taken from the task itself: its target versioned
	// transition and, for verify, the expected event version history. This lets a reader correlate
	// the executing attempt with the eventual applied outcome and history branch.
	if vt := task.GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}
	if attr := task.GetVerifyVersionedTransitionTaskAttributes(); attr != nil {
		if items := attr.GetEventVersionHistory(); len(items) > 0 {
			payload.EventVersionHistory = versionHistoryEntries(items)
		}
	}
	commonevents.EmitReplicationLifecycle(handler, payload)
}

// emitReplicationSent emits a best-effort "sent" ReplicationLifecycle event for the supported
// replication task types. It never affects control flow.
func (s *StreamSenderImpl) emitReplicationSent(
	task *replicationspb.ReplicationTask,
	item tasks.Task,
) {
	if !s.config.EmitReplicationLifecycleEvents() {
		return
	}
	handler := s.shardContext.GetEventHandler()
	if handler == nil {
		return
	}

	var taskType string
	switch task.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		taskType = commonevents.ReplTaskSyncWorkflowState
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		taskType = commonevents.ReplTaskSyncVersionedTransition
	case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
		taskType = commonevents.ReplTaskVerifyVersionedTransition
	default:
		return
	}

	nsID := item.GetNamespaceID()
	nsName, err := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if err != nil {
		nsName = namespace.EmptyName
	}

	payload := commonevents.ReplicationLifecyclePayload{
		Phase:       commonevents.ReplicationSent,
		TaskType:    taskType,
		Shard:       s.serverShardKey.ShardID,
		Namespace:   nsName.String(),
		NamespaceID: nsID,
		WorkflowID:  item.GetWorkflowID(),
		RunID:       item.GetRunID(),
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

	// parent info, extracted from the mutable state carried in the task payload (child->parent
	// only). The verify task ships no mutable state, so it contributes no parent info here.
	populateSentParentInfo(&payload, task)

	commonevents.EmitReplicationLifecycle(handler, payload)
}

// populateSentParentInfo records the child->parent identity from the mutable state carried in the
// task payload, when present. Task types that ship no mutable state (e.g. verify) are a no-op.
func populateSentParentInfo(payload *commonevents.ReplicationLifecyclePayload, task *replicationspb.ReplicationTask) {
	switch task.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		if ms := task.GetSyncWorkflowStateTaskAttributes().GetWorkflowState(); ms != nil {
			payload.PopulateParentInfo(ms.GetExecutionInfo())
		}
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		art := task.GetSyncVersionedTransitionTaskAttributes().GetVersionedTransitionArtifact()
		if art == nil {
			return
		}
		if snap := art.GetSyncWorkflowStateSnapshotAttributes(); snap != nil {
			if ms := snap.GetState(); ms != nil {
				payload.PopulateParentInfo(ms.GetExecutionInfo())
			}
		} else if mut := art.GetSyncWorkflowStateMutationAttributes(); mut != nil {
			if m := mut.GetStateMutation(); m != nil {
				payload.PopulateParentInfo(m.GetExecutionInfo())
			}
		}
	default:
	}
}

// emitReplicationVerifyApplied emits a best-effort "applied" ReplicationLifecycle event for a
// verify task. Outcome is "verified" when verification passed (no error) and "resend_needed" when
// the task requested a state resend; any other error is reported with its message. ms may be nil
// (e.g. the workflow was not found) in which case only identity fields are populated.
func (e *ExecutableVerifyVersionedTransitionTask) emitReplicationVerifyApplied(
	ms *persistencespb.WorkflowMutableState,
	retErr error,
) {
	if !e.Config.EmitReplicationLifecycleEvents() {
		return
	}
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(namespace.ID(e.NamespaceID), e.WorkflowID)
	if err != nil {
		return
	}
	handler := shardContext.GetEventHandler()
	if handler == nil {
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

	payload := commonevents.ReplicationLifecyclePayload{
		Phase:       commonevents.ReplicationApplied,
		TaskType:    commonevents.ReplTaskVerifyVersionedTransition,
		Shard:       shardContext.GetShardID(),
		Namespace:   nsName,
		NamespaceID: e.NamespaceID,
		WorkflowID:  e.WorkflowID,
		RunID:       e.RunID,
		Outcome:     outcome,
		Error:       errStr,
	}
	if vt := e.ReplicationTask().GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}

	// For verify, record an expected-vs-actual comparison in Details rather than a mutable-state
	// summary. When verify triggers a resend, the state replicator emits its own applied event with
	// the full summary, so repeating it here would only duplicate. "expected" is what the task
	// requires the passive to have; "actual" is what the inspected mutable state currently has.
	details := map[string]any{"expected": e.verifyExpected()}
	if ms != nil {
		details["actual"] = verifyActual(ms)
	}
	payload.Details = details

	commonevents.EmitReplicationLifecycle(handler, payload)
}

// verifyExpected describes what the verify task requires the passive to have: the target versioned
// transition, next event id, and expected event version history.
func (e *ExecutableVerifyVersionedTransitionTask) verifyExpected() map[string]any {
	expected := map[string]any{"next_event_id": e.taskAttr.GetNextEventId()}
	if vt := e.ReplicationTask().GetVersionedTransition(); vt != nil {
		expected["versioned_transition"] = versionedTransitionEntry(vt)
	}
	if items := e.taskAttr.GetEventVersionHistory(); len(items) > 0 {
		expected["event_version_history"] = versionHistoryEntries(items)
	}
	return expected
}

// verifyActual describes what the passive's inspected mutable state currently has.
func verifyActual(ms *persistencespb.WorkflowMutableState) map[string]any {
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

func versionedTransitionEntry(vt *persistencespb.VersionedTransition) commonevents.VersionedTransitionEntry {
	return commonevents.VersionedTransitionEntry{
		FailoverVersion: vt.GetNamespaceFailoverVersion(),
		TransitionCount: vt.GetTransitionCount(),
	}
}

func versionHistoryEntries(items []*historyspb.VersionHistoryItem) []commonevents.VersionHistoryEntry {
	out := make([]commonevents.VersionHistoryEntry, 0, len(items))
	for _, item := range items {
		out = append(out, commonevents.VersionHistoryEntry{EventID: item.GetEventId(), Version: item.GetVersion()})
	}
	return out
}
