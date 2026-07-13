package replication

import (
	"errors"
	"maps"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
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
		Phase:       wideevents.ReplicationExecuting,
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
	if items := task.GetVerifyVersionedTransitionTaskAttributes().GetEventVersionHistory(); len(items) > 0 {
		payload.EventVersionHistory = versionHistoryEntries(items)
	}
	applyReplicationDetails(shardContext, &payload)
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
	default:
		return
	}

	nsID := item.GetNamespaceID()
	nsName, err := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if err != nil {
		nsName = namespace.EmptyName
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:        wideevents.ReplicationSent,
		TaskType:     taskType,
		Shard:        s.serverShardKey.ShardID,
		Namespace:    nsName.String(),
		NamespaceID:  nsID,
		WorkflowID:   item.GetWorkflowID(),
		RunID:        item.GetRunID(),
		SourceTaskID: item.GetTaskID(),
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

	applyReplicationDetails(s.shardContext, &payload)
	wideevents.Emit(logger, payload)
}

// populateSentParentInfo records the child->parent identity from the mutable state carried in the
// task payload, when present. Task types that ship no mutable state (e.g. verify) are a no-op.
func populateSentParentInfo(payload *wideevents.ReplicationLifecyclePayload, task *replicationspb.ReplicationTask) {
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
		Phase:       wideevents.ReplicationApplied,
		TaskType:    wideevents.ReplTaskVerifyVersionedTransition,
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
	details := map[string]any{"expected": e.expectedState()}
	if ms != nil {
		details["actual"] = actualState(ms)
	}
	payload.Details = details

	applyReplicationDetails(shardContext, &payload)
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

// applyReplicationDetails merges deployment-specific details from the shard's ReplicationDetailer
// (if configured) into the payload's Details before it is emitted. A nil detailer or empty result
// is a no-op.
func applyReplicationDetails(shardContext historyi.ShardContext, payload *wideevents.ReplicationLifecyclePayload) {
	detailer := shardContext.GetReplicationDetailer()
	if detailer == nil {
		return
	}
	extra := detailer.ReplicationDetails(payload.NamespaceID, payload.Namespace, payload.WorkflowID, payload.RunID)
	if len(extra) == 0 {
		return
	}
	if payload.Details == nil {
		payload.Details = make(map[string]any, len(extra))
	}
	maps.Copy(payload.Details, extra)
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
