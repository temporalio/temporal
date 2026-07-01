package replication

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	commonevents "go.temporal.io/server/common/events"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
)

type (
	ExecutableVerifyVersionedTransitionTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask

		taskAttr *replicationspb.VerifyVersionedTransitionTaskAttributes
	}
)

var _ ctasks.Task = (*ExecutableVerifyVersionedTransitionTask)(nil)
var _ TrackableExecutableTask = (*ExecutableVerifyVersionedTransitionTask)(nil)

func NewExecutableVerifyVersionedTransitionTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableVerifyVersionedTransitionTask {
	task := replicationTask.GetVerifyVersionedTransitionTaskAttributes()
	if task.ArchetypeId == chasm.UnspecifiedArchetypeID {
		task.ArchetypeId = chasm.WorkflowArchetypeID
	}
	return &ExecutableVerifyVersionedTransitionTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.VerifyVersionedTransitionTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		),
		taskAttr: task,
	}
}

func (e *ExecutableVerifyVersionedTransitionTask) QueueID() any {
	return e.WorkflowKey
}

func (e *ExecutableVerifyVersionedTransitionTask) Execute() (retErr error) {
	if e.TerminalState() {
		return nil
	}
	e.MarkExecutionStart()

	// TODO: gate behind dynamic config before production
	emitReplicationExecuting(e.ProcessToolBox, e.WorkflowKey, commonevents.ReplTaskVerifyVersionedTransition, int32(e.Attempt()))

	// inspectedMS is the mutable-state snapshot examined during verification, captured for the
	// best-effort "applied" lifecycle event emitted below.
	var inspectedMS *persistencespb.WorkflowMutableState
	defer func() {
		// TODO: gate behind dynamic config before production
		e.emitReplicationVerifyApplied(inspectedMS, retErr)
	}()

	callerInfo := getReplicaitonCallerInfo(e.GetPriority())
	namespaceName, apply, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		callerInfo,
	), e.NamespaceID, e.WorkflowID)
	if nsError != nil {
		return nsError
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.TaskID()),
		)
		metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
			1,
			metrics.OperationTag(metrics.VerifyVersionedTransitionTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
	defer cancel()

	ms, err := e.getMutableState(ctx, e.RunID)
	inspectedMS = ms
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			return serviceerrors.NewSyncState(
				"missing mutable state, resend",
				e.NamespaceID,
				e.WorkflowID,
				e.RunID,
				e.taskAttr.GetArchetypeId(),
				nil,
				nil,
			)
		default:
			return err
		}
	}

	transitionHistory := ms.GetExecutionInfo().TransitionHistory
	if len(transitionHistory) == 0 {
		return serviceerrors.NewSyncState(
			"mutable state is not up to date",
			e.NamespaceID,
			e.WorkflowID,
			e.RunID,
			e.taskAttr.GetArchetypeId(),
			nil,
			ms.GetExecutionInfo().VersionHistories,
		)
	}
	err = transitionhistory.StalenessCheck(transitionHistory, e.ReplicationTask().VersionedTransition)

	// case 1: VersionedTransition is up-to-date on current mutable state
	if err == nil {
		if ms.GetNextEventId() < e.taskAttr.NextEventId {
			return softassert.UnexpectedDataLoss(e.Logger, "Workflow event missed",
				fmt.Errorf("NamespaceId: %v, workflowId: %v, runId: %v, expected last eventId: %v, versionedTransition: %v",
					e.NamespaceID, e.WorkflowID, e.RunID, e.taskAttr.NextEventId-1, e.ReplicationTask().VersionedTransition))
		}
		return e.verifyNewRunExist(ctx)
	}

	// case 2: verify task has newer VersionedTransition, need to sync state
	if transitionhistory.Compare(e.ReplicationTask().VersionedTransition, transitionhistory.LastVersionedTransition(transitionHistory)) > 0 {
		return serviceerrors.NewSyncState(
			"mutable state not up to date",
			e.NamespaceID,
			e.WorkflowID,
			e.RunID,
			e.taskAttr.GetArchetypeId(),
			transitionhistory.LastVersionedTransition(transitionHistory),
			ms.GetExecutionInfo().VersionHistories,
		)
	}
	// case 3: state transition is on non-current branch, but no event to verify
	if e.taskAttr.NextEventId == common2.EmptyEventID {
		return e.verifyNewRunExist(ctx)
	}

	if len(e.taskAttr.EventVersionHistory) == 0 {
		// no events to verify
		return nil
	}

	targetHistory := &historyspb.VersionHistory{
		Items: e.taskAttr.EventVersionHistory,
	}

	lcaItem, _, err := versionhistory.FindLCAVersionHistoryItemAndIndex(ms.GetExecutionInfo().VersionHistories, targetHistory)
	if err != nil {
		return err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(targetHistory)
	if err != nil {
		return err
	}
	// case 4: event on non-current branch are up-to-date
	if versionhistory.IsEqualVersionHistoryItem(lcaItem, lastItem) {
		return e.verifyNewRunExist(ctx)
	}
	// case 5: event on non-current branch are not up-to-date, we need to backfill events
	startEventVersion, err := versionhistory.GetVersionHistoryEventVersion(targetHistory, lcaItem.EventId+1)
	if err != nil {
		return err
	}
	return e.BackFillEvents(
		ctx,
		e.SourceClusterName(),
		e.WorkflowKey,
		lcaItem.EventId+1,
		startEventVersion,
		lastItem.EventId,
		lastItem.Version,
		e.taskAttr.NewRunId,
	)
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
	if ms != nil {
		info := ms.GetExecutionInfo()
		state := ms.GetExecutionState()
		payload.State = state.GetState().String()
		payload.Status = state.GetStatus().String()
		payload.TransitionHistoryLen = int32(len(info.GetTransitionHistory()))
		if last := transitionhistory.LastVersionedTransition(info.GetTransitionHistory()); last != nil {
			payload.AppliedFailoverVersion = last.GetNamespaceFailoverVersion()
			payload.AppliedTransitionCount = last.GetTransitionCount()
		}
		if currentHistory, vhErr := versionhistory.GetCurrentVersionHistory(info.GetVersionHistories()); vhErr == nil {
			if lastItem, itemErr := versionhistory.GetLastVersionHistoryItem(currentHistory); itemErr == nil {
				payload.LastEventID = lastItem.GetEventId()
				payload.LastEventVersion = lastItem.GetVersion()
			}
		}
		payload.StateTransitionCount = info.GetStateTransitionCount()
		payload.NewExecutionRunID = info.GetNewExecutionRunId()
		payload.SuccessorRunID = info.GetSuccessorRunId()
		payload.FirstExecutionRunID = info.GetFirstExecutionRunId()
		payload.OriginalExecutionRunID = info.GetOriginalExecutionRunId()
		payload.ResetRunID = info.GetResetRunId()
		payload.WorkflowWasReset = info.GetWorkflowWasReset()
		if ts := info.GetStartTime(); ts != nil && !ts.AsTime().IsZero() {
			payload.StartTime = ts.AsTime().Format(time.RFC3339)
		}
		if ts := info.GetExecutionTime(); ts != nil && !ts.AsTime().IsZero() {
			payload.ExecutionTime = ts.AsTime().Format(time.RFC3339)
		}
		if ts := info.GetCloseTime(); ts != nil && !ts.AsTime().IsZero() {
			payload.CloseTime = ts.AsTime().Format(time.RFC3339)
		}
		if luvt := state.GetLastUpdateVersionedTransition(); luvt != nil {
			payload.LastUpdateFailoverVersion = luvt.GetNamespaceFailoverVersion()
			payload.LastUpdateTransitionCount = luvt.GetTransitionCount()
		}
		payload.SignalCount = info.GetSignalCount()
		payload.ActivityCount = info.GetActivityCount()
		payload.ChildExecutionCount = info.GetChildExecutionCount()
		payload.UpdateCount = info.GetUpdateCount()
	}

	commonevents.EmitReplicationLifecycle(handler, payload)
}

func (e *ExecutableVerifyVersionedTransitionTask) verifyNewRunExist(ctx context.Context) error {
	if len(e.taskAttr.NewRunId) == 0 {
		return nil
	}
	_, err := e.getMutableState(ctx, e.taskAttr.NewRunId)
	switch err.(type) {
	case nil:
		return nil
	case *serviceerror.NotFound:
		return softassert.UnexpectedDataLoss(e.Logger, "workflow new run not found",
			fmt.Errorf("NamespaceId: %v, workflowId: %v, runId: %v, newRunId: %v",
				e.NamespaceID, e.WorkflowID, e.RunID, e.taskAttr.NewRunId))
	default:
		return err
	}
}

func (e *ExecutableVerifyVersionedTransitionTask) getMutableState(ctx context.Context, runId string) (_ *persistencespb.WorkflowMutableState, retError error) {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return nil, err
	}
	wfContext, release, err := e.WorkflowCache.GetOrCreateChasmExecution(
		ctx,
		shardContext,
		namespace.ID(e.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: e.WorkflowID,
			RunId:      runId,
		},
		e.taskAttr.GetArchetypeId(),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()
	ms, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}

	return ms.CloneToProto(), nil
}

func (e *ExecutableVerifyVersionedTransitionTask) HandleErr(err error) error {
	metrics.ReplicationTasksErrorByType.With(e.MetricsHandler).Record(
		1,
		metrics.OperationTag(metrics.VerifyVersionedTransitionTaskScope),
		metrics.NamespaceTag(e.NamespaceName()),
		metrics.ServiceErrorTypeTag(err),
	)
	if errors.Is(err, consts.ErrDuplicate) {
		e.MarkTaskDuplicated()
		return nil
	}
	e.Logger.Error("VerifyVersionedTransition replication task encountered error",
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.TaskID()),
		tag.Error(err),
	)
	switch taskErr := err.(type) {
	case *serviceerrors.SyncState:
		callerInfo := getReplicaitonCallerInfo(e.GetPriority())
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			callerInfo,
		), e.NamespaceID, e.WorkflowID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
		defer cancel()

		if doContinue, syncStateErr := e.SyncState(
			ctx,
			taskErr,
			ResendAttempt,
		); syncStateErr != nil || !doContinue {
			if syncStateErr != nil {
				e.Logger.Error("VerifyVersionedTransition replication task encountered error during sync state",
					tag.WorkflowNamespaceID(e.NamespaceID),
					tag.WorkflowID(e.WorkflowID),
					tag.WorkflowRunID(e.RunID),
					tag.TaskID(e.TaskID()),
					tag.Error(syncStateErr),
				)
				return err
			}
			return nil
		}
		return e.Execute()
	case *serviceerror.NotFound:
		e.Logger.Error(
			"workflow not found in source cluster, proceed to cleanup",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
		)
		// workflow is not found in source cluster, cleanup workflow in target cluster.
		// This handles workflow deletion from source cluster and this is optional as deletion operation will replicate to target clusters.
		deletionTask := NewExecutableDeleteExecutionTask(e.ProcessToolBox, e.TaskID(), e.TaskCreationTime(), e.SourceClusterName(), e.SourceShardKey(), e.ReplicationTask())
		return deletionTask.Execute()
	default:
		return err
	}
}
