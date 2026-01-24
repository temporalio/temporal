//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination activity_state_replicator_mock.go

package ndc

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	resendMissingEventMessage  = "Resend missed sync activity events"
	resendHigherVersionMessage = "Resend sync activity events due to a higher version received"
)

type (
	ActivityStateReplicator interface {
		SyncActivityState(
			ctx context.Context,
			request *historyservice.SyncActivityRequest,
		) error
		SyncActivitiesState(
			ctx context.Context,
			request *historyservice.SyncActivitiesRequest,
		) error
	}

	ActivityStateReplicatorImpl struct {
		shardContext  historyi.ShardContext
		workflowCache wcache.Cache
		logger        log.Logger
	}
)

func NewActivityStateReplicator(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
) *ActivityStateReplicatorImpl {

	return &ActivityStateReplicatorImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
		logger:        log.With(logger, tag.ComponentActivityStateReplicator),
	}
}

func (r *ActivityStateReplicatorImpl) SyncActivityState(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
) (retError error) {

	// sync activity info will only be sent from active side, when
	// 1. activity retry
	// 2. activity start
	// 3. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	namespaceID := namespace.ID(request.GetNamespaceId())
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: request.GetWorkflowId(),
		RunId:      request.GetRunId(),
	}.Build()

	executionContext, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		execution,
		locks.PriorityHigh,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := executionContext.LoadMutableState(ctx, r.shardContext)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// this can happen if the workflow start event and this sync activity task are out of order
			// or the target workflow is long gone
			// the safe solution to this is to throw away the sync activity task
			// or otherwise, worker attempt will exceed limit and put this message to DLQ
			return nil
		}
		return err
	}
	applied, err := r.syncSingleActivityState(
		&definition.WorkflowKey{
			NamespaceID: request.GetNamespaceId(),
			WorkflowID:  request.GetWorkflowId(),
			RunID:       request.GetRunId(),
		},
		mutableState,
		historyservice.ActivitySyncInfo_builder{
			Version:                    request.GetVersion(),
			ScheduledEventId:           request.GetScheduledEventId(),
			ScheduledTime:              request.GetScheduledTime(),
			StartedEventId:             request.GetStartedEventId(),
			StartVersion:               request.GetStartVersion(),
			StartedTime:                request.GetStartedTime(),
			LastHeartbeatTime:          request.GetLastHeartbeatTime(),
			Details:                    request.GetDetails(),
			Attempt:                    request.GetAttempt(),
			LastFailure:                request.GetLastFailure(),
			LastWorkerIdentity:         request.GetLastWorkerIdentity(),
			LastStartedBuildId:         request.GetLastStartedBuildId(),
			LastStartedRedirectCounter: request.GetLastStartedRedirectCounter(),
			VersionHistory:             request.GetVersionHistory(),
			FirstScheduledTime:         request.GetFirstScheduledTime(),
			LastAttemptCompleteTime:    request.GetLastAttemptCompleteTime(),
			Stamp:                      request.GetStamp(),
			Paused:                     request.GetPaused(),
			RetryInitialInterval:       request.GetRetryInitialInterval(),
			RetryMaximumInterval:       request.GetRetryMaximumInterval(),
			RetryMaximumAttempts:       request.GetRetryMaximumAttempts(),
			RetryBackoffCoefficient:    request.GetRetryBackoffCoefficient(),
		}.Build(),
	)
	if err != nil {
		return err
	}
	if !applied {
		return consts.ErrDuplicate
	}

	// passive logic need to explicitly call create timer
	if _, err := workflow.NewTimerSequence(
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	if r.shardContext.GetConfig().EnableUpdateWorkflowModeIgnoreCurrent() {
		return executionContext.UpdateWorkflowExecutionAsPassive(ctx, r.shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state, _ := mutableState.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return executionContext.UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		historyi.TransactionPolicyPassive,
		nil,
	)
}

func (r *ActivityStateReplicatorImpl) SyncActivitiesState(
	ctx context.Context,
	request *historyservice.SyncActivitiesRequest,
) (retError error) {
	// sync activity info will only be sent from active side, when
	// 1. activity retry
	// 2. activity start
	// 3. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	namespaceID := namespace.ID(request.GetNamespaceId())
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: request.GetWorkflowId(),
		RunId:      request.GetRunId(),
	}.Build()

	executionContext, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		execution,
		locks.PriorityHigh,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := executionContext.LoadMutableState(ctx, r.shardContext)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// this can happen if the workflow start event and this sync activity task are out of order
			// or the target workflow is long gone
			// the safe solution to this is to throw away the sync activity task
			// or otherwise, worker attempt will exceed limit and put this message to DLQ

			// TODO: this should return serviceerrors.NewRetryReplication to trigger a resend
			// resend logic will handle not found case and drop the task.
			return nil
		}
		return err
	}
	anyEventApplied := false
	for _, syncActivityInfo := range request.GetActivitiesInfo() {
		applied, err := r.syncSingleActivityState(
			&definition.WorkflowKey{
				NamespaceID: request.GetNamespaceId(),
				WorkflowID:  request.GetWorkflowId(),
				RunID:       request.GetRunId(),
			},
			mutableState,
			syncActivityInfo,
		)
		if err != nil {
			return err
		}
		anyEventApplied = anyEventApplied || applied
	}
	if !anyEventApplied {
		return consts.ErrDuplicate
	}

	// passive logic need to explicitly call create timer
	if _, err := workflow.NewTimerSequence(
		mutableState,
	).CreateNextActivityTimer(); err != nil {
		return err
	}

	if r.shardContext.GetConfig().EnableUpdateWorkflowModeIgnoreCurrent() {
		return executionContext.UpdateWorkflowExecutionAsPassive(ctx, r.shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state, _ := mutableState.GetWorkflowStateStatus(); state == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return executionContext.UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		historyi.TransactionPolicyPassive,
		nil,
	)
}

func (r *ActivityStateReplicatorImpl) syncSingleActivityState(
	workflowKey *definition.WorkflowKey,
	mutableState historyi.MutableState,
	activitySyncInfo *historyservice.ActivitySyncInfo,
) (applied bool, retError error) {
	scheduledEventID := activitySyncInfo.GetScheduledEventId()
	shouldApply, err := r.compareVersionHistory(
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		scheduledEventID,
		mutableState,
		activitySyncInfo.GetVersionHistory(),
	)
	if err != nil || !shouldApply {
		return false, err
	}

	activityInfo, ok := mutableState.GetActivityInfo(scheduledEventID)
	if !ok {
		// this should not retry, can be caused by out of order delivery
		// since the activity is already finished
		return false, nil
	}
	if shouldApply := r.compareActivity(
		activitySyncInfo.GetVersion(),
		activitySyncInfo.GetAttempt(),
		activitySyncInfo.GetStamp(),
		timestamp.TimeValue(activitySyncInfo.GetLastHeartbeatTime()),
		activityInfo,
	); !shouldApply {
		return false, nil
	}

	// sync activity with empty started ID means activity retry
	eventTime := timestamp.TimeValue(activitySyncInfo.GetScheduledTime())
	if activitySyncInfo.GetStartedEventId() == common.EmptyEventID && activitySyncInfo.GetAttempt() > activityInfo.GetAttempt() {
		mutableState.AddTasks(&tasks.ActivityRetryTimerTask{
			WorkflowKey:         *workflowKey,
			VisibilityTimestamp: eventTime,
			EventID:             activitySyncInfo.GetScheduledEventId(),
			Version:             activitySyncInfo.GetVersion(),
			Attempt:             activitySyncInfo.GetAttempt(),
			Stamp:               activitySyncInfo.GetStamp(),
		})
	}

	if err := mutableState.UpdateActivityInfo(
		activitySyncInfo,
		mutableState.ShouldResetActivityTimerTaskMask(
			activityInfo,
			persistencespb.ActivityInfo_builder{
				Version: activitySyncInfo.GetVersion(),
				Attempt: activitySyncInfo.GetAttempt(),
			}.Build(),
		),
	); err != nil {
		return false, err
	}

	return true, nil
}

func (r *ActivityStateReplicatorImpl) compareActivity(
	version int64,
	attempt int32,
	stamp int32,
	lastHeartbeatTime time.Time,
	activityInfo *persistencespb.ActivityInfo,
) bool {

	if activityInfo.GetVersion() > version {
		// this should not retry, can be caused by failover or reset
		return false
	}

	if activityInfo.GetVersion() < version {
		// incoming version larger then local version, should update activity
		return true
	}

	if activityInfo.GetStamp() < stamp {
		// stamp changed, should update activity
		return true
	}

	if activityInfo.GetStamp() > stamp {
		// stamp is older than we have, should not update activity
		return false
	}

	// activityInfo.Version == version
	if activityInfo.GetAttempt() > attempt {
		// this should not retry, can be caused by failover or reset
		return false
	}

	// activityInfo.Version == version
	if activityInfo.GetAttempt() < attempt {
		// version equal & attempt larger then existing, should update activity
		return true
	}

	// activityInfo.Version == version & activityInfo.Attempt == attempt

	// last heartbeat after existing heartbeat & should update activity
	if activityInfo.HasLastHeartbeatUpdateTime() &&
		!activityInfo.GetLastHeartbeatUpdateTime().AsTime().IsZero() &&
		activityInfo.GetLastHeartbeatUpdateTime().AsTime().After(lastHeartbeatTime) {
		// this should not retry, can be caused by out of order delivery
		return false
	}
	return true
}

func (r *ActivityStateReplicatorImpl) compareVersionHistory(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	scheduledEventID int64,
	mutableState historyi.MutableState,
	incomingVersionHistory *historyspb.VersionHistory,
) (bool, error) {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return false, err
	}

	lastLocalItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return false, err
	}

	lastIncomingItem, err := versionhistory.GetLastVersionHistoryItem(incomingVersionHistory)
	if err != nil {
		return false, err
	}

	lcaItem, err := versionhistory.FindLCAVersionHistoryItem(currentVersionHistory, incomingVersionHistory)
	if err != nil {
		return false, err
	}

	// case 1: local version history is superset of incoming version history
	//  or incoming version history is superset of local version history
	//  resend the missing event if local version history doesn't have the schedule event

	// case 2: local version history and incoming version history diverged
	//  case 2-1: local version history has the higher version and discard the incoming event
	//  case 2-2: incoming version history has the higher version and resend the missing incoming events
	if versionhistory.IsLCAVersionHistoryItemAppendable(currentVersionHistory, lcaItem) ||
		versionhistory.IsLCAVersionHistoryItemAppendable(incomingVersionHistory, lcaItem) {
		// case 1
		if scheduledEventID > lcaItem.GetEventId() {
			return false, serviceerrors.NewRetryReplication(
				resendMissingEventMessage,
				namespaceID.String(),
				workflowID,
				runID,
				lcaItem.GetEventId(),
				lcaItem.GetVersion(),
				common.EmptyEventID,
				common.EmptyVersion,
			)
		}
	} else {
		// case 2
		if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
			// case 2-1
			return false, nil
		} else if lastIncomingItem.GetVersion() > lastLocalItem.GetVersion() {
			// case 2-2
			return false, serviceerrors.NewRetryReplication(
				resendHigherVersionMessage,
				namespaceID.String(),
				workflowID,
				runID,
				lcaItem.GetEventId(),
				lcaItem.GetVersion(),
				common.EmptyEventID,
				common.EmptyVersion,
			)
		}
	}

	state, _ := mutableState.GetWorkflowStateStatus()
	return state != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, nil
}
