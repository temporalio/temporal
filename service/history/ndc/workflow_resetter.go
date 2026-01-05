//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_resetter_mock.go

package ndc

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api/updateworkflowoptions"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxChildrenInResetMutableState = 1000 // max number of children tracked in reset mutable state
)

var (
	errWorkflowResetterMaxChildren = serviceerror.NewInvalidArgumentf("WorkflowResetter encountered max allowed children [%d] while resetting.", maxChildrenInResetMutableState)
)

type (
	workflowResetReapplyEventsFn func(ctx context.Context, resetMutableState historyi.MutableState) error

	WorkflowResetter interface {
		ResetWorkflow(
			ctx context.Context,
			namespaceID namespace.ID,
			workflowID string,
			baseRunID string,
			baseBranchToken []byte,
			baseRebuildLastEventID int64,
			baseRebuildLastEventVersion int64,
			baseNextEventID int64,
			resetRunID string,
			resetRequestID string,
			baseWorkflow Workflow,
			currentWorkflow Workflow,
			resetReason string,
			additionalReapplyEvents []*historypb.HistoryEvent,
			resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
			allowResetWithPendingChildren bool,
			postResetOperations []*workflowpb.PostResetOperation,
		) error
	}

	workflowResetterImpl struct {
		shardContext      historyi.ShardContext
		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		workflowCache     wcache.Cache
		stateRebuilder    StateRebuilder
		transaction       workflow.Transaction
		logger            log.Logger
	}
)

var _ WorkflowResetter = (*workflowResetterImpl)(nil)

func NewWorkflowResetter(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
) *workflowResetterImpl {
	return &workflowResetterImpl{
		shardContext:      shardContext,
		namespaceRegistry: shardContext.GetNamespaceRegistry(),
		clusterMetadata:   shardContext.GetClusterMetadata(),
		executionMgr:      shardContext.GetExecutionManager(),
		workflowCache:     workflowCache,
		stateRebuilder:    NewStateRebuilder(shardContext, logger),
		transaction:       workflow.NewTransaction(shardContext),
		logger:            logger,
	}
}

// ResetWorkflow resets the given base run and creates a new run that would start after baseNextEventID. It additionally does the following
// - cherry-picks the relevant events from base run and reapplies them to the new run.
// - Terminates the current run if one is running. Also cherry picks and reapplies any relevant events generated due to termination of current run.
// - Performs other bookkeeping like linking base->new run, marking current as terminated due to reset etc.
func (r *workflowResetterImpl) ResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	baseNextEventID int64,
	resetRunID string,
	resetRequestID string,
	baseWorkflow Workflow,
	currentWorkflow Workflow,
	resetReason string,
	additionalReapplyEvents []*historypb.HistoryEvent,
	resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
	allowResetWithPendingChildren bool,
	postResetOperations []*workflowpb.PostResetOperation,
) (retError error) {

	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	// update base workflow to point to new runID after the reset.
	baseWorkflow.GetMutableState().UpdateResetRunID(resetRunID)

	resetWorkflowVersion := namespaceEntry.FailoverVersion()

	var currentWorkflowMutation *persistence.WorkflowMutation
	var currentWorkflowEventsSeq []*persistence.WorkflowEvents
	var reapplyEventsFn workflowResetReapplyEventsFn
	currentMutableState := currentWorkflow.GetMutableState()
	if currentMutableState.IsWorkflowExecutionRunning() {
		currentMutableState.GetExecutionInfo().WorkflowWasReset = true
		if err := r.terminateWorkflow(
			currentMutableState,
			resetReason,
		); err != nil {
			return err
		}
		resetWorkflowVersion = currentMutableState.GetCurrentVersion()

		currentWorkflowMutation, currentWorkflowEventsSeq, err = currentMutableState.CloseTransactionAsMutation(
			ctx,
			historyi.TransactionPolicyActive,
		)
		if err != nil {
			return err
		}

		reapplyEventsFn = func(ctx context.Context, resetMutableState historyi.MutableState) error {
			lastVisitedRunID, err := r.reapplyContinueAsNewWorkflowEvents(
				ctx,
				resetMutableState,
				currentWorkflow,
				namespaceID,
				workflowID,
				baseRunID,
				baseBranchToken,
				baseRebuildLastEventID+1,
				baseNextEventID,
				resetReapplyExcludeTypes,
				allowResetWithPendingChildren,
			)
			if err != nil {
				return err
			}

			if lastVisitedRunID == currentMutableState.GetExecutionState().RunId {
				for _, event := range currentWorkflowEventsSeq {
					if _, err := r.reapplyEvents(ctx, resetMutableState, event.Events, resetReapplyExcludeTypes); err != nil {
						return err
					}
				}
			}
			return nil
		}
	} else {
		reapplyEventsFn = func(ctx context.Context, resetMutableState historyi.MutableState) error {
			_, err := r.reapplyContinueAsNewWorkflowEvents(
				ctx,
				resetMutableState,
				currentWorkflow,
				namespaceID,
				workflowID,
				baseRunID,
				baseBranchToken,
				baseRebuildLastEventID+1,
				baseNextEventID,
				resetReapplyExcludeTypes,
				allowResetWithPendingChildren,
			)
			return err
		}
	}

	resetWorkflow, err := r.prepareResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetRequestID,
		resetWorkflowVersion,
		resetReason,
		allowResetWithPendingChildren,
	)
	if err != nil {
		return err
	}
	defer func() { resetWorkflow.GetReleaseFn()(retError) }()

	resetMS := resetWorkflow.GetMutableState()
	if err := reapplyEventsFn(ctx, resetMS); err != nil {
		return err
	}
	if _, err := r.reapplyEvents(ctx, resetMS, additionalReapplyEvents, nil); err != nil {
		return err
	}

	if err := r.performPostResetOperations(ctx, resetMS, postResetOperations); err != nil {
		return err
	}

	if err := workflow.ScheduleWorkflowTask(resetMS); err != nil {
		return err
	}

	if err = r.persistToDB(
		ctx,
		baseWorkflow,
		currentWorkflow,
		currentWorkflowMutation,
		currentWorkflowEventsSeq,
		resetWorkflow,
	); err != nil {
		return err
	}

	currentWorkflow.GetContext().UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowCompleted)

	return nil
}

func (r *workflowResetterImpl) prepareResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetRequestID string,
	resetWorkflowVersion int64,
	resetReason string,
	allowResetWithPendingChildren bool,
) (Workflow, error) {

	resetWorkflow, err := r.replayResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetRequestID,
	)
	if err != nil {
		return nil, err
	}

	// Reset expiration time
	resetMutableState := resetWorkflow.GetMutableState()

	// if workflow was reset after it was expired - at this point expiration task will
	// already be fired since it is (re)created from the event, and event has old expiration time
	// generate workflow execution task. again. this time with proper expiration time

	if err := resetMutableState.RefreshExpirationTimeoutTask(ctx); err != nil {
		return nil, err
	}

	if resetMutableState.GetCurrentVersion() > resetWorkflowVersion {
		return nil, serviceerror.NewInternal("WorkflowResetter encountered version mismatch.")
	}
	if err := resetMutableState.UpdateCurrentVersion(
		resetWorkflowVersion,
		false,
	); err != nil {
		return nil, err
	}

	if !allowResetWithPendingChildren && len(resetMutableState.GetPendingChildExecutionInfos()) > 0 {
		return nil, serviceerror.NewInvalidArgument("WorkflowResetter encountered pending child workflows.")
	}

	if err := r.failWorkflowTask(
		resetMutableState,
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetReason,
	); err != nil {
		return nil, err
	}

	if err := r.failInflightActivity(
		resetMutableState.GetExecutionState().StartTime.AsTime(),
		resetMutableState,
		resetReason,
	); err != nil {
		return nil, err
	}

	return resetWorkflow, nil
}

func (r *workflowResetterImpl) persistToDB(
	ctx context.Context,
	baseWorkflow Workflow,
	currentWorkflow Workflow,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	resetWorkflow Workflow,
) error {
	currentRunID := currentWorkflow.GetMutableState().GetExecutionState().GetRunId()
	baseRunID := baseWorkflow.GetMutableState().GetExecutionState().GetRunId()
	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := resetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return err
	}

	if currentRunID == baseRunID {
		// There are just 2 runs to update - base & new run.
		// Additionally since base is the same as current, we should ensure that we prepare only one of them for transaction and do it only once (to avoid DBRecordVersion conflict)
		// So check if current was already prepared for transaction. If not prepare the mutation for transaction.
		if currentWorkflowMutation == nil {
			currentWorkflowMutation, currentWorkflowEventsSeq, err = currentWorkflow.GetMutableState().CloseTransactionAsMutation(
				ctx,
				historyi.TransactionPolicyActive,
			)
			if err != nil {
				return err
			}

		}

		if _, _, err := r.transaction.UpdateWorkflowExecution(
			ctx,
			persistence.UpdateWorkflowModeUpdateCurrent,
			chasm.WorkflowArchetypeID,
			currentWorkflow.GetMutableState().GetCurrentVersion(),
			currentWorkflowMutation,
			currentWorkflowEventsSeq,
			workflow.MutableStateFailoverVersion(resetWorkflow.GetMutableState()),
			resetWorkflowSnapshot,
			resetWorkflowEventsSeq,
			currentWorkflow.GetMutableState().IsWorkflow(),
		); err != nil {
			return err
		}
		return nil

	}

	if currentWorkflowMutation == nil {
		// We have 2 different runs to update here - the base run & the new run. There were no changes to current.
		// However we are still preparing current for transaction only to be able to use transaction.ConflictResolveWorkflowExecution() method below.
		currentWorkflowMutation, currentWorkflowEventsSeq, err = currentWorkflow.GetMutableState().CloseTransactionAsMutation(
			ctx,
			historyi.TransactionPolicyActive,
		)
		if err != nil {
			return err
		}
	}

	// We have 3 different runs to update here. However we have to prepare the snapshot of the base for transaction to be used in transaction.ConflictResolveWorkflowExecution() method.
	// We use this method since it allows us to commit changes from all 3 different runs in the same DB transaction.
	baseSnapshot, baseEventsSeq, err := baseWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return err
	}
	resetRunVersion := resetWorkflow.GetMutableState().GetCurrentVersion()
	currentRunVerson := currentWorkflow.GetMutableState().GetCurrentVersion()
	if _, _, _, err := r.transaction.ConflictResolveWorkflowExecution(
		ctx,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		chasm.WorkflowArchetypeID,
		baseWorkflow.GetMutableState().GetCurrentVersion(),
		baseSnapshot,
		baseEventsSeq,
		&resetRunVersion,
		resetWorkflowSnapshot,
		resetWorkflowEventsSeq,
		&currentRunVerson,
		currentWorkflowMutation,
		currentWorkflowEventsSeq,
		currentWorkflow.GetMutableState().IsWorkflow(),
	); err != nil {
		return err
	}
	return nil
}

func (r *workflowResetterImpl) replayResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetRequestID string,
) (Workflow, error) {

	resetBranchToken, err := r.forkAndGenerateBranchToken(
		ctx,
		namespaceID,
		workflowID,
		baseBranchToken,
		baseRebuildLastEventID+1,
		resetRunID,
	)
	if err != nil {
		return nil, err
	}

	resetContext := workflow.NewContext(
		r.shardContext.GetConfig(),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			resetRunID,
		),
		chasm.WorkflowArchetypeID,
		r.logger,
		r.shardContext.GetLogger(),
		r.shardContext.GetMetricsHandler(),
	)

	resetMutableState, resetStats, err := r.stateRebuilder.Rebuild(
		ctx,
		r.shardContext.GetTimeSource().Now(),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		util.Ptr(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	)
	if err != nil {
		return nil, err
	}

	resetMutableState.SetBaseWorkflow(
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
	)
	resetMutableState.AddHistorySize(resetStats.HistorySize)
	resetMutableState.AddExternalPayloadSize(resetStats.ExternalPayloadSize)
	resetMutableState.AddExternalPayloadCount(resetStats.ExternalPayloadCount)
	return NewWorkflow(
		r.clusterMetadata,
		resetContext,
		resetMutableState,
		wcache.NoopReleaseFn,
	), nil
}

func (r *workflowResetterImpl) failWorkflowTask(
	resetMutableState historyi.MutableState,
	baseRunID string,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetReason string,
) error {

	workflowTask := resetMutableState.GetPendingWorkflowTask()
	if workflowTask == nil {
		// TODO if resetMutableState.HadOrHasWorkflowTask() == true
		//  meaning workflow history has NO workflow task ever
		//  should also allow workflow reset, the only remaining issues are
		//  * what if workflow is a cron workflow, e.g. should add a workflow task directly or still respect the cron job
		return serviceerror.NewInvalidArgumentf(
			"Can only reset workflow to event ID in range [WorkflowTaskScheduled +1, WorkflowTaskStarted + 1]: %v",
			baseRebuildLastEventID+1,
		)
	}

	var err error
	if workflowTask.StartedEventID == common.EmptyVersion {
		_, workflowTask, err = resetMutableState.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			workflowTask.TaskQueue,
			consts.IdentityHistoryService,
			nil,
			nil,
			nil,
			// skipping versioning checks because this task is not actually dispatched but will fail immediately.
			true,
			nil,
		)
		if err != nil {
			return err
		}
	}

	_, err = resetMutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
		nil,
		"",
		baseRunID,
		resetRunID,
		baseRebuildLastEventVersion,
	)
	return err
}

func (r *workflowResetterImpl) failInflightActivity(
	now time.Time,
	mutableState historyi.MutableState,
	terminateReason string,
) error {

	for _, ai := range mutableState.GetPendingActivityInfos() {
		switch ai.StartedEventId {
		case common.EmptyEventID:
			// activity not started, noop
			if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
				// override the scheduled activity time to now
				activityInfo.ScheduledTime = timestamppb.New(now)
				activityInfo.FirstScheduledTime = timestamppb.New(now)
				return nil
			}); err != nil {
				return err
			}

		case common.TransientEventID:
			// activity is started (with retry policy)
			// should not encounter this case when rebuilding mutable state
			return serviceerror.NewInternal("WorkflowResetter encountered transient activity.")

		default:
			if _, err := mutableState.AddActivityTaskFailedEvent(
				ai.ScheduledEventId,
				ai.StartedEventId,
				failure.NewResetWorkflowFailure(terminateReason, ai.LastHeartbeatDetails),
				enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				ai.StartedIdentity,
				nil,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *workflowResetterImpl) forkAndGenerateBranchToken(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	forkBranchToken []byte,
	forkNodeID int64,
	resetRunID string,
) ([]byte, error) {
	// fork a new history branch
	shardID := r.shardContext.GetShardID()
	resp, err := r.executionMgr.ForkHistoryBranch(ctx, &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: forkBranchToken,
		ForkNodeID:      forkNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID.String(), workflowID, resetRunID),
		ShardID:         shardID,
		NamespaceID:     namespaceID.String(),
		NewRunID:        resetRunID,
	})
	if err != nil {
		return nil, err
	}

	return resp.NewBranchToken, nil
}

func (r *workflowResetterImpl) terminateWorkflow(
	mutableState historyi.MutableState,
	terminateReason string,
) error {

	return workflow.TerminateWorkflow(
		mutableState,
		terminateReason,
		nil,
		consts.IdentityResetter,
		false,
		nil, // No links necessary.
	)
}

func (r *workflowResetterImpl) reapplyContinueAsNewWorkflowEvents(
	ctx context.Context,
	resetMutableState historyi.MutableState,
	currentWorkflow Workflow,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildNextEventID int64,
	baseNextEventID int64,
	resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
	allowResetWithPendingChildren bool,
) (string, error) {

	// TODO change this logic to fetching all workflow [baseWorkflow, currentWorkflow]
	//  from visibility for better coverage of events eligible for re-application.

	lastVisitedRunID := baseRunID
	if r.shouldExcludeAllReapplyEvents(resetReapplyExcludeTypes) {
		// All subsequent events should be excluded from being re-applied. So, do nothing and return.
		return lastVisitedRunID, nil
	}

	// track the child workflows initiated after reset.
	// This will be saved in the parent workflow (in execution info) and used by the parent later to determine how to start these child workflows again.
	childrenInitializedAfterReset := make(map[string]*persistencespb.ResetChildInfo)

	// First, special handling of remaining events for base workflow
	nextRunID, err := r.reapplyEventsFromBranch(
		ctx,
		resetMutableState,
		baseRebuildNextEventID,
		baseNextEventID,
		baseBranchToken,
		resetReapplyExcludeTypes,
		allowResetWithPendingChildren,
		childrenInitializedAfterReset,
	)
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		r.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(workflowID), tag.WorkflowRunID(baseRunID))
		return "", err
	default:
		return "", err
	}

	getNextEventIDBranchToken := func(runID string) (nextEventID int64, branchToken []byte, retError error) {
		var wfCtx historyi.WorkflowContext
		var err error

		if runID == currentWorkflow.GetMutableState().GetWorkflowKey().RunID {
			wfCtx = currentWorkflow.GetContext()
		} else {
			var release historyi.ReleaseWorkflowContextFunc
			wfCtx, release, err = r.workflowCache.GetOrCreateWorkflowExecution(
				ctx,
				r.shardContext,
				namespaceID,
				&commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				locks.PriorityHigh,
			)
			if err != nil {
				return 0, nil, err
			}
			defer func() { release(retError) }()
		}

		mutableState, err := wfCtx.LoadMutableState(ctx, r.shardContext)
		if err != nil {
			// no matter what error happen, we need to retry
			return 0, nil, err
		}

		nextEventID = mutableState.GetNextEventID()
		branchToken, err = mutableState.GetCurrentBranchToken()
		if err != nil {
			return 0, nil, err
		}
		return nextEventID, branchToken, nil
	}

	// Second, for remaining continue as new workflow, reapply eligible events
	for len(nextRunID) != 0 {
		lastVisitedRunID = nextRunID
		nextWorkflowNextEventID, nextWorkflowBranchToken, err := getNextEventIDBranchToken(nextRunID)
		if err != nil {
			return "", err
		}

		nextRunID, err = r.reapplyEventsFromBranch(
			ctx,
			resetMutableState,
			common.FirstEventID,
			nextWorkflowNextEventID,
			nextWorkflowBranchToken,
			resetReapplyExcludeTypes,
			allowResetWithPendingChildren,
			childrenInitializedAfterReset,
		)
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.DataLoss:
			// log event
			r.logger.Error("encounter data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(workflowID), tag.WorkflowRunID(nextRunID))
			return "", err
		default:
			return "", err
		}
	}
	if len(childrenInitializedAfterReset) > 0 {
		resetMutableState.SetChildrenInitializedPostResetPoint(childrenInitializedAfterReset)
	}
	return lastVisitedRunID, nil
}

func (r *workflowResetterImpl) reapplyEventsFromBranch(
	ctx context.Context,
	mutableState historyi.MutableState,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
	resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
	allowResetWithPendingChildren bool,
	childrenInitializedAfterReset map[string]*persistencespb.ResetChildInfo,
) (string, error) {

	// TODO change this logic to fetching all workflow [baseWorkflow, currentWorkflow]
	//  from visibility for better coverage of events eligible for re-application.
	//  after the above change, this API do not have to return the continue as new run ID

	iter := collection.NewPagingIterator(r.getPaginationFn(
		ctx,
		firstEventID,
		nextEventID,
		branchToken,
	))

	var nextRunID string
	var lastEvents []*historypb.HistoryEvent

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return "", err
		}
		lastEvents = batch.Events
		if _, err := r.reapplyEvents(ctx, mutableState, lastEvents, resetReapplyExcludeTypes); err != nil {
			return "", err
		}
		// TODO: uncomment this code to enable phase 2 of reset workflow feature.
		// track the child workflows initiated after reset-point
		// if allowResetWithPendingChildren {
		// 	for _, event := range lastEvents {
		// 		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
		// 			attr := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		// 			// TODO: there is a possibility the childIDs constructed this way may not be unique. But the probability of that is very low.
		// 			// Need to figure out a better way to track these child workflows.
		// 			childID := fmt.Sprintf("%s:%s", attr.GetWorkflowType().Name, attr.GetWorkflowId())
		// 			childrenInitializedAfterReset[childID] = &persistencespb.ResetChildInfo{
		// 				ShouldTerminateAndStart: true,
		// 			}
		// 			if len(childrenInitializedAfterReset) > maxChildrenInResetMutableState {
		// 				return "", errWorkflowResetterMaxChildren
		// 			}
		// 		}
		// 	}
		// }
	}

	if len(lastEvents) > 0 {
		lastEvent := lastEvents[len(lastEvents)-1]
		if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
			nextRunID = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
		}
	}
	return nextRunID, nil
}

func (r *workflowResetterImpl) reapplyEvents(
	ctx context.Context,
	mutableState historyi.MutableState,
	events []*historypb.HistoryEvent,
	resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
) ([]*historypb.HistoryEvent, error) {
	// When reapplying events during WorkflowReset, we do not check for conflicting update IDs (they are not possible,
	// since the workflow was in a consistent state before reset), and we do not perform deduplication (because we never
	// did, before the refactoring that unified two code paths; see comment below.)
	return reapplyEvents(ctx, mutableState, nil, r.shardContext.StateMachineRegistry(), events, resetReapplyExcludeTypes, "", true)
}

func reapplyEvents(
	ctx context.Context,
	mutableState historyi.MutableState,
	targetBranchUpdateRegistry update.Registry,
	stateMachineRegistry *hsm.Registry,
	events []*historypb.HistoryEvent,
	resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{},
	runIdForDeduplication string,
	isReset bool,
) ([]*historypb.HistoryEvent, error) {
	// TODO (dan): This implementation is the result of unifying two previous implementations, one of which did
	// deduplication. Can we always/never do this deduplication, or must it be decided by the caller?
	isDuplicate := func(event *historypb.HistoryEvent) bool {
		if runIdForDeduplication == "" {
			return false
		}
		resource := definition.NewEventReappliedID(runIdForDeduplication, event.GetEventId(), event.GetVersion())
		return mutableState.IsResourceDuplicated(resource)
	}
	_, excludeSignal := resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL]
	_, excludeUpdate := resetReapplyExcludeTypes[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE]
	var reappliedEvents []*historypb.HistoryEvent
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			if excludeSignal || isDuplicate(event) {
				continue
			}
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			if _, err := mutableState.AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
				attr.GetHeader(),
				event.Links,
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
			if excludeUpdate || isDuplicate(event) {
				continue
			}
			attr := event.GetWorkflowExecutionUpdateAdmittedEventAttributes()
			// targetBranchUpdateRegistry is a nil in a Reset case, and not nil in a conflict resolution case.
			// If the Update with the same UpdateId is already present in the target branch (Find returns non-nil),
			// it is skipped and not reapplied.
			if targetBranchUpdateRegistry != nil && targetBranchUpdateRegistry.Find(ctx, attr.Request.Meta.UpdateId) != nil {
				continue
			}
			if _, err := mutableState.AddWorkflowExecutionUpdateAdmittedEvent(
				attr.GetRequest(),
				attr.Origin,
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			if excludeUpdate || isDuplicate(event) {
				continue
			}
			attr := event.GetWorkflowExecutionUpdateAcceptedEventAttributes()
			// targetBranchUpdateRegistry is a nil in a Reset case, and not nil in a conflict resolution case.
			// If the Update with the same UpdateId is already present in the target branch (Find returns non-nil),
			// it is skipped and not reapplied.
			if targetBranchUpdateRegistry != nil && targetBranchUpdateRegistry.Find(ctx, attr.ProtocolInstanceId) != nil {
				continue
			}
			request := attr.GetAcceptedRequest()
			if request == nil {
				// An UpdateAccepted event lacks a request payload if and only if it is preceded by an UpdateAdmitted
				// event (these always have the payload). If an UpdateAccepted event has no preceding UpdateAdmitted
				// event then we reapply it (converting it to UpdateAdmitted on the new branch). But if there is a
				// preceding UpdateAdmitted event then we do not reapply the UpdateAccepted event.
				continue
			}
			if _, err := mutableState.AddWorkflowExecutionUpdateAdmittedEvent(
				request,
				enumspb.UPDATE_ADMITTED_EVENT_ORIGIN_REAPPLY,
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
			if isReset || isDuplicate(event) {
				continue
			}
			if mutableState.IsCancelRequested() {
				// This is a no-op because the cancel request is already recorded.
				continue
			}
			attr := event.GetWorkflowExecutionCancelRequestedEventAttributes()
			request := &historyservice.RequestCancelWorkflowExecutionRequest{
				CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
					Reason:   attr.GetCause(),
					Identity: attr.GetIdentity(),
					Links:    event.Links,
				},
				ExternalInitiatedEventId:  attr.GetExternalInitiatedEventId(),
				ExternalWorkflowExecution: attr.GetExternalWorkflowExecution(),
			}
			if _, err := mutableState.AddWorkflowExecutionCancelRequestedEvent(
				request,
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
			if isDuplicate(event) {
				continue
			}
			attr := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()
			callbacks := attr.GetAttachedCompletionCallbacks()
			requestID := attr.GetAttachedRequestId()
			if len(callbacks) > 0 && requestID != "" {
				if mutableState.HasRequestID(requestID) {
					if attr.GetVersioningOverride() == nil && !attr.GetUnsetVersioningOverride() {
						// if completion callbacks exist, and no other updates in the event, we should skip the event
						continue
					}
					return reappliedEvents, serviceerror.NewInternalf("unable to reapply WorkflowExecutionOptionsUpdated event: %d completion callbacks are already attached but the event contains additional workflow option updates", len(callbacks))
				}
			}
			if _, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(
				attr.GetVersioningOverride(),
				attr.GetUnsetVersioningOverride(),
				requestID,
				callbacks,
				event.Links,
				attr.GetIdentity(),
				attr.GetPriority(),
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
			if isReset || isDuplicate(event) {
				continue
			}
			attr := event.GetWorkflowExecutionTerminatedEventAttributes()
			if attr.GetIdentity() == consts.IdentityHistoryService || attr.GetIdentity() == consts.IdentityResetter {
				continue
			}
			if err := workflow.TerminateWorkflow(
				mutableState,
				attr.GetReason(),
				attr.GetDetails(),
				attr.GetIdentity(),
				false,
				event.Links,
			); err != nil {
				return reappliedEvents, err
			}
			reappliedEvents = append(reappliedEvents, event)
		// Handle all non-init events for the child.
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			if isDuplicate(event) {
				continue
			}
			err := reapplyChildEvents(mutableState, event)
			if err != nil {
				return nil, err
			}
		default:
			root := mutableState.HSM()
			def, ok := stateMachineRegistry.EventDefinition(event.GetEventType())
			if !ok {
				// Only reapply hardcoded events above or ones registered and are cherry-pickable in the HSM framework.
				continue
			}
			if err := def.CherryPick(root, event, resetReapplyExcludeTypes); err != nil {
				if errors.Is(err, hsm.ErrNotCherryPickable) || errors.Is(err, hsm.ErrStateMachineNotFound) || errors.Is(err, hsm.ErrInvalidTransition) {
					continue
				}
				return reappliedEvents, err
			}
			mutableState.AddHistoryEvent(event.EventType, func(he *historypb.HistoryEvent) {
				he.Attributes = event.Attributes
			})
			reappliedEvents = append(reappliedEvents, event)
		}
		if runIdForDeduplication != "" {
			deDupResource := definition.NewEventReappliedID(runIdForDeduplication, event.GetEventId(), event.GetVersion())
			mutableState.UpdateDuplicatedResource(deDupResource)
		}
	}
	return reappliedEvents, nil
}

// reapplyChildEvents reapplies all child events except EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED.
// This function is intended to pick up all the events for a child that was already initialized before the reset point.
// Re-applying these events is needed to support reconnecting of the child with parent.
func reapplyChildEvents(mutableState historyi.MutableState, event *historypb.HistoryEvent) error { // nolint:revive
	switch event.GetEventType() { // nolint:exhaustive
	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		childEventAttributes := event.GetStartChildWorkflowExecutionFailedEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		attributes := &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			WorkflowId:   childEventAttributes.WorkflowId,
			WorkflowType: childEventAttributes.WorkflowType,
			Control:      childEventAttributes.Control,
		}
		if _, err := mutableState.AddStartChildWorkflowExecutionFailedEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.Cause, attributes); err != nil {
			return err
		}

	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
		childEventAttributes := event.GetChildWorkflowExecutionStartedEventAttributes()
		ci, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists || ci.StartedEventId != common.EmptyEventID {
			return nil
		}
		childClock := ci.Clock
		if _, err := mutableState.AddChildWorkflowExecutionStartedEvent(
			childEventAttributes.WorkflowExecution,
			childEventAttributes.WorkflowType,
			childEventAttributes.GetInitiatedEventId(),
			childEventAttributes.Header,
			childClock); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
		childEventAttributes := event.GetChildWorkflowExecutionCompletedEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		attributes := &historypb.WorkflowExecutionCompletedEventAttributes{
			Result: childEventAttributes.Result,
		}
		if _, err := mutableState.AddChildWorkflowExecutionCompletedEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.WorkflowExecution, attributes); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
		childEventAttributes := event.GetChildWorkflowExecutionFailedEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		attributes := &historypb.WorkflowExecutionFailedEventAttributes{
			Failure:    childEventAttributes.Failure,
			RetryState: childEventAttributes.RetryState,
		}
		if _, err := mutableState.AddChildWorkflowExecutionFailedEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.WorkflowExecution, attributes); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
		childEventAttributes := event.GetChildWorkflowExecutionCanceledEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		attributes := &historypb.WorkflowExecutionCanceledEventAttributes{
			Details: childEventAttributes.Details,
		}
		if _, err := mutableState.AddChildWorkflowExecutionCanceledEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.WorkflowExecution, attributes); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
		childEventAttributes := event.GetChildWorkflowExecutionTimedOutEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		attributes := &historypb.WorkflowExecutionTimedOutEventAttributes{
			RetryState: childEventAttributes.RetryState,
		}
		if _, err := mutableState.AddChildWorkflowExecutionTimedOutEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.WorkflowExecution, attributes); err != nil {
			return err
		}
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		childEventAttributes := event.GetChildWorkflowExecutionTerminatedEventAttributes()
		_, childExists := mutableState.GetChildExecutionInfo(childEventAttributes.GetInitiatedEventId())
		if !childExists {
			return nil
		}
		if _, err := mutableState.AddChildWorkflowExecutionTerminatedEvent(childEventAttributes.GetInitiatedEventId(), childEventAttributes.WorkflowExecution); err != nil {
			return err
		}
	default:
		return serviceerror.NewInternalf("WorkflowResetter encountered an unexpected child event: [%s]", event.GetEventType().String())
	}
	return nil
}

func (r *workflowResetterImpl) getPaginationFn(
	ctx context.Context,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn[*historypb.History] {

	return func(paginationToken []byte) ([]*historypb.History, []byte, error) {

		resp, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      defaultPageSize,
			NextPageToken: paginationToken,
			ShardID:       r.shardContext.GetShardID(),
		})
		if err != nil {
			return nil, nil, err
		}
		return resp.History, resp.NextPageToken, nil
	}
}

func IsTerminatedByResetter(event *historypb.HistoryEvent) bool {
	if attributes := event.GetWorkflowExecutionTerminatedEventAttributes(); attributes != nil && attributes.Identity == consts.IdentityResetter {
		return true
	}
	return false
}

// shouldExcludeAllReapplyEvents returns true if the excludeTypes map contains all the elegible re-apply event types.
func (r *workflowResetterImpl) shouldExcludeAllReapplyEvents(excludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) bool {
	for key := range enumspb.ResetReapplyExcludeType_name {
		eventType := enumspb.ResetReapplyExcludeType(key)
		if eventType == enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UNSPECIFIED {
			continue
		}
		if _, ok := excludeTypes[eventType]; !ok {
			return false
		}
	}
	return true
}

// performPostResetOperations performs the optional post reset operations on the reset workflow.
func (r *workflowResetterImpl) performPostResetOperations(ctx context.Context, resetMS historyi.MutableState, postResetOperations []*workflowpb.PostResetOperation) error {
	for _, operation := range postResetOperations {
		switch op := operation.GetVariant().(type) {
		case *workflowpb.PostResetOperation_UpdateWorkflowOptions_:
			// TODO(carlydf): Put the reset requester in the event so that with state-based replication this code will run on the passive side.
			_, _, err := updateworkflowoptions.MergeAndApply(resetMS, op.UpdateWorkflowOptions.GetWorkflowExecutionOptions(), op.UpdateWorkflowOptions.GetUpdateMask(), "")
			if err != nil {
				return err
			}
		}
	}
	return nil
}
