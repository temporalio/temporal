package startworkflow

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	BeforeCreateHookFunc func(lease api.WorkflowLease) error
	StartOutcome         int
)

const (
	eagerStartDeniedReasonDynamicConfigDisabled    metrics.ReasonString = "dynamic_config_disabled"
	eagerStartDeniedReasonFirstWorkflowTaskBackoff metrics.ReasonString = "first_workflow_task_backoff"
	eagerStartDeniedReasonTaskAlreadyDispatched    metrics.ReasonString = "task_already_dispatched"
)

const (
	StartErr StartOutcome = iota
	StartNew
	StartReused
	StartDeduped
)

// Starter starts a new workflow execution.
type Starter struct {
	metricsHandler             metrics.Handler
	shardContext               historyi.ShardContext
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	matchingClient             matchingservice.MatchingServiceClient
	tokenSerializer            *tasktoken.Serializer
	request                    *historyservice.StartWorkflowExecutionRequest
	namespace                  *namespace.Namespace
	createOrUpdateLeaseFn      api.CreateOrUpdateLeaseFunc
	enableRequestIdRefLinks    dynamicconfig.BoolPropertyFn
	versionMembershipCache     worker_versioning.VersionMembershipCache
}

// creationParams is a container for all information obtained from creating the uncommitted execution.
// The information is later used to create a new execution and handle conflicts.
type creationParams struct {
	workflowID           string
	runID                string
	workflowLease        api.WorkflowLease
	workflowTaskInfo     *historyi.WorkflowTaskInfo
	workflowSnapshot     *persistence.WorkflowSnapshot
	workflowEventBatches []*persistence.WorkflowEvents
}

// mutableStateInfo is a container for the relevant mutable state information to generate a start response with an eager
// workflow task.
type mutableStateInfo struct {
	branchToken  []byte
	lastEventID  int64
	workflowTask *historyi.WorkflowTaskInfo
}

// NewStarter creates a new starter, fails if getting the active namespace fails.
func NewStarter(
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer *tasktoken.Serializer,
	request *historyservice.StartWorkflowExecutionRequest,
	matchingClient matchingservice.MatchingServiceClient,
	versionMembershipCache worker_versioning.VersionMembershipCache,
	createLeaseFn api.CreateOrUpdateLeaseFunc,
) (*Starter, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()), request.StartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}

	return &Starter{
		// metricsHandler is lazily created when needed in Starter.getMetricsHandler
		metricsHandler:             nil,
		shardContext:               shardContext,
		workflowConsistencyChecker: workflowConsistencyChecker,
		matchingClient:             matchingClient,
		tokenSerializer:            tokenSerializer,
		request:                    request,
		namespace:                  namespaceEntry,
		createOrUpdateLeaseFn:      createLeaseFn,
		enableRequestIdRefLinks:    shardContext.GetConfig().EnableRequestIdRefLinks,
		versionMembershipCache:     versionMembershipCache,
	}, nil
}

// prepare applies request overrides, validates the request, and records eager execution metrics.
func (s *Starter) prepare(ctx context.Context) error {
	request := s.request.StartRequest

	// TODO: remove this call in 1.25
	enums.SetDefaultWorkflowIdConflictPolicy(
		&request.WorkflowIdConflictPolicy,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL)

	api.MigrateWorkflowIdReusePolicyForRunningWorkflow(
		&request.WorkflowIdReusePolicy,
		&request.WorkflowIdConflictPolicy)

	api.OverrideStartWorkflowExecutionRequest(
		request,
		metrics.HistoryStartWorkflowExecutionScope,
		s.shardContext,
		s.shardContext.GetMetricsHandler(),
	)

	err := api.ValidateStartWorkflowExecutionRequest(ctx, request, s.shardContext, s.namespace, "StartWorkflowExecution")
	if err != nil {
		return err
	}

	// Validation for versioning override, if any.
	err = worker_versioning.ValidateVersioningOverride(ctx, request.GetVersioningOverride(), s.matchingClient, s.versionMembershipCache, request.GetTaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, s.namespace.ID().String())
	if err != nil {
		return err
	}

	if request.RequestEagerExecution {
		metricsHandler := s.getMetricsHandler()
		metrics.WorkflowEagerExecutionCounter.With(metricsHandler).Record(1)

		// Override to false to avoid having to look up the dynamic config throughout the different code paths.
		if !s.shardContext.GetConfig().EnableEagerWorkflowStart(s.namespace.Name().String()) {
			metrics.WorkflowEagerExecutionDeniedCounter.With(metricsHandler).
				Record(1, metrics.ReasonTag(eagerStartDeniedReasonDynamicConfigDisabled))
			request.RequestEagerExecution = false
		} else if s.request.FirstWorkflowTaskBackoff.AsDuration() > 0 {
			metrics.WorkflowEagerExecutionDeniedCounter.With(metricsHandler).
				Record(1, metrics.ReasonTag(eagerStartDeniedReasonFirstWorkflowTaskBackoff))
			request.RequestEagerExecution = false
		}
	}
	return nil
}

func (s *Starter) getMetricsHandler() metrics.Handler {
	if s.metricsHandler == nil {
		s.metricsHandler = workflow.GetPerTaskQueueFamilyScope(
			s.shardContext.GetMetricsHandler(),
			s.namespace.Name(),
			s.request.StartRequest.TaskQueue.Name,
			s.shardContext.GetConfig(),
			metrics.WorkflowTypeTag(s.request.StartRequest.WorkflowType.Name),
		)
	}
	return s.metricsHandler
}

func (s *Starter) requestEagerStart() bool {
	return s.request.StartRequest.GetRequestEagerExecution()
}

// Invoke starts a new workflow execution.
// NOTE: `beforeCreateHook` might be invoked more than once in the case where the workflow policy
// requires terminating the running workflow first; it is then invoked again on the newly started workflow.
func (s *Starter) Invoke(
	ctx context.Context,
) (resp *historyservice.StartWorkflowExecutionResponse, startOutcome StartOutcome, retError error) {
	request := s.request.StartRequest
	if err := s.prepare(ctx); err != nil {
		return nil, StartErr, err
	}

	creationParams, err := s.prepareNewWorkflow(ctx, request.GetWorkflowId())
	if err != nil {
		return nil, StartErr, err
	}
	defer func() {
		creationParams.workflowLease.GetReleaseFn()(retError)
	}()

	currentExecutionLock, err := s.lockCurrentWorkflowExecution(ctx)
	if err != nil {
		return nil, StartErr, err
	}
	defer func() {
		currentExecutionLock(retError)
	}()

	err = s.createBrandNew(ctx, creationParams)
	if err != nil {
		var currentWorkflowConditionFailedError *persistence.CurrentWorkflowConditionFailedError
		if errors.As(err, &currentWorkflowConditionFailedError) && len(currentWorkflowConditionFailedError.RunID) > 0 {
			// The history and mutable state generated above will be deleted by a background process.
			return s.handleConflict(ctx, creationParams, currentWorkflowConditionFailedError)
		}

		return nil, StartErr, err
	}

	resp, err = s.generateResponse(
		creationParams.runID,
		creationParams.workflowTaskInfo,
		extractHistoryEvents(creationParams.workflowEventBatches),
	)
	return resp, StartNew, err
}

func (s *Starter) lockCurrentWorkflowExecution(
	ctx context.Context,
) (historyi.ReleaseWorkflowContextFunc, error) {
	currentRelease, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateCurrentExecution(
		ctx,
		s.shardContext,
		s.namespace.ID(),
		s.request.StartRequest.WorkflowId,
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	return currentRelease, nil
}

// prepareNewWorkflow creates a new workflow context, and closes its mutable state transaction as snapshot.
// It returns the creationContext which can later be used to insert into the executions table.
func (s *Starter) prepareNewWorkflow(ctx context.Context, workflowID string) (*creationParams, error) {
	runID := primitives.NewUUID().String()
	mutableState, err := api.NewWorkflowWithSignal(
		s.shardContext,
		s.namespace,
		workflowID,
		runID,
		s.request,
		nil,
	)
	if err != nil {
		return nil, err
	}

	workflowLease, err := s.createOrUpdateLeaseFn(nil, s.shardContext, mutableState)
	if err != nil {
		return nil, err
	}

	workflowTaskInfo := mutableState.GetStartedWorkflowTask()
	if s.requestEagerStart() && workflowTaskInfo == nil {
		return nil, softassert.UnexpectedInternalErr(
			s.shardContext.GetLogger(),
			"unexpected error: mutable state did not have a started workflow task",
			nil,
		)
	}
	workflowSnapshot, eventBatches, err := mutableState.CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	if len(eventBatches) != 1 {
		return nil, softassert.UnexpectedInternalErr(
			s.shardContext.GetLogger(),
			"unable to create 1st event batch",
			nil,
		)
	}

	return &creationParams{
		workflowID:           workflowID,
		runID:                runID,
		workflowLease:        workflowLease,
		workflowTaskInfo:     workflowTaskInfo,
		workflowSnapshot:     workflowSnapshot,
		workflowEventBatches: eventBatches,
	}, nil
}

// createBrandNew creates a "brand new" execution in the executions table.
func (s *Starter) createBrandNew(ctx context.Context, creationParams *creationParams) error {
	return creationParams.workflowLease.GetContext().CreateWorkflowExecution(
		ctx,
		s.shardContext,
		persistence.CreateWorkflowModeBrandNew,
		"", // prevRunID
		0,  // prevLastWriteVersion
		creationParams.workflowLease.GetMutableState(),
		creationParams.workflowSnapshot,
		creationParams.workflowEventBatches,
	)
}

// handleConflict handles CurrentWorkflowConditionFailedError where there's a workflow with the same workflowID.
// This may happen either when the currently handled request is a retry of a previous attempt (identified by the
// RequestID) or simply because a different run exists for the same workflow.
func (s *Starter) handleConflict(
	ctx context.Context,
	creationParams *creationParams,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) (*historyservice.StartWorkflowExecutionResponse, StartOutcome, error) {
	request := s.request.StartRequest
	currentWorkflowRequestIDs := currentWorkflowConditionFailed.RequestIDs
	if requestIDInfo, ok := currentWorkflowRequestIDs[request.GetRequestId()]; ok {
		metrics.StartWorkflowRequestDeduped.With(s.getMetricsHandler()).Record(1)

		if requestIDInfo.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			resp, err := s.respondToRetriedRequest(ctx, currentWorkflowConditionFailed.RunID)
			if resp != nil {
				resp.Status = currentWorkflowConditionFailed.Status
			}
			return resp, StartDeduped, err
		}

		resp := &historyservice.StartWorkflowExecutionResponse{
			RunId:   currentWorkflowConditionFailed.RunID,
			Started: false,
			Status:  currentWorkflowConditionFailed.Status,
			Link:    s.generateRequestIdRefLink(currentWorkflowConditionFailed.RunID),
		}
		return resp, StartDeduped, nil
	}

	if err := s.verifyNamespaceActive(creationParams, currentWorkflowConditionFailed); err != nil {
		return nil, StartErr, err
	}

	response, startOutcome, err := s.resolveDuplicateWorkflowID(ctx, currentWorkflowConditionFailed)
	if err != nil {
		return nil, StartErr, err
	} else if response != nil {
		return response, startOutcome, nil
	}

	if err := s.createAsCurrent(ctx, creationParams, currentWorkflowConditionFailed); err != nil {
		return nil, StartErr, err
	}
	resp, err := s.generateResponse(
		creationParams.runID,
		creationParams.workflowTaskInfo,
		extractHistoryEvents(creationParams.workflowEventBatches),
	)
	return resp, startOutcome, err
}

// createAsCurrent creates a new workflow execution and sets it to "current".
func (s *Starter) createAsCurrent(
	ctx context.Context,
	creationParams *creationParams,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) error {
	// TODO(stephanos): remove this hack
	// This is here for Update-with-Start to reapply the Update after it was aborted previously.
	if _, err := s.createOrUpdateLeaseFn(creationParams.workflowLease, s.shardContext, nil); err != nil {
		return err
	}
	return creationParams.workflowLease.GetContext().CreateWorkflowExecution(
		ctx,
		s.shardContext,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentWorkflowConditionFailed.RunID,
		currentWorkflowConditionFailed.LastWriteVersion,
		creationParams.workflowLease.GetMutableState(),
		creationParams.workflowSnapshot,
		creationParams.workflowEventBatches,
	)
}

func (s *Starter) verifyNamespaceActive(
	creationParams *creationParams,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) error {
	if creationParams.workflowLease.GetMutableState().GetCurrentVersion() < currentWorkflowConditionFailed.LastWriteVersion {
		clusterMetadata := s.shardContext.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(s.namespace.IsGlobalNamespace(), currentWorkflowConditionFailed.LastWriteVersion)
		return serviceerror.NewNamespaceNotActive(
			s.namespace.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}
	return nil
}

// resolveDuplicateWorkflowID determines how to resolve a duplicate workflow ID.
// Returns non-nil response if an action was required and completed successfully resulting in a newly created execution.
func (s *Starter) resolveDuplicateWorkflowID(
	ctx context.Context,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) (*historyservice.StartWorkflowExecutionResponse, StartOutcome, error) {
	workflowID := s.request.StartRequest.WorkflowId

	currentWorkflowStartTime := time.Time{}
	if s.shardContext.GetConfig().EnableWorkflowIdReuseStartTimeValidation(s.namespace.Name().String()) &&
		currentWorkflowConditionFailed.StartTime != nil {
		currentWorkflowStartTime = *currentWorkflowConditionFailed.StartTime
	}

	workflowKey := definition.NewWorkflowKey(
		s.namespace.ID().String(),
		workflowID,
		currentWorkflowConditionFailed.RunID,
	)

	// Using a new RunID here to simplify locking: MultiOperation, that re-uses the Starter, is creating
	// a locked workflow context for each new workflow. Using a fresh RunID prevents a deadlock with the
	// previously created workflow context.
	newRunID := primitives.NewUUID().String()

	currentExecutionUpdateAction, err := api.ResolveDuplicateWorkflowID(
		s.shardContext,
		workflowKey,
		s.namespace,
		newRunID,
		currentWorkflowConditionFailed.State,
		currentWorkflowConditionFailed.Status,
		currentWorkflowConditionFailed.RequestIDs,
		s.request.StartRequest.GetWorkflowIdReusePolicy(),
		s.request.StartRequest.GetWorkflowIdConflictPolicy(),
		currentWorkflowStartTime,
		s.request.ParentExecutionInfo,
		s.request.ChildWorkflowOnly,
	)

	switch {
	case errors.Is(err, api.ErrUseCurrentExecution):
		return s.handleUseExistingWorkflowOnConflictOptions(
			ctx,
			workflowKey,
			currentWorkflowConditionFailed,
			currentWorkflowStartTime,
		)
	case err != nil:
		return nil, StartErr, err
	case currentExecutionUpdateAction == nil:
		return nil, StartNew, nil
	}

	// handle terminating the current execution (currentWorkflowUpdateAction) and starting a new workflow
	var workflowLease api.WorkflowLease
	var mutableStateInfo *mutableStateInfo
	// Update current execution and create new execution in one transaction.
	// We already validated that currentWorkflowConditionFailed.RunID is not empty,
	// so the following update won't try to lock the current execution again.
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		currentExecutionUpdateAction,
		func() (historyi.WorkflowContext, historyi.MutableState, error) {
			newMutableState, err := api.NewWorkflowWithSignal(
				s.shardContext,
				s.namespace,
				workflowID,
				newRunID,
				s.request,
				nil)
			if err != nil {
				return nil, nil, err
			}

			workflowLease, err = s.createOrUpdateLeaseFn(nil, s.shardContext, newMutableState)
			if err != nil {
				return nil, nil, err
			}

			// extract information from MutableState in case this is an eager start
			mutableState := workflowLease.GetMutableState()
			mutableStateInfo, err = extractMutableStateInfo(mutableState)
			if err != nil {
				return nil, nil, err
			}

			return workflowLease.GetContext(), mutableState, nil
		},
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	if workflowLease != nil {
		workflowLease.GetReleaseFn()(err)
	}

	switch err {
	case nil:
		if !s.requestEagerStart() {
			return &historyservice.StartWorkflowExecutionResponse{
				RunId:   newRunID,
				Started: true,
				Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Link:    s.generateStartedEventRefLink(newRunID),
			}, StartNew, nil
		}
		events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
		if err != nil {
			return nil, StartErr, err
		}
		resp, err := s.generateResponse(newRunID, mutableStateInfo.workflowTask, events)
		return resp, StartNew, err
	case consts.ErrWorkflowCompleted:
		// Exit and retry again from the top.
		// By returning an Unavailable service error, the entire Start request will be retried.
		// NOTE: This WorkflowIDReusePolicy cannot be RejectDuplicate as the frontend will reject that.
		return nil, StartErr, serviceerror.NewUnavailablef("Termination failed: %v", err)
	default:
		return nil, StartErr, err
	}
}

// respondToRetriedRequest provides a response in case a start request is retried.
//
// NOTE: Workflow is marked as "started" even though the client re-issued a request with the same ID,
// as that provides a consistent response to the client.
func (s *Starter) respondToRetriedRequest(
	ctx context.Context,
	runID string,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	if !s.requestEagerStart() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId:   runID,
			Started: true,
			// Status is set by caller
			Link: s.generateStartedEventRefLink(runID),
		}, nil
	}

	// For eager workflow execution, we need to get the task info and history events in order to construct a poll response.
	mutableStateInfo, err := s.getMutableStateInfo(ctx, runID)
	if err != nil {
		return nil, err
	}

	// The current workflow task is not started or not the first task or we exceeded the first attempt and fell back to
	// matching based dispatch.
	if mutableStateInfo.workflowTask == nil || mutableStateInfo.workflowTask.StartedEventID != 3 || mutableStateInfo.workflowTask.Attempt > 1 {
		metrics.WorkflowEagerExecutionDeniedCounter.With(s.getMetricsHandler()).
			Record(1, metrics.ReasonTag(eagerStartDeniedReasonTaskAlreadyDispatched))

		return &historyservice.StartWorkflowExecutionResponse{
			RunId:   runID,
			Started: true,
			// Status is set by caller
			Link: s.generateStartedEventRefLink(runID),
		}, nil
	}

	events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
	if err != nil {
		return nil, err
	}

	return s.generateResponse(runID, mutableStateInfo.workflowTask, events)
}

// getMutableStateInfo gets the relevant mutable state information while getting the state for the given run from the
// workflow cache and managing the cache lease.
func (s *Starter) getMutableStateInfo(ctx context.Context, runID string) (_ *mutableStateInfo, retErr error) {
	// We technically never want to create a new execution but in practice this should not happen.
	workflowContext, releaseFn, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		s.namespace.ID(),
		&commonpb.WorkflowExecution{WorkflowId: s.request.StartRequest.WorkflowId, RunId: runID},
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { releaseFn(retErr) }()

	ms, err := workflowContext.LoadMutableState(ctx, s.shardContext)
	if err != nil {
		return nil, err
	}

	return extractMutableStateInfo(ms)
}

// extractMutableStateInfo extracts the relevant information to generate a start response with an eager workflow task.
func extractMutableStateInfo(mutableState historyi.MutableState) (*mutableStateInfo, error) {
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	// Future work for the request retry path: extend the task timeout (by failing / timing out the current task).
	workflowTaskSource := mutableState.GetStartedWorkflowTask()
	// The workflowTask returned from the mutable state call is generated on the fly and technically doesn't require
	// cloning. We clone here just in case that changes.
	var workflowTask historyi.WorkflowTaskInfo
	if workflowTaskSource != nil {
		workflowTask = *workflowTaskSource
	}

	return &mutableStateInfo{
		branchToken:  branchToken,
		lastEventID:  mutableState.GetNextEventID() - 1,
		workflowTask: &workflowTask,
	}, nil
}

// getWorkflowHistory loads the workflow history based on given mutable state information from the DB.
func (s *Starter) getWorkflowHistory(ctx context.Context, mutableState *mutableStateInfo) ([]*historypb.HistoryEvent, error) {
	var events []*historypb.HistoryEvent
	// Future optimization: generate the task from mutable state to save the extra DB read.
	// NOTE: While unlikely that there'll be more than one page, it's safer to make less assumptions.
	// TODO: Frontend also supports returning raw history and it's controlled by a feature flag (yycptt thinks).
	for {
		response, err := s.shardContext.GetExecutionManager().ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:     s.shardContext.GetShardID(),
			BranchToken: mutableState.branchToken,
			MinEventID:  1,
			MaxEventID:  mutableState.lastEventID,
			PageSize:    1024,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, response.HistoryEvents...)
		if len(response.NextPageToken) == 0 {
			break
		}
	}

	return events, nil
}

func (s *Starter) handleUseExistingWorkflowOnConflictOptions(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
	currentWorkflowStartTime time.Time,
) (*historyservice.StartWorkflowExecutionResponse, StartOutcome, error) {
	// Default response link is for the started event. If there is OnConflictOptions set, and it's
	// attaching the request ID, then the response link will be a request ID reference.
	responseLink := s.generateStartedEventRefLink(currentWorkflowConditionFailed.RunID)

	var err error
	onConflictOptions := s.request.StartRequest.GetOnConflictOptions()
	if onConflictOptions != nil {
		requestID := ""
		if onConflictOptions.AttachRequestId {
			requestID = s.request.StartRequest.GetRequestId()
			responseLink = s.generateRequestIdRefLink(currentWorkflowConditionFailed.RunID)
		}
		var completionCallbacks []*commonpb.Callback
		if onConflictOptions.AttachCompletionCallbacks {
			completionCallbacks = s.request.StartRequest.GetCompletionCallbacks()
		}
		var links []*commonpb.Link
		if onConflictOptions.AttachLinks {
			links = s.request.StartRequest.GetLinks()
		}
		err = api.GetAndUpdateWorkflowWithNew(
			ctx,
			nil,
			workflowKey,
			func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
				mutableState := workflowLease.GetMutableState()
				if !mutableState.IsWorkflowExecutionRunning() {
					return nil, consts.ErrWorkflowCompleted
				}

				_, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(
					nil,
					false,
					requestID,
					completionCallbacks,
					links,
					"",  // identity
					nil, // priority
				)
				return api.UpdateWorkflowWithoutWorkflowTask, err
			},
			nil, // no new workflow
			s.shardContext,
			s.workflowConsistencyChecker,
		)
	}

	switch err {
	case nil:
		resp := &historyservice.StartWorkflowExecutionResponse{
			RunId:   workflowKey.RunID,
			Started: false, // set explicitly for emphasis
			Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Link:    responseLink,
		}
		return resp, StartReused, nil
	case consts.ErrWorkflowCompleted:
		// Need to re-evaluate the reuse policy because the previous check didn't lock the current
		// execution. So it's possible the workflow completed after the first call of
		// api.ResolveDuplicateWorkflowID and before being able to update the existing workflow.
		err := api.ResolveWorkflowIDReusePolicy(
			s.shardContext,
			workflowKey,
			s.namespace,
			currentWorkflowConditionFailed.Status,
			currentWorkflowConditionFailed.RequestIDs,
			s.request.StartRequest.GetWorkflowIdReusePolicy(),
			currentWorkflowStartTime,
		)
		if err != nil {
			return nil, StartErr, err
		}
		// no error means allowing duplicate workflow id
		// fallthrough to the logic creating a new workflow
		return nil, StartNew, nil
	default:
		return nil, StartErr, err
	}
}

// extractHistoryEvents extracts all history events from a batch of events sent to persistence.
// It's unlikely that persistence events would span multiple batches but better safe than sorry.
func extractHistoryEvents(persistenceEvents []*persistence.WorkflowEvents) []*historypb.HistoryEvent {
	if len(persistenceEvents) == 1 {
		return persistenceEvents[0].Events
	}
	var events []*historypb.HistoryEvent
	for _, page := range persistenceEvents {
		events = append(events, page.Events...)
	}
	return events
}

// generateResponse is a helper for generating StartWorkflowExecutionResponse for eager and non-eager workflow start
// requests.
func (s *Starter) generateResponse(
	runID string,
	workflowTaskInfo *historyi.WorkflowTaskInfo,
	historyEvents []*historypb.HistoryEvent,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	shardCtx := s.shardContext
	tokenSerializer := s.tokenSerializer
	request := s.request.StartRequest
	workflowID := request.WorkflowId

	if !s.requestEagerStart() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId:   runID,
			Started: true,
			Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			Link:    s.generateStartedEventRefLink(runID),
		}, nil
	}

	clock, err := shardCtx.NewVectorClock()
	if err != nil {
		return nil, err
	}

	taskToken := tasktoken.NewWorkflowTaskToken(
		s.namespace.ID().String(),
		workflowID,
		runID,
		workflowTaskInfo.ScheduledEventID,
		workflowTaskInfo.StartedEventID,
		timestamppb.New(workflowTaskInfo.StartedTime),
		workflowTaskInfo.Attempt,
		clock,
		workflowTaskInfo.Version,
	)
	serializedToken, err := tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId:   runID,
		Clock:   clock,
		Started: true,
		Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Link:    s.generateStartedEventRefLink(runID),
		EagerWorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{
			TaskToken:         serializedToken,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			WorkflowType:      request.GetWorkflowType(),
			// TODO: consider getting the ID from mutable state, this was not done to avoid adding more complexity to
			// the code to plumb that value through.
			PreviousStartedEventId:     0,
			StartedEventId:             workflowTaskInfo.StartedEventID,
			Attempt:                    workflowTaskInfo.Attempt,
			History:                    &historypb.History{Events: historyEvents},
			NextPageToken:              nil,
			WorkflowExecutionTaskQueue: workflowTaskInfo.TaskQueue,
			ScheduledTime:              timestamppb.New(workflowTaskInfo.ScheduledTime),
			StartedTime:                timestamppb.New(workflowTaskInfo.StartedTime),
		},
	}, nil
}

func (s *Starter) generateStartedEventRefLink(runID string) *commonpb.Link {
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  s.namespace.Name().String(),
				WorkflowId: s.request.StartRequest.WorkflowId,
				RunId:      runID,
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   common.FirstEventID,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
	}
}

func (s *Starter) generateRequestIdRefLink(runID string) *commonpb.Link {
	if !s.enableRequestIdRefLinks() {
		return s.generateStartedEventRefLink(runID)
	}
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  s.namespace.Name().String(),
				WorkflowId: s.request.StartRequest.WorkflowId,
				RunId:      runID,
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: s.request.StartRequest.RequestId,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
					},
				},
			},
		},
	}
}

func (s StartOutcome) String() string {
	switch s {
	case StartErr:
		return "StartErr"
	case StartNew:
		return "StartNew"
	case StartReused:
		return "StartReused"
	case StartDeduped:
		return "StartDeduped"
	default:
		return "Unknown"
	}
}
