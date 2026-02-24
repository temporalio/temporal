package recordworkflowtaskstarted

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//nolint:revive // cyclomatic complexity
func Invoke(
	ctx context.Context,
	req *historyservice.RecordWorkflowTaskStartedRequest,
	shardContext historyi.ShardContext,
	config *configs.Config,
	eventNotifier events.Notifier,
	persistenceVisibilityMgr manager.VisibilityManager,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), req.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}

	scheduledEventID := req.GetScheduledEventId()
	requestID := req.GetRequestId()

	var workflowKey definition.WorkflowKey
	var resp *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (res *api.UpdateWorkflowAction, retErr error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)
			if workflowTask == nil {
				// This can happen if (one of):
				//  - WFT is already completed as a result of another call (safe to drop this WFT),
				//  - Speculative WFT is lost (ScheduleToStart timeout for speculative WFT will recreate it).
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}
			if req.GetStamp() != mutableState.GetExecutionInfo().GetWorkflowTaskStamp() {
				// This happens when the workflow task was rescheduled.
				return nil, serviceerrors.NewObsoleteMatchingTask("Workflow task stamp mismatch")
			}

			metricsScope := shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryRecordWorkflowTaskStartedScope))

			// Check to see if mutable cache is stale in some extreme cassandra failure cases.
			// For speculative and transient WFT scheduledEventID is always ahead of NextEventID.
			// Because there is a clock check above the stack, this should never happen.
			transientWFT := workflowTask.Attempt > 1
			if workflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && !transientWFT &&
				scheduledEventID >= mutableState.GetNextEventID() {

				metrics.StaleMutableStateCounter.With(metricsScope).Record(1)
				return nil, consts.ErrStaleState
			}

			workflowKey = mutableState.GetWorkflowKey()
			updateAction := &api.UpdateWorkflowAction{}
			updateRegistry := workflowLease.GetContext().UpdateRegistry(ctx)

			if workflowTask.StartedEventID != common.EmptyEventID {
				// If workflow task is started as part of the current request scope then return a positive response
				if workflowTask.RequestID == requestID {
					resp, err = CreateRecordWorkflowTaskStartedResponseWithRawHistory(ctx, mutableState, updateRegistry, workflowTask, req.PollRequest.GetIdentity(), false)
					if err != nil {
						return nil, err
					}
					updateAction.Noop = true
					return updateAction, nil
				}

				// Looks like WorkflowTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}

			// Assuming a workflow is running on a sticky task queue by a workerA.
			// After workerA is dead for more than 10s, matching will return StickyWorkerUnavailable error when history
			// tries to push a new workflow task. When history sees that error, it will fall back to push the task to
			// its original normal task queue without clearing its stickiness to avoid an extra persistence write.
			// We will clear the stickiness here when that task is delivered to another worker polling from normal queue.
			// The stickiness info is used by frontend to decide if it should send down partial history or full history.
			// Sending down partial history will cost the worker an extra fetch to server for the full history.
			currentTaskQueue := mutableState.CurrentTaskQueue()
			pollerTaskQueue := req.PollRequest.TaskQueue
			if currentTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY &&
				currentTaskQueue.GetName() != pollerTaskQueue.GetName() {
				// For versioned workflows we additionally check for the poller queue to not be a sticky queue itself.
				// Although it's ideal to check this for unversioned workflows as well, we can't rely on older clients
				// setting the poller TQ kind.
				if (mutableState.GetAssignedBuildId() != "" ||
					mutableState.GetEffectiveVersioningBehavior() != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED) &&
					pollerTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
					return nil, serviceerrors.NewObsoleteDispatchBuildId("wrong sticky queue")
				}
				// req.PollRequest.TaskQueue.GetName() may include partition, but we only check when sticky is enabled,
				// and sticky queue never has partition, so it does not matter.
				mutableState.ClearStickyTaskQueue()
			}

			if currentTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_NORMAL &&
				pollerTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
				// A poll from a sticky queue while the workflow's task queue is not yet sticky
				// should be rejected. This means the task was a stale task on the matching queue.
				// Matching can drop the task, newer one should be scheduled already.
				// Note that the other way around is not true: a poll from a normal task is valid
				// even if the workflow's queue is sticky. It could be that the transfer task had to
				// be scheduled in the normal task because matching returned a `StickyWorkerUnavailable`
				// error. In that case, the mutable state is not updated at task transfer time until
				// the workflow task starts on the normal queue, and we clear MS's sticky queue.
				return nil, serviceerrors.NewObsoleteMatchingTask("rejecting sticky poller because workflow has moved to normal queue")
			}

			wfBehavior := mutableState.GetEffectiveVersioningBehavior()
			wfDeployment := mutableState.GetEffectiveDeployment()
			//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
			pollerDeployment, err := worker_versioning.DeploymentFromCapabilities(req.PollRequest.WorkerVersionCapabilities, req.PollRequest.DeploymentOptions)
			if err != nil {
				return nil, err
			}
			err = worker_versioning.ValidateTaskVersionDirective(req.GetVersionDirective(), wfBehavior, wfDeployment, req.ScheduledDeployment)
			if err != nil {
				return nil, err
			}

			_, workflowTask, err = mutableState.AddWorkflowTaskStartedEvent(
				scheduledEventID,
				requestID,
				pollerTaskQueue,
				req.PollRequest.Identity,
				worker_versioning.StampFromCapabilities(req.PollRequest.WorkerVersionCapabilities),
				req.GetBuildIdRedirectInfo(),
				workflowLease.GetContext().UpdateRegistry(ctx),
				false,
				req.TargetDeploymentVersion,
			)
			if err != nil {
				// Unable to add WorkflowTaskStarted event to history
				return nil, err
			}

			if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
				updateAction.Noop = true
			} else {
				// If the wft is speculative MS changes are not persisted, so the possibly started
				// transition by the StartDeploymentTransition call above won't be persisted. This is OK
				// because once the speculative task completes the transition will be applied
				// automatically based on wft completion info. If the speculative task fails or times
				// out, future wft will be redirected by matching again and the transition will
				// eventually happen. If an activity starts while the speculative is also started on the
				// new deployment, the activity will cause the transition to be created and persisted in
				// the MS.
				if !pollerDeployment.Equal(wfDeployment) {
					// Dispatching to a different deployment. Try starting a transition. Starting the
					// transition AFTER applying the start event because we don't want this pending
					// wft to be rescheduled by StartDeploymentTransition.
					if err := mutableState.StartDeploymentTransition(pollerDeployment, req.TaskDispatchRevisionNumber); err != nil {
						if errors.Is(err, workflow.ErrPinnedWorkflowCannotTransition) {
							// This must be a task from a time that the workflow was unpinned, but it's
							// now pinned so can't transition. Matching can drop the task safely.
							// TODO (shahab): remove this special error check because it is not
							// expected to happen once scheduledBehavior is always populated. see TODOs above.
							return nil, serviceerrors.NewObsoleteMatchingTask(err.Error())
						}
						return nil, err
					}
				}
			}

			workflowScheduleToStartLatency := workflowTask.StartedTime.Sub(workflowTask.ScheduledTime)
			namespaceName := namespaceEntry.Name()
			tqPartition := tqid.UnsafePartitionFromProto(workflowTask.TaskQueue, req.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			metrics.TaskScheduleToStartLatency.With(
				metrics.GetPerTaskQueuePartitionTypeScope(
					metricsScope,
					namespaceName.String(),
					tqPartition,
					config.BreakdownMetricsByTaskQueue(namespaceName.String(), tqPartition.TaskQueue().Name(), enumspb.TASK_QUEUE_TYPE_WORKFLOW),
				),
			).Record(workflowScheduleToStartLatency)

			resp, err = CreateRecordWorkflowTaskStartedResponseWithRawHistory(
				ctx,
				mutableState,
				updateRegistry,
				workflowTask,
				req.PollRequest.GetIdentity(),
				false,
			)
			if err != nil {
				return nil, err
			}

			return updateAction, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	maxHistoryPageSize := int32(config.HistoryMaxPageSize(namespaceEntry.Name().String()))
	err = setHistoryForRecordWfTaskStartedResp(
		ctx,
		shardContext,
		workflowKey,
		namespaceEntry.Name(),
		maxHistoryPageSize,
		workflowConsistencyChecker,
		eventNotifier,
		persistenceVisibilityMgr,
		resp,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func setHistoryForRecordWfTaskStartedResp(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	namespaceName namespace.Name,
	maximumPageSize int32,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	persistenceVisibilityMgr manager.VisibilityManager,
	response *historyservice.RecordWorkflowTaskStartedResponseWithRawHistory,
) (retError error) {

	firstEventID := common.FirstEventID
	nextEventID := response.GetNextEventId()
	if response.GetStickyExecutionEnabled() {
		// sticky tasks only need partial history
		firstEventID = response.GetPreviousStartedEventId() + 1
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		var dataLossErr *serviceerror.DataLoss
		if errors.As(retError, &dataLossErr) {
			api.TrimHistoryNode(
				ctx,
				shardContext,
				workflowConsistencyChecker,
				eventNotifier,
				workflowKey.GetNamespaceID(),
				workflowKey.GetWorkflowID(),
				workflowKey.GetRunID(),
			)
		}
	}()

	isInternalRawHistoryEnabled := shardContext.GetConfig().SendRawHistoryBetweenInternalServices()
	var rawHistory []*commonpb.DataBlob
	var persistenceToken []byte
	var history *historypb.History
	var err error
	if isInternalRawHistoryEnabled {
		rawHistory, persistenceToken, err = api.GetRawHistory(
			ctx,
			shardContext,
			namespaceName,
			namespace.ID(workflowKey.GetNamespaceID()),
			&commonpb.WorkflowExecution{WorkflowId: workflowKey.GetWorkflowID(), RunId: workflowKey.GetRunID()},
			firstEventID,
			nextEventID,
			maximumPageSize,
			nil,
			response.GetTransientWorkflowTask(),
			response.GetBranchToken(),
		)
	} else {
		history, persistenceToken, err = api.GetHistory(
			ctx,
			shardContext,
			namespaceName,
			namespace.ID(workflowKey.GetNamespaceID()),
			&commonpb.WorkflowExecution{WorkflowId: workflowKey.GetWorkflowID(), RunId: workflowKey.GetRunID()},
			firstEventID,
			nextEventID,
			maximumPageSize,
			nil,
			response.GetTransientWorkflowTask(),
			response.GetBranchToken(),
			persistenceVisibilityMgr,
		)
	}
	if err != nil {
		return err
	}

	var continuation []byte
	if len(persistenceToken) != 0 {
		continuation, err = api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
			RunId:            workflowKey.GetRunID(),
			FirstEventId:     firstEventID,
			NextEventId:      nextEventID,
			PersistenceToken: persistenceToken,
			BranchToken:      response.GetBranchToken(),
		})
		if err != nil {
			return err
		}
	}
	if isInternalRawHistoryEnabled {
		historyBlobs := make([][]byte, len(rawHistory))
		for i, blob := range rawHistory {
			historyBlobs[i] = blob.Data
		}
		if shardContext.GetConfig().SendRawHistoryBytesToMatchingService() {
			response.RawHistoryBytes = historyBlobs
		} else {
			response.RawHistory = historyBlobs //nolint:staticcheck // SA1019: Using deprecated field for backwards compatibility during rollout
		}
	} else {
		response.History = history
	}
	response.NextPageToken = continuation
	return nil
}

func CreateRecordWorkflowTaskStartedResponse(
	ctx context.Context,
	ms historyi.MutableState,
	updateRegistry update.Registry,
	workflowTask *historyi.WorkflowTaskInfo,
	identity string,
	wtHeartbeat bool,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	rawResp, err := CreateRecordWorkflowTaskStartedResponseWithRawHistory(ctx, ms, updateRegistry, workflowTask, identity, wtHeartbeat)
	if err != nil {
		return nil, err
	}
	return &historyservice.RecordWorkflowTaskStartedResponse{
		WorkflowType:               rawResp.WorkflowType,
		PreviousStartedEventId:     rawResp.PreviousStartedEventId,
		ScheduledEventId:           rawResp.ScheduledEventId,
		StartedEventId:             rawResp.StartedEventId,
		NextEventId:                rawResp.NextEventId,
		Attempt:                    rawResp.Attempt,
		StickyExecutionEnabled:     rawResp.StickyExecutionEnabled,
		TransientWorkflowTask:      rawResp.TransientWorkflowTask,
		WorkflowExecutionTaskQueue: rawResp.WorkflowExecutionTaskQueue,
		BranchToken:                rawResp.BranchToken,
		ScheduledTime:              rawResp.ScheduledTime,
		StartedTime:                rawResp.StartedTime,
		Queries:                    rawResp.Queries,
		Clock:                      rawResp.Clock,
		Messages:                   rawResp.Messages,
		Version:                    rawResp.Version,
		NextPageToken:              rawResp.NextPageToken,
	}, nil
}

func CreateRecordWorkflowTaskStartedResponseWithRawHistory(
	ctx context.Context,
	ms historyi.MutableState,
	updateRegistry update.Registry,
	workflowTask *historyi.WorkflowTaskInfo,
	identity string,
	wtHeartbeat bool,
) (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
	response := &historyservice.RecordWorkflowTaskStartedResponseWithRawHistory{}
	response.WorkflowType = ms.GetWorkflowType()
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.LastCompletedWorkflowTaskStartedEventId != common.EmptyEventID {
		response.PreviousStartedEventId = executionInfo.LastCompletedWorkflowTaskStartedEventId
	}

	// Starting workflowTask could result in different scheduledEventID if workflowTask was transient and new events came in
	// before it was started.
	response.ScheduledEventId = workflowTask.ScheduledEventID
	response.StartedEventId = workflowTask.StartedEventID
	response.StickyExecutionEnabled = ms.IsStickyTaskQueueSet()
	response.NextEventId = ms.GetNextEventID()
	response.Attempt = workflowTask.Attempt
	response.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	response.ScheduledTime = timestamppb.New(workflowTask.ScheduledTime)
	response.StartedTime = timestamppb.New(workflowTask.StartedTime)
	response.Version = workflowTask.Version

	// TODO (alex-update): Transient needs to be renamed to "TransientOrSpeculative"
	response.TransientWorkflowTask = ms.GetTransientWorkflowTaskInfo(workflowTask, identity)

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	qr := ms.GetQueryRegistry()
	bufferedQueryIDs := qr.GetBufferedIDs()
	if len(bufferedQueryIDs) > 0 {
		response.Queries = make(map[string]*querypb.WorkflowQuery, len(bufferedQueryIDs))
		for _, bufferedQueryID := range bufferedQueryIDs {
			input, err := qr.GetQueryInput(bufferedQueryID)
			if err != nil {
				continue
			}
			response.Queries[bufferedQueryID] = input
		}
	}

	// If there are updates in the registry which were already sent to worker but still
	// are not processed and not rejected by server, it means that WT that was used to
	// deliver those updates failed, got timed out, or got lost.
	// Resend these updates if this is not a heartbeat WT (includeAlreadySent = !wtHeartbeat).
	// Heartbeat WT delivers only new updates that come while this WT was running (similar to queries and buffered events).
	response.Messages = updateRegistry.Send(ctx, !wtHeartbeat, workflowTask.StartedEventID)

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && len(response.GetMessages()) == 0 {
		return nil, serviceerror.NewNotFound("No messages for speculative workflow task.")
	}

	return response, nil
}
