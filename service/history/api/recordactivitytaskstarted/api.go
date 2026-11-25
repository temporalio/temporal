package recordactivitytaskstarted

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/priorities"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

type rejectCode int32

const (
	rejectCodeUndefined rejectCode = iota
	rejectCodeAccepted
	rejectCodePaused
	rejectCodeStartedTransition
	rejectCodeReplayed
	rejectCodeReplaced
)

func Invoke(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
	recovery *historyservice.RecoverActivityTaskStartedRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	routingInfoCache worker_versioning.RoutingInfoCache,
) (resp *historyservice.RecordActivityTaskStartedResponse, retError error) {
	var err error
	response := &historyservice.RecordActivityTaskStartedResponse{}
	var rejectCode rejectCode

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		request.Clock,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (resp *api.UpdateWorkflowAction, retErr error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			response, rejectCode, err = recordActivityTaskStarted(
				ctx, shardContext, mutableState, request, recovery, matchingClient, routingInfoCache,
			)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop: rejectCode == rejectCodeReplayed,
				// Create new wft if a transition started with this activity.
				// StartDeploymentTransition rescheduled pending wft, but this creates new
				// one if there is no pending wft.
				CreateWorkflowTask: rejectCode == rejectCodeStartedTransition,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	if rejectCode == rejectCodeReplaced {
		return nil, serviceerrors.NewObsoleteMatchingTask("Activity transfer replaced by the active namespace")
	}
	if rejectCode == rejectCodeStartedTransition {
		// Rejecting the activity start because the workflow is now in transition. Matching can drop
		// the task, new activity task will be scheduled after transition completion.
		return nil, serviceerrors.NewActivityStartDuringTransition()
	}

	if rejectCode == rejectCodePaused {
		// Rejecting the activity start because activity was modified (paused_.
		// Matching can drop the task. New activity will be scheduled once activity is resumed.
		errorMessage := fmt.Sprintf("Activity task with this stamp not found: %v", request.Stamp)
		return nil, serviceerror.NewNotFound(errorMessage)
	}

	return response, err
}

func recordActivityTaskStarted(
	ctx context.Context,
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	request *historyservice.RecordActivityTaskStartedRequest,
	recovery *historyservice.RecoverActivityTaskStartedRequest,
	matchingClient matchingservice.MatchingServiceClient,
	routingInfoCache worker_versioning.RoutingInfoCache,
) (*historyservice.RecordActivityTaskStartedResponse, rejectCode, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()), request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, rejectCodeUndefined, err
	}
	namespaceName := namespaceEntry.Name().String()

	scheduledEventID := request.GetScheduledEventId()
	requestID := request.GetRequestId()
	ai, isRunning := mutableState.GetActivityInfo(scheduledEventID)

	taggedMetrics := shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope))

	// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
	// some extreme cassandra failure cases.
	if !isRunning && scheduledEventID >= mutableState.GetNextEventID() {
		metrics.StaleMutableStateCounter.With(taggedMetrics).Record(1)
		return nil, rejectCodeUndefined, consts.ErrStaleState
	}

	// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
	// task is not outstanding than it is most probably a duplicate and complete the task.
	if !isRunning {
		// Looks like ActivityTask already completed as a result of another call.
		// It is OK to drop the task at this point.
		return nil, rejectCodeUndefined, consts.ErrActivityTaskNotFound
	}

	if request.ActivityTransferInfo != nil {
		code, err := checkActivityTransfer(mutableState, ai, request, namespaceEntry.FailoverVersion(request.WorkflowExecution.WorkflowId))
		if err != nil || code == rejectCodeReplaced {
			return nil, code, err
		}
	}

	scheduledEvent, err := mutableState.GetActivityScheduledEvent(ctx, scheduledEventID)
	if err != nil {
		return nil, rejectCodeUndefined, err
	}

	response := &historyservice.RecordActivityTaskStartedResponse{
		ScheduledEvent:              scheduledEvent,
		CurrentAttemptScheduledTime: ai.ScheduledTime,
		Priority:                    priorities.Merge(mutableState.GetExecutionInfo().Priority, ai.Priority),
	}

	if ai.StartedEventId != common.EmptyEventID {
		prior := recovery.GetWithheldStartRequestId()
		if ai.RequestId != requestID && (prior == "" || ai.RequestId != prior) {
			return nil, rejectCodeUndefined, serviceerrors.NewTaskAlreadyStarted("Activity")
		}
		response.Clock = ai.StartedClock
		if request.ActivityTransferInfo != nil {
			if err := checkRecordedStart(mutableState, ai, request); err != nil {
				return nil, rejectCodeUndefined, err
			}
			populateStartResponse(response, ai, mutableState, namespaceName)
			return response, rejectCodeReplayed, nil
		}
		// Legacy starts may predate the persisted clock.
		if response.Clock == nil {
			response.Clock, err = shardContext.NewVectorClock()
			if err != nil {
				return nil, rejectCodeUndefined, err
			}
		}
		response.StartedTime = ai.StartedTime
		response.Attempt = ai.Attempt
		return response, rejectCodeAccepted, nil
	}

	code, err := processActivityWorkflowRules(shardContext, request, mutableState, ai)
	if err != nil || code == rejectCodePaused {
		return nil, code, err
	}

	if ai.Stamp != request.Stamp {
		// This happens when the workflow task was rescheduled.
		errorMessage := fmt.Sprintf(
			"Activity task rejected; stamp has changed. Id: %s,: type: %s, current stamp: %d",
			ai.ActivityId, ai.ActivityType.Name, ai.Stamp)
		return nil, rejectCodeUndefined, serviceerrors.NewObsoleteMatchingTask(errorMessage)
	}

	wfBehavior := mutableState.GetEffectiveVersioningBehavior()
	wfDeployment := mutableState.GetEffectiveDeployment()
	//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
	pollerDeployment, err := worker_versioning.DeploymentFromCapabilities(request.PollRequest.WorkerVersionCapabilities, request.PollRequest.DeploymentOptions)
	if err != nil {
		return nil, rejectCodeUndefined, err
	}
	err = worker_versioning.ValidateTaskVersionDirective(request.GetVersionDirective(), wfBehavior, wfDeployment, request.ScheduledDeployment)
	if err != nil {
		return nil, rejectCodeUndefined, err
	}

	if mutableState.GetDeploymentTransition() != nil {
		// Can't start activity during a redirect. We reject this request so Matching drops
		// the task. The activity will be rescheduled when the redirect completes/fails.
		return nil, rejectCodeUndefined, serviceerrors.NewActivityStartDuringTransition()
	}

	if !pollerDeployment.Equal(wfDeployment) &&
		// Independent activities of pinned workflows are redirected. They should not start a transition on wf.
		wfBehavior != enumspb.VERSIONING_BEHAVIOR_PINNED {
		// AT of an unpinned workflow is redirected, see if a transition on the workflow should start.
		// The workflow transition happens only if the workflow task of the same execution would go
		// to the poller deployment. Otherwise, it means the activity is independently versioned, we
		// allow it to start without affecting the workflow.

		wftDepVer, wftDepRevNum, err := getDeploymentVersionAndRevisionNumberForWorkflowID(ctx,
			request.NamespaceId,
			mutableState.GetExecutionInfo().GetTaskQueue(),
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			matchingClient,
			routingInfoCache,
			mutableState.GetWorkflowKey().WorkflowID,
		)
		if err != nil {
			// Let matching retry
			return nil, rejectCodeUndefined, err
		}

		// We start a transition if one of the following conditions are met:
		// 1. The workflow will be dispatching to the same deployment as the activity but has not yet.
		// 2. The workflow TQ is lagging behind the activity TQ, with respect to the current version of a deployment.

		// Note: We use > instead of >= because a non-backlogged activity task could have the same revision number as the MS and that should not commence a transition.
		// Note: Revision number mechanics are only involved if the dynamic config is enabled.
		useRevisionNumber := shardContext.GetConfig().UseRevisionNumberForWorkerVersioning(namespaceName)
		if pollerDeployment.Equal(worker_versioning.DeploymentFromDeploymentVersion(wftDepVer)) ||
			(useRevisionNumber && pollerDeployment.GetSeriesName() == wftDepVer.GetDeploymentName() && request.TaskDispatchRevisionNumber > wftDepRevNum) {
			if err := mutableState.StartDeploymentTransition(pollerDeployment, request.TaskDispatchRevisionNumber); err != nil {
				if errors.Is(err, workflow.ErrPinnedWorkflowCannotTransition) {
					// This must be a task from a time that the workflow was unpinned, but it's
					// now pinned so can't transition. Matching can drop the task safely.
					// TODO (shahab): remove this special error check because it is not
					// expected to happen once scheduledBehavior is always populated. see TODOs above.
					return nil, rejectCodeUndefined, serviceerrors.NewObsoleteMatchingTask(err.Error())
				}
				return nil, rejectCodeUndefined, err
			}

			// This activity started a transition, make sure the MS changes are written but
			// reject the activity task.
			return nil, rejectCodeStartedTransition, nil
		}
	}

	// Create the shard clock before recording the start event. Matching uses the returned clock
	// to build the task token sent to the worker. We store it in ActivityInfo so that history
	// can later reconstruct the same task token that matching created (e.g. for cancel worker
	// commands). On retries of this RPC, we return the stored clock from the early return
	// path above.
	clock, err := shardContext.NewVectorClock()
	if err != nil {
		return nil, rejectCodeUndefined, err
	}
	versioningStamp := worker_versioning.StampFromCapabilities(request.PollRequest.WorkerVersionCapabilities, request.PollRequest.DeploymentOptions) //nolint:staticcheck // SA1019: WorkerVersionCapabilities is deprecated but still used for old versioning [cleanup-old-wv]
	err = mutableState.UpdateActivity(scheduledEventID, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		activityInfo.Limiters = request.GetLimiters()
		return nil
	})
	if err != nil {
		return nil, rejectCodeUndefined, err
	}
	if _, err := mutableState.AddActivityTaskStartedEvent(
		ai, scheduledEventID, requestID, request.PollRequest.GetIdentity(),
		versioningStamp, pollerDeployment, request.GetBuildIdRedirectInfo(),
		request.PollRequest.GetWorkerControlTaskQueue(),
		clock,
	); err != nil {
		return nil, rejectCodeUndefined, err
	}

	scheduleToStartLatency := ai.GetStartedTime().AsTime().Sub(ai.GetScheduledTime().AsTime())
	config := shardContext.GetConfig()
	breakdownMetricsByBuildID := config.BreakdownMetricsByBuildID(
		namespaceName,
		ai.GetTaskQueue(),
		enumspb.TASK_QUEUE_TYPE_ACTIVITY,
	)
	metrics.TaskScheduleToStartLatency.With(
		metrics.GetPerTaskQueuePartitionTypeScope(
			taggedMetrics,
			namespaceName,
			// passing the root partition all the time as we don't care about partition ID in this metric
			tqid.UnsafeTaskQueueFamily(namespaceEntry.ID().String(),
				ai.GetTaskQueue()).TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(),
			config.BreakdownMetricsByTaskQueue(namespaceName,
				ai.GetTaskQueue(),
				enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		),
	).Record(
		scheduleToStartLatency,
		metrics.WorkerDeploymentNameTag(pollerDeployment.GetSeriesName(), breakdownMetricsByBuildID),
		metrics.WorkerDeploymentBuildIDTag(pollerDeployment.GetBuildId(), breakdownMetricsByBuildID),
	)

	response.Clock = clock
	populateStartResponse(response, ai, mutableState, namespaceName)
	return response, rejectCodeAccepted, nil
}

// checkActivityTransfer runs under the same workflow lease as the start.
func checkActivityTransfer(ms historyi.MutableState, ai *persistencespb.ActivityInfo, request *historyservice.RecordActivityTaskStartedRequest, currentVersion int64) (rejectCode, error) {
	transfer := request.ActivityTransferInfo
	if transfer.RequestId == "" || !transfer.Equal(ai.ActivityTransferInfo) || ai.Stamp != request.Stamp ||
		ai.Paused || ai.CancelRequested || ms.IsWorkflowExecutionStatusPaused() {
		return rejectCodeUndefined, serviceerrors.NewObsoleteMatchingTask("Activity dispatch changed")
	}
	if workflow.ActivityHasTimedOut(ai, ms.Now()) {
		return rejectCodeUndefined, serviceerrors.NewObsoleteMatchingTask("Activity dispatch expired")
	}
	if ms.GetCurrentVersion() != currentVersion {
		return rejectCodeUndefined, consts.ErrStaleState
	}
	if transfer.NamespaceFailoverVersion == currentVersion {
		return rejectCodeAccepted, nil
	}
	if ai.StartedEventId != common.EmptyEventID {
		return rejectCodeUndefined, serviceerrors.NewObsoleteMatchingTask("Activity started in a previous namespace version")
	}
	// The only queued copy can belong to the previous active cluster. Commit a
	// replacement before acknowledging it, even if new IDs are disabled.
	if err := ms.GenerateActivityTask(ai.ScheduledEventId); err != nil {
		return rejectCodeUndefined, err
	}
	return rejectCodeReplaced, nil
}

func checkRecordedStart(ms historyi.MutableState, ai *persistencespb.ActivityInfo, request *historyservice.RecordActivityTaskStartedRequest) error {
	poll := request.PollRequest
	if ai.StartedClock == nil || ai.StartedTime == nil || ai.StartVersion != ai.Version || ai.StartVersion != request.ActivityTransferInfo.NamespaceFailoverVersion ||
		ai.WorkerControlTaskQueue != poll.GetWorkerControlTaskQueue() || (ai.StartedIdentity != "" && ai.StartedIdentity != poll.GetIdentity()) {
		return serviceerrors.NewObsoleteMatchingTask("Activity start or worker binding changed")
	}
	if ms.GetDeploymentTransition() != nil {
		return serviceerrors.NewActivityStartDuringTransition()
	}
	if wfBuild := ai.GetUseWorkflowBuildIdInfo(); wfBuild.GetLastUsedBuildId() != "" &&
		(wfBuild.LastUsedBuildId != ms.GetAssignedBuildId() || wfBuild.LastRedirectCounter != ms.GetExecutionInfo().GetBuildIdRedirectCounter()) {
		return serviceerrors.NewObsoleteMatchingTask("Activity workflow build ID changed")
	}
	//nolint:staticcheck // Old workers send WorkerVersionCapabilities.
	deployment, err := worker_versioning.DeploymentFromCapabilities(poll.GetWorkerVersionCapabilities(), poll.GetDeploymentOptions())
	if err != nil {
		return err
	}
	if !ai.LastDeploymentVersion.Equal(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment)) {
		return serviceerrors.NewObsoleteMatchingTask("Activity worker deployment changed")
	}
	behavior := ms.GetEffectiveVersioningBehavior()
	if request.VersionDirective.GetBehavior() != behavior {
		return serviceerrors.NewObsoleteMatchingTask("Workflow versioning behavior changed")
	}
	return worker_versioning.ValidateTaskVersionDirective(request.VersionDirective, behavior, ms.GetEffectiveDeployment(), request.ScheduledDeployment)
}

func populateStartResponse(response *historyservice.RecordActivityTaskStartedResponse, ai *persistencespb.ActivityInfo, mutableState historyi.MutableState, namespaceName string) {
	response.StartedTime = ai.StartedTime
	response.Attempt = ai.Attempt
	response.HeartbeatDetails = ai.LastHeartbeatDetails
	response.Version = ai.Version
	response.StartVersion = ai.StartVersion

	response.WorkflowType = mutableState.GetWorkflowType()
	response.WorkflowNamespace = namespaceName
	response.RetryPolicy = &commonpb.RetryPolicy{
		InitialInterval:        ai.RetryInitialInterval,
		BackoffCoefficient:     ai.RetryBackoffCoefficient,
		MaximumInterval:        ai.RetryMaximumInterval,
		MaximumAttempts:        ai.RetryMaximumAttempts,
		NonRetryableErrorTypes: ai.RetryNonRetryableErrorTypes,
	}
}

// TODO (Shahab): move this method to a better place
func getDeploymentVersionAndRevisionNumberForWorkflowID(
	ctx context.Context,
	namespaceID string,
	taskQueueName string,
	taskQueueType enumspb.TaskQueueType,
	matchingClient matchingservice.MatchingServiceClient,
	routingInfoCache worker_versioning.RoutingInfoCache,
	workflowId string,
) (*deploymentspb.WorkerDeploymentVersion, int64, error) {
	// Check cache first for task queue routing info (independent of workflow ID)
	routingInfo, ok := routingInfoCache.Get(namespaceID, taskQueueName, taskQueueType)

	if !ok {
		// Cache miss, fetch from matching
		resp, err := matchingClient.GetTaskQueueUserData(ctx,
			&matchingservice.GetTaskQueueUserDataRequest{
				NamespaceId:   namespaceID,
				TaskQueue:     taskQueueName,
				TaskQueueType: taskQueueType,
			})
		if err != nil {
			return nil, 0, err
		}
		tqData, ok := resp.GetUserData().GetData().GetPerType()[int32(taskQueueType)]
		if ok {
			// Calculate the routing info for versioned task queue
			routingInfo.Current, routingInfo.CurrentRevisionNumber, _, routingInfo.Ramping, _, routingInfo.RampPercentage, routingInfo.RampingRevisionNumber, _ = worker_versioning.CalculateTaskQueueVersioningInfo(tqData.GetDeploymentData())
		}
		// else: routingInfo fields are all zero/nil (unversioned)

		// Store routing info in cache (without workflow ID) - even for unversioned task queues
		routingInfoCache.Put(namespaceID, taskQueueName, taskQueueType, routingInfo.Current, routingInfo.CurrentRevisionNumber, routingInfo.Ramping, routingInfo.RampPercentage, routingInfo.RampingRevisionNumber)
	}

	// Apply workflow-specific routing logic
	// Note: Passing false useRampingVersionForInitialTask because activity is never the initial task.
	targetDeploymentVersion, targetDeploymentRevisionNumber := worker_versioning.FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
		routingInfo.Current,
		routingInfo.CurrentRevisionNumber,
		routingInfo.Ramping,
		routingInfo.RampPercentage,
		routingInfo.RampingRevisionNumber,
		workflowId,
		false,
	)

	return targetDeploymentVersion, targetDeploymentRevisionNumber, nil
}

func processActivityWorkflowRules(
	shardContext historyi.ShardContext,
	request *historyservice.RecordActivityTaskStartedRequest,
	ms historyi.MutableState,
	ai *persistencespb.ActivityInfo,
) (rejectCode, error) {
	if ai.Stamp == request.Stamp && ai.Paused {
		// this shouldn't happen. For now log an error
		shardContext.GetLogger().Error(
			fmt.Sprintf(
				"Activity is paused, but new task was schedulled. Activity ID: %v, Stamp: %v, event ID: %v",
				ai.ActivityId, request.Stamp, ai.StartedEventId),
		)
	}

	// if activity is already paused or Stamp is not the same as the one in the request we shouldn't process workflow rules
	// this is a no-op
	if ai.Stamp != request.Stamp || ai.Paused {
		return rejectCodeUndefined, nil
	}

	// This is an attempt to reduce the number of workflow rules calls.
	// We only need to process the first invocation of an activity.
	// Other invocations should be blocked by either RetryActivity or retryTask.
	if ai.Attempt > 1 {
		return rejectCodeUndefined, nil
	}

	ruleMatched := workflow.ActivityMatchWorkflowRules(ms, shardContext.GetTimeSource(), shardContext.GetLogger(), ai)
	if !ruleMatched || !ai.Paused {
		return rejectCodeUndefined, nil
	}

	// activity was paused, need to update activity
	if err := ms.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		workflow.ClearActivityStartedState(activityInfo)
		return nil
	}); err != nil {
		return rejectCodeUndefined, err
	}

	// if activity was paused we will need to update mutable state
	return rejectCodePaused, nil
}
