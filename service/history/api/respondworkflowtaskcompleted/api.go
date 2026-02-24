package respondworkflowtaskcompleted

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/recordworkflowtaskstarted"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	WorkflowTaskCompletedHandler struct {
		config                         *configs.Config
		shardContext                   historyi.ShardContext
		workflowConsistencyChecker     api.WorkflowConsistencyChecker
		timeSource                     clock.TimeSource
		namespaceRegistry              namespace.Registry
		eventNotifier                  events.Notifier
		tokenSerializer                *tasktoken.Serializer
		metricsHandler                 metrics.Handler
		logger                         log.Logger
		throttledLogger                log.Logger
		commandAttrValidator           *api.CommandAttrValidator
		searchAttributesMapperProvider searchattribute.MapperProvider
		searchAttributesValidator      *searchattribute.Validator
		persistenceVisibilityMgr       manager.VisibilityManager
		commandHandlerRegistry         *workflow.CommandHandlerRegistry
		matchingClient                 matchingservice.MatchingServiceClient
		versionMembershipCache         worker_versioning.VersionMembershipCache
	}
)

func NewWorkflowTaskCompletedHandler(
	shardContext historyi.ShardContext,
	tokenSerializer *tasktoken.Serializer,
	eventNotifier events.Notifier,
	commandHandlerRegistry *workflow.CommandHandlerRegistry,
	searchAttributesValidator *searchattribute.Validator,
	visibilityManager manager.VisibilityManager,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	versionMembershipCache worker_versioning.VersionMembershipCache,
) *WorkflowTaskCompletedHandler {
	return &WorkflowTaskCompletedHandler{
		config:                     shardContext.GetConfig(),
		shardContext:               shardContext,
		workflowConsistencyChecker: workflowConsistencyChecker,
		timeSource:                 shardContext.GetTimeSource(),
		namespaceRegistry:          shardContext.GetNamespaceRegistry(),
		eventNotifier:              eventNotifier,
		tokenSerializer:            tokenSerializer,
		metricsHandler:             shardContext.GetMetricsHandler(),
		logger:                     shardContext.GetLogger(),
		throttledLogger:            shardContext.GetThrottledLogger(),
		commandAttrValidator: api.NewCommandAttrValidator(
			shardContext.GetNamespaceRegistry(),
			shardContext.GetConfig(),
			searchAttributesValidator,
		),
		searchAttributesMapperProvider: shardContext.GetSearchAttributesMapperProvider(),
		searchAttributesValidator:      searchAttributesValidator,
		persistenceVisibilityMgr:       visibilityManager,
		commandHandlerRegistry:         commandHandlerRegistry,
		matchingClient:                 matchingClient,
		versionMembershipCache:         versionMembershipCache,
	}
}

//nolint:revive // cyclomatic complexity
func (handler *WorkflowTaskCompletedHandler) Invoke(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (_ *historyservice.RespondWorkflowTaskCompletedResponse, retError error) {
	// By default, retError is passed to workflow lease release method in deferred function.
	// If error is passed, then workflow context and mutable state are cleared.
	// If no changes to mutable state are made or changes already persisted (in memory version corresponds to the database),
	// then the lease is released without an error, i.e. workflow context and mutable state are NOT cleared.
	releaseLeaseWithError := true

	request := req.CompleteRequest
	token, err0 := handler.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, consts.ErrDeserializingToken
	}

	namespaceEntry, err := api.GetActiveNamespace(handler.shardContext, namespace.ID(req.GetNamespaceId()), token.WorkflowId)
	if err != nil {
		return nil, err
	}

	workflowLease, err := handler.workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		token.Clock,
		func(mutableState historyi.MutableState) bool {
			workflowTask := mutableState.GetWorkflowTaskByID(token.GetScheduledEventId())
			if workflowTask == nil && token.GetScheduledEventId() >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(handler.metricsHandler).Record(
					1,
					metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
				return false
			}
			return true
		},
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			token.WorkflowId,
			token.RunId,
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	weContext := workflowLease.GetContext()
	ms := workflowLease.GetMutableState()
	currentWorkflowTask := ms.GetWorkflowTaskByID(token.GetScheduledEventId())

	if len(request.Commands) == 0 {
		// Context metadata is automatically set during mutable state transaction close. For RespondWorkflowTaskCompleted
		// with no commands (e.g., workflow task heartbeat or only readonly messages like `update.Rejection`), the transaction
		// is never closed. We explicitly call SetContextMetadata here to ensure workflow metadata is populated in the context.
		ms.SetContextMetadata(ctx)
	}

	defer func() {
		var errForRelease error
		if releaseLeaseWithError {
			// If the workflow context needs to be cleared, operation error passed to Release func (default).
			// Otherwise, leave it nil here to avoid clearing the workflow context (but still return error to the caller).
			errForRelease = retError
		}
		if retError != nil && currentWorkflowTask != nil && currentWorkflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && ms.IsStickyTaskQueueSet() {
			// If, while completing WFT, error is occurred and returned to the worker then worker will clear its cache.
			// New WFT will also be created on sticky task queue and sent to worker.
			// Because worker doesn't have a workflow in cache, it needs to replay it from the beginning,
			// but sticky WFT has only partial history. Worker will request full history using GetWorkflowExecutionHistory API.
			// This history doesn't have speculative WFT events. If WFT is speculative,
			// then worker will see inconsistency between history from it and full history, and will fail WFT.
			//
			// To prevent unexpected WFT failure, server clears stickiness for this workflow, next WFT will go to normal task queue,
			// and will have full history attached to it.
			// This is NOT 100% bulletproof solution because this write operation may also fail.
			// TODO: remove this call when GetWorkflowExecutionHistory includes speculative WFT events.
			if clearStickyErr := handler.clearStickyTaskQueue(ctx, workflowLease.GetContext()); clearStickyErr != nil {
				handler.logger.Error("Failed to clear stickiness after speculative workflow task failed to complete.",
					tag.NewErrorTag("clear-sticky-error", clearStickyErr),
					tag.Error(retError),
					tag.WorkflowID(token.GetWorkflowId()),
					tag.WorkflowRunID(token.GetRunId()),
					tag.WorkflowNamespaceID(namespaceEntry.ID().String()))

				// Workflow context is already cleared and need to be cleared one more time if clearStickyTaskQueue failed.
				// Use clearStickyErr for that.
				errForRelease = clearStickyErr
			}
		}

		workflowLease.GetReleaseFn()(errForRelease)
	}()

	if !ms.IsWorkflowExecutionRunning() ||
		currentWorkflowTask == nil ||
		currentWorkflowTask.StartedEventID == common.EmptyEventID ||
		(token.StartedEventId != common.EmptyEventID && token.StartedEventId != currentWorkflowTask.StartedEventID) ||
		(token.StartedTime != nil && !currentWorkflowTask.StartedTime.IsZero() && !token.StartedTime.AsTime().Equal(currentWorkflowTask.StartedTime)) ||
		currentWorkflowTask.Attempt != token.Attempt ||
		(token.Version != common.EmptyVersion && token.Version != currentWorkflowTask.Version) {
		// Mutable state wasn't changed yet and doesn't have to be cleared.
		releaseLeaseWithError = false
		return nil, serviceerror.NewNotFound("Workflow task not found.")
	}

	// We don't accept the request to create a new workflow task if the workflow is paused.
	if ms.IsWorkflowExecutionStatusPaused() && request.GetForceCreateNewWorkflowTask() {
		// Mutable state wasn't changed yet and doesn't have to be cleared.
		releaseLeaseWithError = false
		return nil, serviceerror.NewFailedPrecondition("Workflow is paused and force create new workflow task is not allowed.")
	}

	behavior := request.GetVersioningBehavior()
	deployment := worker_versioning.DeploymentFromDeploymentVersion(worker_versioning.DeploymentVersionFromOptions(request.GetDeploymentOptions()))
	//nolint:staticcheck // SA1019 deprecated Deployment will clean up later
	if behavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED && request.GetDeployment() == nil &&
		(request.GetDeploymentOptions() == nil || request.GetDeploymentOptions().GetWorkerVersioningMode() != enumspb.WORKER_VERSIONING_MODE_VERSIONED) {
		// Mutable state wasn't changed yet and doesn't have to be cleared.
		releaseLeaseWithError = false
		return nil, serviceerror.NewInvalidArgument("versioning behavior cannot be specified without deployment options being set with versioned mode")
	}

	assignedBuildId := ms.GetAssignedBuildId()
	wftCompletedBuildId := request.GetWorkerVersionStamp().GetBuildId()
	if assignedBuildId != "" && !ms.IsStickyTaskQueueSet() {
		// Worker versioning is used, make sure the task was completed by the right build ID, unless we're using a
		// sticky queue in which case Matching will not send the build ID until old versioning is cleaned up
		// TODO: remove !ms.IsStickyTaskQueueSet() from above condition after old WV cleanup [cleanup-old-wv]
		wftStartedBuildId := ms.GetExecutionInfo().GetWorkflowTaskBuildId()
		if wftCompletedBuildId != wftStartedBuildId {
			// Mutable state wasn't changed yet and doesn't have to be cleared.
			releaseLeaseWithError = false
			return nil, serviceerror.NewNotFoundf("this workflow task was dispatched to Build ID %s, not %s", wftStartedBuildId, wftCompletedBuildId)
		}
	}

	var effects effect.Buffer
	defer func() {
		// `effects` are canceled immediately on WFT failure or persistence errors.
		// This `defer` handles rare cases where an error is returned but the cancellation didn't happen.
		if retError != nil {
			cancelled := effects.Cancel(ctx)
			if cancelled {
				handler.logger.Info("Canceled effects due to error",
					tag.Error(retError),
					tag.WorkflowID(token.GetWorkflowId()),
					tag.WorkflowRunID(token.GetRunId()),
					tag.WorkflowNamespaceID(namespaceEntry.ID().String()))
			}
		}
	}()

	// TODO(carlydf): change condition when deprecating versionstamp
	// It's an error if the workflow has used versioning in the past but this task has no versioning info.
	if ms.GetMostRecentWorkerVersionStamp().GetUseVersioning() &&
		//nolint:staticcheck // SA1019 deprecated stamp will clean up later
		!request.GetWorkerVersionStamp().GetUseVersioning() &&
		request.GetDeploymentOptions().GetWorkerVersioningMode() != enumspb.WORKER_VERSIONING_MODE_VERSIONED &&
		// This check is not needed for V3 versioning
		ms.GetEffectiveVersioningBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		// Mutable state wasn't changed yet and doesn't have to be cleared.
		releaseLeaseWithError = false
		return nil, serviceerror.NewInvalidArgument("Workflow using versioning must continue to use versioning.")
	}

	nsName := namespaceEntry.Name().String()
	limits := historyi.WorkflowTaskCompletionLimits{
		MaxResetPoints:              handler.config.MaxAutoResetPoints(nsName),
		MaxSearchAttributeValueSize: handler.config.SearchAttributesSizeOfValueLimit(nsName),
	}
	// TODO: this metric is inaccurate, it should only be emitted if a new binary checksum (or build ID) is added in this completion.
	if ms.GetExecutionInfo().AutoResetPoints != nil && limits.MaxResetPoints == len(ms.GetExecutionInfo().AutoResetPoints.Points) {
		metrics.AutoResetPointsLimitExceededCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
	}

	var wtHeartbeatTimedOut bool
	var completedEvent *historypb.HistoryEvent

	// SDKs set ForceCreateNewWorkflowTask flag to true when they are doing WT heartbeats.
	// In a mean time, there might be pending commands and messages on the worker side.
	// If those commands/messages are sent on the heartbeat WT it means that WF is making progress.
	// WT heartbeat timeout is applicable only when WF doesn't make any progress and does heartbeats only.
	checkWTHeartbeatTimeout := request.GetForceCreateNewWorkflowTask() && len(request.Commands) == 0 && len(request.Messages) == 0

	if checkWTHeartbeatTimeout {
		// WorkflowTaskHeartbeatTimeout is a total duration for which workflow is allowed to send continuous heartbeats.
		// Default is 30 minutes.
		// Heartbeat duration is computed between now and OriginalScheduledTime, which is set when WT is scheduled and
		// carried over to the consequence WTs if they are heartbeat WTs.
		// After this timeout is expired, WT is timed out (although this specific WT doesn't)
		// and new WT will be scheduled on non-sticky task queue (see ClearStickyTaskQueue call bellow).
		wtHeartbeatTimeoutDuration := handler.config.WorkflowTaskHeartbeatTimeout(nsName)
		if currentWorkflowTask.OriginalScheduledTime.UnixNano() > 0 &&
			handler.timeSource.Now().After(currentWorkflowTask.OriginalScheduledTime.Add(wtHeartbeatTimeoutDuration)) {

			scope := handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(nsName),
			)
			metrics.WorkflowTaskHeartbeatTimeoutCounter.With(scope).Record(1)
			completedEvent, err = ms.AddWorkflowTaskTimedOutEvent(currentWorkflowTask)
			if err != nil {
				return nil, err
			}
			ms.ClearStickyTaskQueue()
			wtHeartbeatTimedOut = true
		}
	}
	// WT wasn't timed out (due to too many heartbeats), therefore WTCompleted event should be created.
	if !wtHeartbeatTimedOut {
		completedEvent, err = ms.AddWorkflowTaskCompletedEvent(currentWorkflowTask, request, limits)
		if err != nil {
			return nil, err
		}
	}
	// NOTE: completedEvent might be nil if WT was speculative and request has only `update.Rejection` messages.
	// See workflowTaskStateMachine.skipWorkflowTaskCompletedEvent for more details.

	if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskQueue == nil {
		metrics.CompleteWorkflowTaskWithStickyDisabledCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		ms.ClearStickyTaskQueue()
	} else {
		metrics.CompleteWorkflowTaskWithStickyEnabledCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		if (assignedBuildId == "" || assignedBuildId == wftCompletedBuildId) &&
			(ms.GetDeploymentTransition() == nil || ms.GetDeploymentTransition().GetDeployment().Equal(deployment)) {
			// TODO: clean up. this is not applicable to V3
			// For versioned workflows, only set sticky queue if the WFT is completed by the WF's current build ID.
			// It is possible that the WF has been redirected to another build ID since this WFT started, in that case
			// we should not set sticky queue of the old build ID and keep the normal queue to let Matching send the
			// next WFT to the right build ID.
			ms.SetStickyTaskQueue(request.StickyAttributes.WorkerTaskQueue.GetName(), request.StickyAttributes.GetScheduleToStartTimeout())
		}
	}

	var (
		wtFailedCause               *workflowTaskFailedCause
		activityNotStartedCancelled bool
		newMutableState             historyi.MutableState
		responseMutations           []workflowTaskResponseMutation
	)
	updateRegistry := weContext.UpdateRegistry(ctx)
	// hasBufferedEventsOrMessages indicates if there are any buffered events
	// or admitted updates which should generate a new workflow task.

	// TODO: HasOutgoingMessages call (=check for admitted updates) is comment out
	//   because non-durable admitted updates can't block WF from closing,
	//   because everytime WFT is failing, WF context is cleared together with update registry
	//   and admitted updates are lost. Uncomment this check when durable admitted is implemented
	//   or updates stay in the registry after WFT is failed.
	hasBufferedEventsOrMessages := ms.HasBufferedEvents() // || updateRegistry.HasOutgoingMessages(false)
	if err := namespaceEntry.VerifyBinaryChecksum(request.GetBinaryChecksum()); err != nil {
		wtFailedCause = newWorkflowTaskFailedCause(
			enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_BINARY,
			serviceerror.NewInvalidArgumentf(
				"binary %v is marked as bad deployment",
				//nolint:staticcheck // SA1019 deprecated stamp will clean up later
				request.GetBinaryChecksum()),
			false)
	} else {
		namespace := namespaceEntry.Name()
		workflowSizeChecker := newWorkflowSizeChecker(
			workflowSizeLimits{
				blobSizeLimitWarn:              handler.config.BlobSizeLimitWarn(namespace.String()),
				blobSizeLimitError:             handler.config.BlobSizeLimitError(namespace.String()),
				memoSizeLimitWarn:              handler.config.MemoSizeLimitWarn(namespace.String()),
				memoSizeLimitError:             handler.config.MemoSizeLimitError(namespace.String()),
				numPendingChildExecutionsLimit: handler.config.NumPendingChildExecutionsLimit(namespace.String()),
				numPendingActivitiesLimit:      handler.config.NumPendingActivitiesLimit(namespace.String()),
				numPendingSignalsLimit:         handler.config.NumPendingSignalsLimit(namespace.String()),
				numPendingCancelsRequestLimit:  handler.config.NumPendingCancelsRequestLimit(namespace.String()),
			},
			ms,
			handler.searchAttributesValidator,
			handler.metricsHandler.WithTags(
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
				metrics.NamespaceTag(namespace.String()),
			),
			handler.throttledLogger,
		)

		workflowTaskHandler := newWorkflowTaskCompletedHandler(
			request.GetIdentity(),
			completedEvent.GetEventId(), // If completedEvent is nil, then GetEventId() returns 0 and this value shouldn't be used in workflowTaskHandler.
			ms,
			updateRegistry,
			&effects,
			handler.commandAttrValidator,
			workflowSizeChecker,
			handler.logger,
			handler.namespaceRegistry,
			handler.metricsHandler,
			handler.config,
			handler.shardContext,
			handler.searchAttributesMapperProvider,
			hasBufferedEventsOrMessages,
			handler.commandHandlerRegistry,
			handler.matchingClient,
			handler.versionMembershipCache,
		)

		if responseMutations, err = workflowTaskHandler.handleCommands(
			ctx,
			request.Commands,
			collection.NewIndexedTakeList(
				request.Messages,
				func(msg *protocolpb.Message) string { return msg.Id },
			),
		); err != nil {
			return nil, err
		}

		// Worker must respond with Update Accepted or Update Rejected message on every Update Requested
		// message that were delivered on specific WT, when completing this WT.
		// If worker ignored the update request (old SDK or SDK bug), then server rejects this update.
		// Otherwise, this update will be delivered (and new WT created) again and again.
		workflowTaskHandler.rejectUnprocessedUpdates(
			ctx,
			currentWorkflowTask.ScheduledEventID,
			request.GetForceCreateNewWorkflowTask(),
			weContext.GetWorkflowKey(),
			request.GetIdentity(),
		)

		// If the Workflow completed itself, but there are still accepted
		// (but not completed) Updates, they need to be aborted.
		// Reason is always "WorkflowCompleted" because accepted Updates
		// are not moving to continuing runs (if any).
		if !ms.IsWorkflowExecutionRunning() {
			updateRegistry.AbortAccepted(update.AbortReasonWorkflowCompleted, &effects)
		}

		// set the vars used by following logic
		// further refactor should also clean up the vars used below
		wtFailedCause = workflowTaskHandler.workflowTaskFailedCause

		// failMessage is not used by workflowTaskHandlerCallbacks
		activityNotStartedCancelled = workflowTaskHandler.activityNotStartedCancelled
		// continueAsNewTimerTasks is not used by workflowTaskHandlerCallbacks

		newMutableState = workflowTaskHandler.newMutableState

		hasBufferedEventsOrMessages = workflowTaskHandler.hasBufferedEventsOrMessages
	}

	wtFailedShouldCreateNewTask := false
	if wtFailedCause != nil {
		effects.Cancel(ctx)

		// Abort all Updates with explicit reason, to prevent them to be aborted with generic
		// registryClearedErr, which may lead to continuous retries of UpdateWorkflowExecution API.
		updateRegistry.Abort(update.AbortReasonWorkflowTaskFailed)

		metrics.FailedWorkflowTasksCounter.With(handler.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
			metrics.NamespaceTag(namespaceEntry.Name().String()),
			metrics.VersioningBehaviorTag(ms.GetEffectiveVersioningBehavior()),
			metrics.FailureTag(wtFailedCause.failedCause.String()),
			metrics.FirstAttemptTag(currentWorkflowTask.Attempt),
		)
		handler.logger.Info("Failing the workflow task.",
			tag.Value(wtFailedCause.Message()),
			tag.WorkflowID(token.GetWorkflowId()),
			tag.WorkflowRunID(token.GetRunId()),
			tag.WorkflowNamespaceID(namespaceEntry.ID().String()),
			tag.Attempt(currentWorkflowTask.Attempt),
			tag.Cause(wtFailedCause.failedCause.String()),
		)
		if currentWorkflowTask.Attempt > 1 && wtFailedCause.failedCause != enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			// drop this workflow task if it keeps failing. This will cause the workflow task to timeout and get retried after timeout.
			return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
		}

		// wtFailedEventID must be used as the event batch ID for any following workflow termination events
		var wtFailedEventID int64
		ms, wtFailedEventID, err = failWorkflowTask(ctx, handler.shardContext, weContext, currentWorkflowTask, wtFailedCause, request)
		if err != nil {
			return nil, err
		}
		wtFailedShouldCreateNewTask = true
		newMutableState = nil

		if wtFailedCause.terminateWorkflow {
			// Flush buffer event before terminating the workflow
			ms.FlushBufferedEvents()

			_, err := ms.AddWorkflowExecutionTerminatedEvent(
				wtFailedEventID,
				wtFailedCause.Message(),
				nil,
				consts.IdentityHistoryService,
				false,
				nil, // No links necessary.
			)
			if err != nil {
				return nil, err
			}

			wtFailedShouldCreateNewTask = false
		}
	}

	newWorkflowTaskType := enumsspb.WORKFLOW_TASK_TYPE_UNSPECIFIED
	if ms.IsWorkflowExecutionRunning() {
		if request.GetForceCreateNewWorkflowTask() || // Heartbeat WT is always of Normal type.
			wtFailedShouldCreateNewTask ||
			hasBufferedEventsOrMessages ||
			activityNotStartedCancelled ||
			// If the workflow has an ongoing transition to another deployment version, we should ensure
			// it has a pending wft so it does not remain in the transition phase for long.
			ms.GetDeploymentTransition() != nil {

			newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL

		} else if updateRegistry.HasOutgoingMessages(true) {
			// There shouldn't be any sent updates in the registry because
			// all sent but not processed updates were rejected by server.
			// Therefore, it doesn't matter if to includeAlreadySent or not.

			newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
		}
	}

	bypassTaskGeneration := request.GetReturnNewWorkflowTask() && wtFailedCause == nil
	// TODO (alex-update): All current SDKs always set ReturnNewWorkflowTask to true
	//  which means that server always bypass task generation if WFT didn't fail.
	//  ReturnNewWorkflowTask flag needs to be removed.

	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && !bypassTaskGeneration {
		// If task generation can't be bypassed (i.e. WFT has failed),
		// WFT must be created as Normal because speculative WFT by nature skips task generation.
		newWorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
	}

	var newWorkflowTask *historyi.WorkflowTaskInfo

	// Speculative workflow task will be created after mutable state is persisted.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_NORMAL {
		versioningStamp := request.WorkerVersionStamp
		if versioningStamp.GetUseVersioning() {
			if ms.GetAssignedBuildId() == "" {
				// old versioning is used. making sure the versioning stamp does not go through otherwise the
				// workflow will start using new versioning which may surprise users.
				// TODO: remove this block when deleting old wv [cleanup-old-wv]
				versioningStamp = nil
			} else {
				// new versioning is used. do not return new wft to worker if stamp build ID does not match wf build ID
				// let the task go through matching and get dispatched to the right worker
				if versioningStamp.GetBuildId() != ms.GetAssignedBuildId() {
					bypassTaskGeneration = false
				}
			}
		}

		if ms.GetDeploymentTransition() != nil {
			// Do not return new wft to worker if the workflow is transitioning to a different deployment version.
			// Let the task go through matching and get dispatched to the right worker
			bypassTaskGeneration = false
		}

		var newWTErr error
		// If we checked WT heartbeat timeout before and WT wasn't timed out,
		// then OriginalScheduledTime needs to be carried over to the new WT.
		if checkWTHeartbeatTimeout && !wtHeartbeatTimedOut {
			newWorkflowTask, newWTErr = ms.AddWorkflowTaskScheduledEventAsHeartbeat(
				bypassTaskGeneration,
				timestamppb.New(currentWorkflowTask.OriginalScheduledTime),
				enumsspb.WORKFLOW_TASK_TYPE_NORMAL, // Heartbeat workflow task is always of Normal type.
			)
		} else {
			newWorkflowTask, newWTErr = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, newWorkflowTaskType)
		}
		if newWTErr != nil {
			return nil, newWTErr
		}

		// skip transfer task for workflow task if request asking to return new workflow task
		if bypassTaskGeneration {
			// start the new workflow task if request asked to do so
			// TODO: replace the poll request
			_, newWorkflowTask, err = ms.AddWorkflowTaskStartedEvent(
				newWorkflowTask.ScheduledEventID,
				"request-from-RespondWorkflowTaskCompleted",
				newWorkflowTask.TaskQueue,
				request.Identity,
				versioningStamp,
				nil,
				workflowLease.GetContext().UpdateRegistry(ctx),
				false,
				nil,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	var updateErr error
	if newMutableState != nil {
		newWorkflowExecutionInfo := newMutableState.GetExecutionInfo()
		newWorkflowExecutionState := newMutableState.GetExecutionState()
		updateErr = weContext.UpdateWorkflowExecutionWithNewAsActive(
			ctx,
			handler.shardContext,
			workflow.NewContext(
				handler.shardContext.GetConfig(),
				definition.NewWorkflowKey(
					newWorkflowExecutionInfo.NamespaceId,
					newWorkflowExecutionInfo.WorkflowId,
					newWorkflowExecutionState.RunId,
				),
				chasm.WorkflowArchetypeID,
				handler.logger,
				handler.shardContext.GetThrottledLogger(),
				handler.shardContext.GetMetricsHandler(),
			),
			newMutableState,
		)
	} else {
		// If completedEvent is not nil (which means that this WT wasn't speculative)
		// OR new WT is normal, then mutable state needs to be persisted.
		// Otherwise, (both current and new WT are speculative) mutable state is updated in memory only but not persisted.
		if completedEvent != nil || newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_NORMAL {
			updateErr = weContext.UpdateWorkflowExecutionAsActive(ctx, handler.shardContext)
		}
	}

	if updateErr != nil {
		effects.Cancel(ctx)
		if persistence.IsConflictErr(updateErr) {
			metrics.ConcurrencyUpdateFailureCounter.With(handler.metricsHandler).Record(
				1,
				metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope))
		}

		// if updateErr resulted in TransactionSizeLimitError then fail workflow
		switch updateErr.(type) {
		case *persistence.TransactionSizeLimitError:
			// must reload mutable state because the first call to updateWorkflowExecutionWithContext or continueAsNewWorkflowExecution
			// clears mutable state if error is returned
			ms, err = weContext.LoadMutableState(ctx, handler.shardContext)
			if err != nil {
				return nil, err
			}

			if err := workflow.TerminateWorkflow(
				ms,
				common.FailureReasonTransactionSizeExceedsLimit,
				payloads.EncodeString(updateErr.Error()),
				consts.IdentityHistoryService,
				false,
				nil, // no links necessary.
			); err != nil {
				return nil, err
			}
			if err := weContext.UpdateWorkflowExecutionAsActive(
				ctx,
				handler.shardContext,
			); err != nil {
				return nil, err
			}
			updateRegistry.Abort(update.AbortReasonWorkflowCompleted)
		}

		return nil, updateErr
	}

	// If mutable state was persisted successfully (or persistence was skipped),
	// then effects needs to be applied immediately to keep registry and mutable state in sync.
	effects.Apply(ctx)

	if !ms.IsWorkflowExecutionRunning() {
		// NOTE: It is important to call this *after* applying effects to be sure there are no
		// Updates in ProvisionallyCompleted state.

		// Because:
		//  (1) all unprocessed Updates were already rejected
		//  (2) all accepted Updates were aborted
		// the registry only has: Updates received while this WFT was running (new Updates).
		hasNewRun := newMutableState != nil
		if hasNewRun {
			// If a new run was created (e.g. ContinueAsNew, Retry, Cron), then Updates that were
			// received while this WFT was running are aborted with a retryable error.
			// Then, the SDK will retry the API call and the Update will land on the new run.
			updateRegistry.Abort(update.AbortReasonWorkflowContinuing)
		} else {
			// If the Workflow completed itself via one of the completion commands without
			// creating a new run, abort all Updates with a non-retryable error.
			updateRegistry.Abort(update.AbortReasonWorkflowCompleted)
		}
	}

	// Create speculative workflow task after mutable state is persisted.
	if newWorkflowTaskType == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		newWorkflowTask, err = ms.AddWorkflowTaskScheduledEvent(bypassTaskGeneration, newWorkflowTaskType)
		if err != nil {
			return nil, err
		}
		versioningStamp := request.WorkerVersionStamp
		if versioningStamp.GetUseVersioning() && ms.GetAssignedBuildId() == "" {
			// old versioning is used. making sure the versioning stamp does not go through otherwise the
			// workflow will start using new versioning which may surprise users.
			// TODO: remove this block when deleting old wv [cleanup-old-wv]
			versioningStamp = nil
		}
		_, newWorkflowTask, err = ms.AddWorkflowTaskStartedEvent(
			newWorkflowTask.ScheduledEventID,
			"request-from-RespondWorkflowTaskCompleted",
			newWorkflowTask.TaskQueue,
			request.Identity,
			versioningStamp,
			nil,
			workflowLease.GetContext().UpdateRegistry(ctx),
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	handler.handleBufferedQueries(ms, req.GetCompleteRequest().GetQueryResults(), newWorkflowTask != nil, namespaceEntry)

	if wtHeartbeatTimedOut {
		// Mutable state was already persisted and doesn't need to be cleared although error is returned to the worker.
		releaseLeaseWithError = false
		return nil, serviceerror.NewNotFound("workflow task heartbeat timeout")
	}

	if wtFailedCause != nil {
		// Mutable state was already persisted and doesn't need to be cleared although error is returned to the worker.
		releaseLeaseWithError = false
		return nil, serviceerror.NewInvalidArgument(wtFailedCause.Message())
	}

	resp := &historyservice.RespondWorkflowTaskCompletedResponse{}
	//nolint:staticcheck
	if newWorkflowTask != nil && bypassTaskGeneration {
		resp.StartedResponse, err = recordworkflowtaskstarted.CreateRecordWorkflowTaskStartedResponse(
			ctx,
			ms,
			updateRegistry,
			newWorkflowTask,
			request.GetIdentity(),
			request.GetForceCreateNewWorkflowTask(),
		)
		if err != nil {
			return nil, err
		}
		// sticky is always enabled when worker request for new workflow task from RespondWorkflowTaskCompleted
		resp.StartedResponse.StickyExecutionEnabled = true

		resp.NewWorkflowTask, err = handler.withNewWorkflowTask(ctx, namespaceEntry.Name(), req, resp.StartedResponse)
		if err != nil {
			return nil, err
		}
	}

	// If completedEvent is nil then it means that WT was speculative and
	// WT events (scheduled/started/completed) were not written to the history and were dropped.
	// SDK needs to know where to roll back its history event pointer, i.e. after what event all other events needs to be dropped.
	// SDK uses WorkflowTaskStartedEventID to do that.
	if completedEvent == nil {
		resp.ResetHistoryEventId = ms.GetExecutionInfo().LastCompletedWorkflowTaskStartedEventId
	}

	for _, mutation := range responseMutations {
		if err := mutation(resp); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func (handler *WorkflowTaskCompletedHandler) createPollWorkflowTaskQueueResponse(
	ctx context.Context,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	matchingResp *matchingservice.PollWorkflowTaskQueueResponseWithRawHistory,
	branchToken []byte,
	maximumPageSize int32,
) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no workflow task to be send to worker / caller
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	var history *historypb.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &historypb.History{
			Events: []*historypb.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}

		// TODO below is a temporal solution to guard against invalid event batch
		//  when data inconsistency occurs
		//  long term solution should check event batch pointing backwards within history store
		defer func() {
			var dataLossErr *serviceerror.DataLoss
			if errors.As(retError, &dataLossErr) {
				api.TrimHistoryNode(
					ctx,
					handler.shardContext,
					handler.workflowConsistencyChecker,
					handler.eventNotifier,
					namespaceID.String(),
					matchingResp.WorkflowExecution.GetWorkflowId(),
					matchingResp.WorkflowExecution.GetRunId(),
				)
			}
		}()
		history, persistenceToken, err = api.GetHistory(
			ctx,
			handler.shardContext,
			namespaceName,
			namespaceID,
			matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			maximumPageSize,
			nil,
			matchingResp.GetTransientWorkflowTask(),
			branchToken,
			handler.persistenceVisibilityMgr,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
				RunId:            matchingResp.WorkflowExecution.GetRunId(),
				FirstEventId:     firstEventID,
				NextEventId:      nextEventID,
				PersistenceToken: persistenceToken,
				BranchToken:      branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  matchingResp.TaskToken,
		WorkflowExecution:          matchingResp.WorkflowExecution,
		WorkflowType:               matchingResp.WorkflowType,
		PreviousStartedEventId:     matchingResp.PreviousStartedEventId,
		StartedEventId:             matchingResp.StartedEventId,
		Query:                      matchingResp.Query,
		BacklogCountHint:           matchingResp.BacklogCountHint,
		Attempt:                    matchingResp.Attempt,
		History:                    history,
		NextPageToken:              continuation,
		WorkflowExecutionTaskQueue: matchingResp.WorkflowExecutionTaskQueue,
		ScheduledTime:              matchingResp.ScheduledTime,
		StartedTime:                matchingResp.StartedTime,
		Queries:                    matchingResp.Queries,
		Messages:                   matchingResp.Messages,
	}

	return resp, nil
}

func (handler *WorkflowTaskCompletedHandler) withNewWorkflowTask(
	ctx context.Context,
	namespaceName namespace.Name,
	request *historyservice.RespondWorkflowTaskCompletedRequest,
	response *historyservice.RecordWorkflowTaskStartedResponse,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	taskToken, err := handler.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, consts.ErrDeserializingToken
	}

	taskToken = tasktoken.NewWorkflowTaskToken(
		taskToken.GetNamespaceId(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		response.GetScheduledEventId(),
		response.GetStartedEventId(),
		response.GetStartedTime(),
		response.GetAttempt(),
		response.GetClock(),
		response.GetVersion(),
	)
	token, err := handler.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: taskToken.GetWorkflowId(),
		RunId:      taskToken.GetRunId(),
	}
	matchingResp := common.CreateMatchingPollWorkflowTaskQueueResponse(response, workflowExecution, token)
	return handler.createPollWorkflowTaskQueueResponse(
		ctx,
		namespaceName,
		namespace.ID(taskToken.NamespaceId),
		matchingResp,
		matchingResp.GetBranchToken(),
		int32(handler.config.HistoryMaxPageSize(namespaceName.String())),
	)
}

func (handler *WorkflowTaskCompletedHandler) handleBufferedQueries(
	ms historyi.MutableState,
	queryResults map[string]*querypb.WorkflowQueryResult,
	createNewWorkflowTask bool,
	namespaceEntry *namespace.Namespace,
) {
	queryRegistry := ms.GetQueryRegistry()
	if !queryRegistry.HasBufferedQuery() {
		return
	}

	namespaceName := namespaceEntry.Name()
	workflowID := ms.GetExecutionInfo().WorkflowId
	runID := ms.GetExecutionState().GetRunId()

	scope := handler.metricsHandler.WithTags(
		metrics.OperationTag(metrics.HistoryRespondWorkflowTaskCompletedScope),
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.CommandTypeTag("ConsistentQuery"))

	sizeLimitError := handler.config.BlobSizeLimitError(namespaceName.String())
	sizeLimitWarn := handler.config.BlobSizeLimitWarn(namespaceName.String())

	// Complete or fail all queries we have results for
	for id, result := range queryResults {
		if err := common.CheckEventBlobSizeLimit(
			result.GetAnswer().Size(),
			sizeLimitWarn,
			sizeLimitError,
			namespaceName.String(),
			workflowID,
			runID,
			scope,
			handler.throttledLogger,
			"ConsistentQuery",
		); err != nil {
			handler.logger.Info("failing query because query result size is too large",
				tag.WorkflowNamespace(namespaceName.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.QueryID(id),
				tag.Error(err))
			failedCompletionState := &historyi.QueryCompletionState{
				Type: workflow.QueryCompletionTypeFailed,
				Err:  err,
			}
			if err := queryRegistry.SetCompletionState(id, failedCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to failed",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			}
		} else {
			succeededCompletionState := &historyi.QueryCompletionState{
				Type:   workflow.QueryCompletionTypeSucceeded,
				Result: result,
			}
			if err := queryRegistry.SetCompletionState(id, succeededCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to succeeded",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			}
		}
	}

	// If no workflow task was created then it means no buffered events came in during this workflow task's handling.
	// This means all unanswered buffered queries can be dispatched directly through matching at this point.
	if !createNewWorkflowTask {
		buffered := queryRegistry.GetBufferedIDs()
		for _, id := range buffered {
			unblockCompletionState := &historyi.QueryCompletionState{
				Type: workflow.QueryCompletionTypeUnblocked,
			}
			if err := queryRegistry.SetCompletionState(id, unblockCompletionState); err != nil {
				handler.logger.Error(
					"failed to set query completion state to unblocked",
					tag.WorkflowNamespace(namespaceName.String()),
					tag.WorkflowID(workflowID),
					tag.WorkflowRunID(runID),
					tag.QueryID(id),
					tag.Error(err))
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			}
		}
	}
}

func failWorkflowTask(
	ctx context.Context,
	shardContext historyi.ShardContext,
	wfContext historyi.WorkflowContext,
	workflowTask *historyi.WorkflowTaskInfo,
	wtFailedCause *workflowTaskFailedCause,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (historyi.MutableState, int64, error) {

	// clear any updates we have accumulated so far
	wfContext.Clear()

	// Reload workflow execution so we can apply the workflow task failure event
	mutableState, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, common.EmptyEventID, err
	}
	wtFailedEvent, err := mutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		wtFailedCause.failedCause,
		failure.NewServerFailure(wtFailedCause.Message(), false),
		request.GetIdentity(),
		nil,
		request.GetBinaryChecksum(),
		"",
		"",
		0,
	)
	if err != nil {
		return nil, common.EmptyEventID, err
	}

	var wtFailedEventID int64
	if wtFailedEvent != nil {
		// If WTFailed event was added to the history then use its Id as wtFailedEventID.
		wtFailedEventID = wtFailedEvent.GetEventId()
	} else {
		// Otherwise, if it was transient WT, last event should be WTFailed event from the 1st attempt.
		wtFailedEventID = mutableState.GetNextEventID() - 1
	}

	// Return reloaded mutable state back to the caller for further updates.
	return mutableState, wtFailedEventID, nil
}

func (handler *WorkflowTaskCompletedHandler) clearStickyTaskQueue(ctx context.Context, wfContext historyi.WorkflowContext) error {

	// Clear all changes in the workflow context that was made already.
	wfContext.Clear()

	ms, err := wfContext.LoadMutableState(ctx, handler.shardContext)
	if err != nil {
		return err
	}
	ms.ClearStickyTaskQueue()
	err = wfContext.UpdateWorkflowExecutionAsActive(ctx, handler.shardContext)
	if err != nil {
		return err
	}
	return nil
}
