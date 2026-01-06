package matching

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errDefaultQueueNotInit = serviceerror.NewInternal("defaultQueue is not initializaed")

const (
	defaultTaskDispatchRPS    = 100000.0
	defaultTaskDispatchRPSTTL = time.Minute
)

type (

	// Represents a single partition of a (user-level) Task Queue in memory state. Under the hood, each Task Queue
	// partition is made of one or more DB-level queues. There is always a default DB queue. For
	// versioned TQs, there is an additional DB queue for each Build ID.
	// Currently, the liveness of a partition manager is tied to its default queue. More specifically:
	//  - If the default queue dies, all the other queues are also stopped and the whole partition is unloaded from
	//    matching engine.
	//  - Any requests sent to the partition keeps the default queue alive, even if it's served by a versioned queue.
	//  - If a versioned queue dies, we only unload that specific queue from the partition.
	//  - This behavior is subject to optimizations in the future: for versioned queues, keeping the default queue
	//    loaded all the time may be suboptimal.
	taskQueuePartitionManagerImpl struct {
		engine    *matchingEngineImpl
		partition tqid.Partition
		ns        *namespace.Namespace
		config    *taskQueueConfig
		// used for non-sticky versioned queues (one for each version)
		versionedQueues     map[PhysicalTaskQueueVersion]physicalTaskQueueManager
		versionedQueuesLock sync.RWMutex // locks mutation of versionedQueues
		userDataManager     userDataManager
		logger              log.Logger
		throttledLogger     log.ThrottledLogger
		matchingClient      matchingservice.MatchingServiceClient
		metricsHandler      metrics.Handler // namespace/taskqueue tagged metric scope
		// TODO(stephanos): move cache out of partition manager
		cache cache.Cache // non-nil for root-partition

		autoEnableRateLimiter quotas.RateLimiter
		fairnessState         enumsspb.FairnessState // Set once on initialization and read only after
		defaultQueueFuture    *future.FutureImpl[physicalTaskQueueManager]
		initCtx               context.Context
		initCancel            func()

		cancelNewMatcherSub func()
		cancelFairnessSub   func()

		// rateLimitManager is used to manage the rate limit for task queues.
		rateLimitManager *rateLimitManager
	}
)

func (pm *taskQueuePartitionManagerImpl) PutCache(key any, value any) {
	pm.cache.Put(key, value)
}

func (pm *taskQueuePartitionManagerImpl) GetCache(key any) any {
	return pm.cache.Get(key)
}

var _ taskQueuePartitionManager = (*taskQueuePartitionManagerImpl)(nil)

func newTaskQueuePartitionManager(
	e *matchingEngineImpl,
	ns *namespace.Namespace,
	partition tqid.Partition,
	tqConfig *taskQueueConfig,
	logger log.Logger,
	throttledLogger log.Logger,
	metricsHandler metrics.Handler,
	userDataManager userDataManager,
) (*taskQueuePartitionManagerImpl, error) {
	rateLimitManager := newRateLimitManager(
		userDataManager,
		tqConfig,
		partition.TaskQueue().TaskType())
	pm := &taskQueuePartitionManagerImpl{
		engine:                e,
		partition:             partition,
		ns:                    ns,
		config:                tqConfig,
		logger:                logger,
		throttledLogger:       throttledLogger,
		matchingClient:        e.matchingRawClient,
		metricsHandler:        metricsHandler,
		versionedQueues:       make(map[PhysicalTaskQueueVersion]physicalTaskQueueManager),
		userDataManager:       userDataManager,
		rateLimitManager:      rateLimitManager,
		defaultQueueFuture:    future.NewFuture[physicalTaskQueueManager](),
		autoEnableRateLimiter: quotas.NewRateLimiter(1.0/60, 1),
	}
	pm.initCtx, pm.initCancel = context.WithCancel(context.Background())

	if pm.partition.IsRoot() {
		pm.cache = cache.New(10000, &cache.Options{
			TTL: max(1, tqConfig.TaskQueueInfoByBuildIdTTL())}, // ensure TTL is never zero (which would disable TTL)
		)
	}

	return pm, nil
}

func (pm *taskQueuePartitionManagerImpl) initialize() (retErr error) {
	defer func() { pm.defaultQueueFuture.SetIfNotReady(nil, retErr) }()
	unload := func(bool) {
		pm.unloadFromEngine(unloadCauseConfigChange)
	}

	err := pm.userDataManager.WaitUntilInitialized(pm.initCtx)
	if err != nil {
		return err
	}
	data, _, err := pm.getPerTypeUserData()
	if err != nil {
		return err
	}

	pm.fairnessState = data.GetFairnessState()
	switch {
	case !pm.config.AutoEnableV2() || pm.fairnessState == enumsspb.FAIRNESS_STATE_UNSPECIFIED:
		var fairness bool
		changeKey := pm.partition.GradualChangeKey()
		fairness, pm.cancelFairnessSub = dynamicconfig.SubscribeGradualChange(
			pm.config.EnableFairnessSub, changeKey, unload, pm.engine.timeSource)
		// Fairness is disabled for sticky queues for now so that we can still use TTLs.
		pm.config.EnableFairness = fairness && pm.partition.Kind() != enumspb.TASK_QUEUE_KIND_STICKY
		if fairness {
			pm.config.NewMatcher = true
		} else {
			pm.config.NewMatcher, pm.cancelNewMatcherSub = dynamicconfig.SubscribeGradualChange(
				pm.config.NewMatcherSub, changeKey, unload, pm.engine.timeSource)
		}
	case pm.fairnessState == enumsspb.FAIRNESS_STATE_V0:
		pm.config.NewMatcher = false
		pm.config.EnableFairness = false
	case pm.fairnessState == enumsspb.FAIRNESS_STATE_V1:
		pm.config.NewMatcher = true
		pm.config.EnableFairness = false
	case pm.fairnessState == enumsspb.FAIRNESS_STATE_V2:
		pm.config.NewMatcher = true
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			pm.config.EnableFairness = false
		} else {
			pm.config.EnableFairness = true
		}
	default:
		return serviceerror.NewInternal("Unknown FairnessState in UserData")
	}

	defaultQ, err := newPhysicalTaskQueueManager(pm, UnversionedQueueKey(pm.partition))
	if err != nil {
		return err
	}
	pm.defaultQueueFuture.Set(defaultQ, nil)
	defaultQ.Start()
	return nil
}

func (pm *taskQueuePartitionManagerImpl) defaultQueue() physicalTaskQueueManager {
	queue, err := pm.defaultQueueFuture.GetIfReady()
	if err != nil {
		softassert.Fail(pm.logger, "defaultQueue used but not initialized or initialization failed", tag.Error(err))
	}
	return queue
}

func (pm *taskQueuePartitionManagerImpl) Start() {
	pm.engine.updateTaskQueuePartitionGauge(pm.Namespace(), pm.partition, 1)
	pm.userDataManager.Start()
	//nolint:errcheck
	go pm.initialize()
}

func (pm *taskQueuePartitionManagerImpl) GetRateLimitManager() *rateLimitManager {
	return pm.rateLimitManager
}

// Stop does not unload the partition from matching engine. It is intended to be called by matching engine when
// unloading the partition. For stopping and unloading a partition call unloadFromEngine instead.
func (pm *taskQueuePartitionManagerImpl) Stop(unloadCause unloadCause) {
	pm.initCancel()
	queue, err := pm.defaultQueueFuture.Get(context.Background())
	if err == nil {
		queue.Stop(unloadCause)
	}

	if pm.cancelFairnessSub != nil {
		pm.cancelFairnessSub()
	}
	if pm.cancelNewMatcherSub != nil {
		pm.cancelNewMatcherSub()
	}

	pm.versionedQueuesLock.Lock()
	// First, stop all queues to wrap up ongoing operations.
	for _, vq := range pm.versionedQueues {
		vq.Stop(unloadCause)
	}
	pm.versionedQueuesLock.Unlock()

	// Then, stop user data manager to wrap up any reads/writes.
	pm.userDataManager.Stop()

	// Finally, stop rate limit manager (used by queues and using user data manager).
	pm.rateLimitManager.Stop()

	pm.engine.updateTaskQueuePartitionGauge(pm.Namespace(), pm.partition, -1)
}

func (pm *taskQueuePartitionManagerImpl) Namespace() *namespace.Namespace {
	return pm.ns
}

func (pm *taskQueuePartitionManagerImpl) MarkAlive() {
	dbq := pm.defaultQueue()
	if dbq != nil {
		dbq.MarkAlive()
	}
}

func (pm *taskQueuePartitionManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	queue, err := pm.defaultQueueFuture.Get(ctx)
	if err != nil {
		return err
	}
	return queue.WaitUntilInitialized(ctx)
}

func (pm *taskQueuePartitionManagerImpl) autoEnableIfNeeded(ctx context.Context, params addTaskParams) {
	if pm.fairnessState != enumsspb.FAIRNESS_STATE_UNSPECIFIED {
		return
	}
	if params.taskInfo.Priority.GetFairnessKey() == "" {
		if params.taskInfo.Priority.GetPriorityKey() == int32(0) {
			return
		}
		// Do not auto enable if we only see priority and we're using new matcher already
		if pm.config.NewMatcher {
			return
		}
	}
	if !pm.Partition().IsRoot() || pm.Partition().Kind() == enumspb.TASK_QUEUE_KIND_STICKY || !pm.config.AutoEnableV2() {
		return
	}
	if !pm.autoEnableRateLimiter.Allow() {
		return
	}
	req := &matchingservice.UpdateFairnessStateRequest{
		NamespaceId:   pm.Namespace().ID().String(),
		TaskQueue:     pm.Partition().RpcName(),
		TaskQueueType: pm.Partition().TaskType(),
		FairnessState: enumsspb.FAIRNESS_STATE_V2,
	}
	_, err := pm.matchingClient.UpdateFairnessState(ctx, req)
	if err != nil {
		pm.logger.Error("could not update userdata for autoenable", tag.Error(err))
	}
}

func (pm *taskQueuePartitionManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (buildId string, syncMatched bool, err error) {
	var spoolQueue, syncMatchQueue physicalTaskQueueManager
	directive := params.taskInfo.GetVersionDirective()

	pm.autoEnableIfNeeded(ctx, params)
	// spoolQueue will be nil iff task is forwarded.
reredirectTask:
	spoolQueue, syncMatchQueue, _, taskDispatchRevisionNumber, targetVersion, err := pm.getPhysicalQueuesForAdd(ctx, directive, params.forwardInfo, params.taskInfo.GetRunId(), params.taskInfo.GetWorkflowId(), false)
	if err != nil {
		return "", false, err
	}

	syncMatchTask := newInternalTaskForSyncMatch(params.taskInfo, params.forwardInfo, taskDispatchRevisionNumber, targetVersion)
	pm.config.setDefaultPriority(syncMatchTask)
	if spoolQueue != nil && spoolQueue.QueueKey().Version().BuildId() != syncMatchQueue.QueueKey().Version().BuildId() {
		// Task is not forwarded and build ID is different on the two queues -> redirect rule is being applied.
		// Set redirectInfo in the task as it will be needed if we have to forward the task.
		syncMatchTask.redirectInfo = &taskqueuespb.BuildIdRedirectInfo{
			AssignedBuildId: spoolQueue.QueueKey().Version().BuildId(),
		}
	}

	dbq := pm.defaultQueue()
	if dbq == nil {
		return "", false, errDefaultQueueNotInit
	}
	if dbq != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		dbq.MarkAlive()
	}

	if pm.partition.IsRoot() && !pm.HasAnyPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		pm.metricsHandler.Counter(metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()).Record(1)
	}

	isActive, err := pm.isActiveInCluster()
	if err != nil {
		return "", false, err
	}

	if isActive {
		syncMatched, err = syncMatchQueue.TrySyncMatch(ctx, syncMatchTask)
		if syncMatched && !pm.shouldBacklogSyncMatchTaskOnError(err) {

			// Build ID is not returned for sync match. The returned build ID is used by History to update
			// mutable state (and visibility) when the first workflow task is spooled.
			// For sync-match case, History has already received the build ID in the Record*TaskStarted call.
			// By omitting the build ID from this response we help History immediately know that no MS update is needed.
			return "", syncMatched, err
		} else if errors.Is(err, errReprocessTask) {
			// We get this if userdata changed while the task was blocked in TrySyncMatch
			// (only for backlog tasks forwarded to root with the new matcher)
			goto reredirectTask
		}
		// other errors are ignored and we try to spool the task
	}

	if spoolQueue == nil {
		// This means the task is being forwarded. Child partition will persist the task when sync match fails.
		return "", false, errRemoteSyncMatchFailed
	}

	var assignedBuildId string
	if directive.GetUseAssignmentRules() != nil {
		// return build ID only if a new one is assigned.
		assignedBuildId = spoolQueue.QueueKey().Version().BuildId()
	}

	return assignedBuildId, false, spoolQueue.SpoolTask(params.taskInfo)
}

func (pm *taskQueuePartitionManagerImpl) shouldBacklogSyncMatchTaskOnError(err error) bool {
	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if err != nil && errors.As(err, &resourceExhaustedErr) {
		if resourceExhaustedErr.Cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
			return true
		}
	}
	return false
}

func (pm *taskQueuePartitionManagerImpl) isActiveInCluster() (bool, error) {
	ns, err := pm.engine.namespaceRegistry.GetNamespaceByID(pm.ns.ID())
	if err == nil {
		return ns.ActiveInCluster(pm.engine.clusterMeta.GetCurrentClusterName()), nil
	}
	return false, err
}

// PollTask returns a task if there was a match, a boolean that reports whether a versionSet was used for the match, and an error
func (pm *taskQueuePartitionManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, bool, error) {
	var err error
	dbq := pm.defaultQueue()
	if dbq == nil {
		return nil, false, errDefaultQueueNotInit
	}
	versionSetUsed := false
	deployment, err := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities, pollMetadata.deploymentOptions)
	if err != nil {
		return nil, false, err
	}

	if deployment != nil {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// TODO: reject poller of old sticky queue if newer version exist
		} else {
			// default queue should stay alive even if requests go to other queues
			dbq.MarkAlive()
			dbq, err = pm.getVersionedQueue(ctx, "", "", deployment, true)

			if err != nil {
				return nil, false, err
			}
		}
	} else if pollMetadata.workerVersionCapabilities.GetUseVersioning() {
		// V1 & V2 versioning
		userData, _, err := pm.userDataManager.GetUserData()
		if err != nil {
			return nil, false, err
		}

		versioningData := userData.GetData().GetVersioningData()
		buildId := pollMetadata.workerVersionCapabilities.GetBuildId()
		if buildId == "" {
			return nil, false, serviceerror.NewInvalidArgument("build ID must be provided when using worker versioning")
		}

		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// In the sticky case we always use the unversioned queue
			// For the old API, we may kick off this worker if there's a newer one.
			oldVersioning, err := checkVersionForStickyPoll(versioningData, pollMetadata.workerVersionCapabilities)
			if err != nil {
				return nil, false, err
			}

			if !oldVersioning {
				activeRules := getActiveRedirectRules(versioningData.GetRedirectRules())
				terminalBuildId := findTerminalBuildId(buildId, activeRules)
				if terminalBuildId != buildId {
					return nil, false, serviceerror.NewNewerBuildExists(terminalBuildId)
				}
			}
			// We set versionSetUsed to true for all sticky tasks until old versioning is cleaned up.
			// this value is used by matching_engine to decide if it should pass the worker build ID
			// to history in the recordStart call or not. We don't need to pass build ID for sticky
			// tasks as no redirect happen in a sticky queue.
			versionSetUsed = true
		} else {
			// default queue should stay alive even if requests go to other queues
			dbq.MarkAlive()

			var versionSet string
			if versioningData.GetVersionSets() != nil {
				versionSet, err = pm.getVersionSetForPoll(pollMetadata.workerVersionCapabilities, versioningData)
				if err != nil {
					return nil, false, err
				}
			}

			// use version set if found, otherwise assume user is using new API
			if versionSet != "" {
				versionSetUsed = true
				dbq, err = pm.getVersionedQueue(ctx, versionSet, "", nil, true)
			} else {
				activeRules := getActiveRedirectRules(versioningData.GetRedirectRules())
				terminalBuildId := findTerminalBuildId(buildId, activeRules)
				if terminalBuildId != buildId {
					return nil, false, serviceerror.NewNewerBuildExists(terminalBuildId)
				}
				pm.loadUpstreamBuildIds(buildId, activeRules)
				dbq, err = pm.getVersionedQueue(ctx, "", buildId, nil, true)
			}
		}

		if err != nil {
			return nil, false, err
		}
	}

	if identity, ok := ctx.Value(identityKey).(string); ok && identity != "" {
		dbq.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
		// update timestamp when long poll ends
		defer dbq.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
	}

	// The desired global rate limit for the task queue can come from multiple sources:
	// UpdateTaskQueueConfig API call, poller metadata, or system default.
	// In the case of worker set rate limits at the time of poll task :
	// we update the ratelimiter rps if it has changed from the previous poll.
	// Last poller wins if different pollers provide different values
	// Highest priority is given to the rate limit set by the UpdateTaskQueueConfig api call.
	// Followed by the rate limit set by the poller.
	// UpdateRateLimit implicitly handles whether an update is required or not,
	// based on whether the effectiveRPS has changed.
	pm.rateLimitManager.InjectWorkerRPS(pollMetadata)

	task, err := dbq.PollTask(ctx, pollMetadata)
	if task != nil {
		task.pollerScalingDecision = dbq.MakePollerScalingDecision(ctx, pollMetadata.localPollStartTime)
	}

	return task, versionSetUsed, err
}

// GetPhysicalQueueAdjustedStats retrieves task queue stats for the given physical queue.
// Returns nil if stats cannot be retrieved or are not available.
func (pm *taskQueuePartitionManagerImpl) GetPhysicalQueueAdjustedStats(
	ctx context.Context,
	physicalQueue physicalTaskQueueManager,
) *taskqueuepb.TaskQueueStats {
	// buildID would be empty for either the unversioned queue or when using v3 worker-versioning.
	buildID := physicalQueue.QueueKey().Version().BuildId()

	// Check if the queue is versioned queue using v3 worker-versioning
	deployment := physicalQueue.QueueKey().Version().Deployment()
	if deployment != nil {
		buildID = worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment))
	}

	partitionInfo, err := pm.Describe(ctx, map[string]bool{buildID: true}, false, true, false, false)
	if err != nil {
		return nil
	}

	info, ok := partitionInfo.GetVersionsInfoInternal()[buildID]
	if !ok || info.GetPhysicalTaskQueueInfo().GetTaskQueueStats() == nil {
		return nil
	}
	return info.GetPhysicalTaskQueueInfo().GetTaskQueueStats()
}

// TODO(pri): old matcher cleanup
func (pm *taskQueuePartitionManagerImpl) ProcessSpooledTask(
	ctx context.Context,
	task *internalTask,
	backlogQueue *PhysicalTaskQueueKey,
) error {
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	directive := taskInfo.GetVersionDirective()
	assignedBuildId := backlogQueue.Version().BuildId()
	if assignedBuildId != "" {
		// construct directive based on the build ID of the spool queue
		directive = worker_versioning.MakeBuildIdDirective(assignedBuildId)
	}
	// Redirect and re-resolve if we're blocked in matcher and user data changes.
	for {
		newBacklogQueue, syncMatchQueue, userDataChanged, taskDispatchRevisionNumber, targetVersion, err := pm.getPhysicalQueuesForAdd(ctx,
			directive,
			nil,
			taskInfo.GetRunId(),
			taskInfo.GetWorkflowId(),
			false)
		if err != nil {
			return err
		}

		task.targetWorkerDeploymentVersion = targetVersion

		// Update the task dispatch revision number on the task since the routingConfig of the partition
		// may have changed after the task was spooled.
		task.taskDispatchRevisionNumber = taskDispatchRevisionNumber

		// set redirect info if spoolQueue and syncMatchQueue build ids are different
		if assignedBuildId != syncMatchQueue.QueueKey().Version().BuildId() {
			task.redirectInfo = &taskqueuespb.BuildIdRedirectInfo{
				AssignedBuildId: assignedBuildId,
			}
		} else {
			// make sure to reset redirectInfo in case it was set in a previous loop cycle
			task.redirectInfo = nil
		}
		if !backlogQueue.version.Deployment().Equal(newBacklogQueue.QueueKey().version.Deployment()) {
			// Backlog queue has changed, spool to the new queue. This should happen rarely: when
			// activity of pinned workflow was determined independent and sent to the default queue
			// but now at dispatch time, the determination is different because the activity pollers
			// on the pinned deployment have reached server.
			// TODO: before spooling, try to sync-match the task on the new queue
			err = newBacklogQueue.SpoolTask(taskInfo)
			if err != nil {
				// return the error so task_reader retries the outer call
				return err
			}
			// Finish the task because now it is copied to the other backlog. It should be considered
			// invalid because a poller did not receive the task.
			task.finish(nil, false)
			return nil
		}
		err = syncMatchQueue.DispatchSpooledTask(ctx, task, userDataChanged)
		if err != errInterrupted {
			return err
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) AddSpooledTask(
	ctx context.Context,
	task *internalTask,
	backlogQueue *PhysicalTaskQueueKey,
) error {
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	directive := taskInfo.GetVersionDirective()
	assignedBuildId := backlogQueue.Version().BuildId()
	if assignedBuildId != "" {
		// construct directive based on the build ID of the spool queue
		directive = worker_versioning.MakeBuildIdDirective(assignedBuildId)
	}
	newBacklogQueue, syncMatchQueue, _, taskDispatchRevisionNumber, targetVersion, err := pm.getPhysicalQueuesForAdd(
		ctx,
		directive,
		nil,
		taskInfo.GetRunId(),
		taskInfo.GetWorkflowId(),
		false,
	)
	if err != nil {
		return err
	}

	task.targetWorkerDeploymentVersion = targetVersion

	// Update the task dispatch revision number on the task since the routingConfig of the partition
	// may have changed after the task was spooled.
	task.taskDispatchRevisionNumber = taskDispatchRevisionNumber

	// set redirect info if spoolQueue and syncMatchQueue build ids are different
	if assignedBuildId != syncMatchQueue.QueueKey().Version().BuildId() {
		task.redirectInfo = &taskqueuespb.BuildIdRedirectInfo{
			AssignedBuildId: assignedBuildId,
		}
	} else {
		// make sure to reset redirectInfo in case it was set in a previous loop cycle
		task.redirectInfo = nil
	}
	if !backlogQueue.version.Deployment().Equal(newBacklogQueue.QueueKey().version.Deployment()) {
		// Backlog queue has changed, spool to the new queue. This should happen rarely: when
		// activity of pinned workflow was determined independent and sent to the default queue
		// but now at dispatch time, the determination is different because the activity pollers
		// on the pinned deployment have reached server.
		// TODO: before spooling, try to sync-match the task on the new queue
		err = newBacklogQueue.SpoolTask(taskInfo)
		if err != nil {
			// return the error so task_reader retries the outer call
			return err
		}
		// Finish the task because now it is copied to the other backlog. It should be considered
		// invalid because a poller did not receive the task.
		task.finish(nil, false)
		return nil
	}
	syncMatchQueue.AddSpooledTaskToMatcher(task)
	return nil
}

func (pm *taskQueuePartitionManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
reredirectTask:
	_, syncMatchQueue, _, _, _, err := pm.getPhysicalQueuesForAdd(ctx,
		request.VersionDirective,
		// We do not pass forwardInfo because we want the parent partition to make fresh versioning decision. Note that
		// forwarded Query/Nexus task requests do not expire rapidly in contrast to forwarded activity/workflow tasks
		// that only try up to 200ms sync-match. Therefore, to prevent blocking the request on the wrong build ID, its
		// more important to allow the parent partition to make a fresh versioning decision in case the child partition
		// did not have up-to-date User Data when selected a dispatch build ID.
		nil,
		request.GetQueryRequest().GetExecution().GetRunId(),
		request.GetQueryRequest().GetExecution().GetWorkflowId(),
		true,
	)
	if err != nil {
		return nil, err
	}

	dbq := pm.defaultQueue()
	if dbq == nil {
		return nil, errDefaultQueueNotInit
	}
	if dbq != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		dbq.MarkAlive()
	}

	res, err := syncMatchQueue.DispatchQueryTask(ctx, taskID, request)
	if errors.Is(err, errReprocessTask) {
		// We get this if userdata changed while the task was blocked in DispatchQueryTask
		goto reredirectTask
	}
	return res, err
}

func (pm *taskQueuePartitionManagerImpl) DispatchNexusTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.DispatchNexusTaskRequest,
) (*matchingservice.DispatchNexusTaskResponse, error) {
reredirectTask:
	_, syncMatchQueue, _, _, _, err := pm.getPhysicalQueuesForAdd(ctx,
		worker_versioning.MakeUseAssignmentRulesDirective(),
		// We do not pass forwardInfo because we want the parent partition to make fresh versioning decision. Note that
		// forwarded Query/Nexus task requests do not expire rapidly in contrast to forwarded activity/workflow tasks
		// that only try up to 200ms sync-match. Therefore, to prevent blocking the request on the wrong build ID, its
		// more important to allow the parent partition to make a fresh versioning decision in case the child partition
		// did not have up-to-date User Data when selected a dispatch build ID.
		nil,
		"",
		"",
		false)
	if err != nil {
		return nil, err
	}

	dbq := pm.defaultQueue()
	if dbq == nil {
		return nil, errDefaultQueueNotInit
	}
	if dbq != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		dbq.MarkAlive()
	}

	res, err := syncMatchQueue.DispatchNexusTask(ctx, taskId, request)
	if errors.Is(err, errReprocessTask) {
		// We get this if userdata changed while the task was blocked in DispatchNexusTask
		goto reredirectTask
	}
	return res, err
}

func (pm *taskQueuePartitionManagerImpl) GetUserDataManager() userDataManager {
	return pm.userDataManager
}

func (pm *taskQueuePartitionManagerImpl) GetConfig() *taskQueueConfig {
	return pm.config
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (pm *taskQueuePartitionManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	dbq := pm.defaultQueue()
	var ret []*taskqueuepb.PollerInfo
	if dbq != nil {
		ret = dbq.GetAllPollerInfo()
	}
	pm.versionedQueuesLock.RLock()
	defer pm.versionedQueuesLock.RUnlock()
	for _, vq := range pm.versionedQueues {
		info := vq.GetAllPollerInfo()
		ret = append(ret, info...)

		// We want DescribeTaskQueue to count for task queue liveness for all loaded versioned
		// queues, and this is the most convenient place to mark liveness, since it's called by
		// LegacyDescribeTaskQueue (and also getPhysicalQueuesForAdd, but that's versioning 1
		// and will be removed soon).
		vq.MarkAlive()
	}
	return ret
}

func (pm *taskQueuePartitionManagerImpl) HasAnyPollerAfter(accessTime time.Time) bool {
	dbq := pm.defaultQueue()
	if dbq != nil && dbq.HasPollerAfter(accessTime) {
		return true
	}
	pm.versionedQueuesLock.RLock()
	defer pm.versionedQueuesLock.RUnlock()
	for _, ptqm := range pm.versionedQueues {
		if ptqm.HasPollerAfter(accessTime) {
			return true
		}
	}
	return false
}

func (pm *taskQueuePartitionManagerImpl) HasPollerAfter(buildId string, accessTime time.Time) bool {
	if buildId == "" {
		dbq := pm.defaultQueue()
		if dbq != nil && dbq.HasPollerAfter(accessTime) {
			return true
		}
		return false
	}
	pm.versionedQueuesLock.RLock()
	// TODO: support v3 versioning
	vq, ok := pm.versionedQueues[PhysicalTaskQueueVersion{buildId: buildId}]
	pm.versionedQueuesLock.RUnlock()
	if !ok {
		return false
	}
	return vq.HasPollerAfter(accessTime)
}

func (pm *taskQueuePartitionManagerImpl) LegacyDescribeTaskQueue(includeTaskQueueStatus bool) (*matchingservice.DescribeTaskQueueResponse, error) {
	resp := &matchingservice.DescribeTaskQueueResponse{
		DescResponse: &workflowservice.DescribeTaskQueueResponse{
			Pollers: pm.GetAllPollerInfo(),
		},
	}
	dbq := pm.defaultQueue()
	if dbq == nil {
		return nil, errDefaultQueueNotInit
	}
	if includeTaskQueueStatus {
		//nolint:staticcheck // SA1019: [cleanup-wv-3.1]
		resp.DescResponse.TaskQueueStatus = dbq.LegacyDescribeTaskQueue(true).DescResponse.TaskQueueStatus
	}
	if pm.partition.Kind() != enumspb.TASK_QUEUE_KIND_STICKY {
		perTypeUserData, _, err := pm.getPerTypeUserData()
		if err != nil {
			return nil, err
		}

		current, _, currentUpdateTime, ramping, isRamping, rampingPercentage, _, rampingUpdateTime := worker_versioning.CalculateTaskQueueVersioningInfo(perTypeUserData.GetDeploymentData())

		info := &taskqueuepb.TaskQueueVersioningInfo{
			//nolint:staticcheck // SA1019: [cleanup-wv-3.1]
			CurrentVersion:           worker_versioning.WorkerDeploymentVersionToStringV31(current),
			CurrentDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromVersion(current),
			UpdateTime:               timestamppb.New(currentUpdateTime),
		}

		if isRamping {
			info.RampingVersionPercentage = rampingPercentage
			// If task queue is ramping to unversioned, ramping will be nil, which converts to "__unversioned__"
			//nolint:staticcheck // SA1019: [cleanup-wv-3.1]
			info.RampingVersion = worker_versioning.WorkerDeploymentVersionToStringV31(ramping)
			info.RampingDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromVersion(ramping)
			if info.GetUpdateTime().AsTime().Before(rampingUpdateTime) {
				info.UpdateTime = timestamppb.New(rampingUpdateTime)
			}
		}

		resp.DescResponse.VersioningInfo = info
	}
	return resp, nil
}

// In order to accommodate the changes brought in by versioning-3.1, `buildIDs` will now also accept versionID's that represent worker-deployment versions.
func (pm *taskQueuePartitionManagerImpl) Describe(
	ctx context.Context,
	buildIds map[string]bool,
	includeAllActive, reportStats, reportPollers, internalTaskQueueStatus bool,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	pm.versionedQueuesLock.RLock()

	versions := make(map[PhysicalTaskQueueVersion]bool)

	// Active means that the physical queue for that version is loaded.
	// An empty string refers to the unversioned queue, which is always loaded.
	// In the future, active will mean that the physical queue for that version has had a task added recently or a recent poller.
	if includeAllActive {
		for k := range pm.versionedQueues {
			versions[k] = true
		}
	}

	for b := range buildIds {
		if b == "" {
			dbq := pm.defaultQueue()
			if dbq == nil {
				return nil, errDefaultQueueNotInit
			}
			versions[dbq.QueueKey().Version()] = true
		} else {
			found := false
			for k := range pm.versionedQueues {
				// Storing the versioned queue if the buildID is a v2 based buildID or a versionID representing a worker-deployment version (which could be v31 or v32)
				if k.BuildId() == b ||
					worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(k.Deployment())) == b ||
					worker_versioning.ExternalWorkerDeploymentVersionToStringV31(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(k.Deployment())) == b {
					versions[k] = true
					found = true
					break
				}
			}
			if !found {
				dv, _ := worker_versioning.WorkerDeploymentVersionFromStringV32(b)
				if dv != nil {
					// Add v3 version.
					versions[PhysicalTaskQueueVersion{
						buildId:              dv.BuildId,
						deploymentSeriesName: dv.DeploymentName,
					}] = true
				} else {
					// Still add v2 version because user explicitly asked for the stats, we'd make sure to load the TQ.
					versions[PhysicalTaskQueueVersion{buildId: b}] = true
				}
			}
		}
	}

	pm.versionedQueuesLock.RUnlock()

	var unversionedStatsByPriority map[int32]*taskqueuepb.TaskQueueStats
	var currentVersion *deploymentspb.WorkerDeploymentVersion
	var rampingVersion *deploymentspb.WorkerDeploymentVersion
	var rampPercentage float32
	var currentExists bool
	var rampingExists bool
	var isRamping bool
	var unversionedCurrentShareByPriority map[int32]*taskqueuepb.TaskQueueStats
	var unversionedRampingShareByPriority map[int32]*taskqueuepb.TaskQueueStats

	if reportStats {
		// Consider the default/unversioned queue. For current/ramping deployment versions, tasks are backlogged
		// here, so we include this queue's stats if the version to describe is a current/ramping version.
		dbq := pm.defaultQueue()
		if dbq == nil {
			return nil, errDefaultQueueNotInit
		}
		unversionedStatsByPriority = dbq.GetStatsByPriority()

		userData, _, err := pm.GetUserDataManager().GetUserData()
		if err != nil {
			return nil, err
		}
		perType := userData.GetData().GetPerType()[int32(pm.Partition().TaskType())]
		deploymentData := perType.GetDeploymentData()

		currentVersion, _, _, rampingVersion, isRamping, rampPercentage, _, _ =
			worker_versioning.CalculateTaskQueueVersioningInfo(deploymentData)

		// Technically, one could have a current version of "unversioned" which shall make currentExists false according
		// to the current logic. However, as of now, the user cannot query the stats of the "unversioned" version so this
		// logic is fine. In other words, this logic is used to only attribute the unversioned backlog to the current version
		// when current version is NOT "unversioned".
		//
		// When the ramping version is "unversioned", isRamping is true which shall make the attribution logic work as expected.
		currentExists = currentVersion != nil
		rampingExists = isRamping && rampPercentage > 0

		// Split the unversioned queue's stats per priority so TaskQueueStatsByPriorityKey can
		// be adjusted consistently with TaskQueueStats.
		unversionedCurrentShareByPriority = map[int32]*taskqueuepb.TaskQueueStats{}
		unversionedRampingShareByPriority = map[int32]*taskqueuepb.TaskQueueStats{}
		if rampingExists {
			unversionedCurrentShareByPriority, unversionedRampingShareByPriority =
				splitStatsByPriorityByRampPercentage(unversionedStatsByPriority, rampPercentage)
		} else if currentExists {
			// If there exist no ramping version, weattribute the entire unversioned backlog to the current version.
			unversionedCurrentShareByPriority = cloneStatsByPriority(unversionedStatsByPriority)
		}
	}

	versionsInfo := make(map[string]*taskqueuespb.TaskQueueVersionInfoInternal, 0)
	for v := range versions {
		vInfo := &taskqueuespb.TaskQueueVersionInfoInternal{
			PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{},
		}

		// `getPhysicalQueue` always needs the right buildID passed to function correctly. If the version is a worker-deployment version and an empty buildID is passed,
		// the function returns the default queue which is not what we want.
		// The following assigns buildID to either a v2 based buildID or a buildID part of a worker-deployment version.
		buildID := v.BuildId()
		if v.Deployment() != nil {
			buildID = v.Deployment().BuildId
		}

		physicalQueue, err := pm.getPhysicalQueue(ctx, buildID, v.Deployment())
		if err != nil {
			return nil, err
		}
		if reportPollers {
			vInfo.PhysicalTaskQueueInfo.Pollers = physicalQueue.GetAllPollerInfo()
		}
		if reportStats {
			physicalStatsByPriority := physicalQueue.GetStatsByPriority()

			// Clone the physical queue's stats by priority so we can adjust (either add, subtract) them based on the
			// attribution model defined below.
			adjustedStatsByPriority := cloneStatsByPriority(physicalStatsByPriority)

			// Attribution model (applied per-priority):
			// - If current and/or ramping deployment versions exist, we first "give away" a portion of the
			//   unversioned queue's per-priority stats.
			//
			// Depending on the version described, we have the following options:
			// - For the unversioned version itself, subtract the given-away portion (so we don't double count).
			// - For current/ramping versions, add their share on top of their physical queue stats.
			deploymentVersion := worker_versioning.DeploymentVersionFromDeployment(v.Deployment())

			isUnversionedDescribe := deploymentVersion == nil
			isCurrentDescribe := deploymentVersion.GetDeploymentName() == currentVersion.GetDeploymentName() &&
				deploymentVersion.GetBuildId() == currentVersion.GetBuildId()

			// "Ramping to unversioned" is represented by "rampingExists==true AND rampingVersion==nil".
			// In that case, the ramp share should remain attributed to the unversioned queue stats and
			// there is no separate versioned queue to merge that share into.
			isRampingToUnversioned := rampingExists && rampingVersion == nil
			isRampingDescribe := deploymentVersion.GetDeploymentName() == rampingVersion.GetDeploymentName() &&
				deploymentVersion.GetBuildId() == rampingVersion.GetBuildId()

			if isUnversionedDescribe {
				// Reduce unversioned stats by any shares attributed to versioned queues.
				if currentExists {
					subtractStatsByPriority(adjustedStatsByPriority, unversionedCurrentShareByPriority)
				}
				// Only subtract the ramping share when ramping is to a versioned deployment. If ramping is to
				// unversioned, that share should remain part of the unversioned queue stats.
				if rampingExists && !isRampingToUnversioned {
					subtractStatsByPriority(adjustedStatsByPriority, unversionedRampingShareByPriority)
				}
			} else if isCurrentDescribe && currentExists {
				mergeStatsByPriority(adjustedStatsByPriority, unversionedCurrentShareByPriority)
			} else if isRampingDescribe && rampingExists {
				mergeStatsByPriority(adjustedStatsByPriority, unversionedRampingShareByPriority)
			}

			vInfo.PhysicalTaskQueueInfo.TaskQueueStatsByPriorityKey = adjustedStatsByPriority
			vInfo.PhysicalTaskQueueInfo.TaskQueueStats = aggregateStats(adjustedStatsByPriority)
		}
		if internalTaskQueueStatus {
			vInfo.PhysicalTaskQueueInfo.InternalTaskQueueStatus = physicalQueue.GetInternalTaskQueueStatus()
		}

		// The following assigns buildID to either a v2 based buildID or a versionID representing a worker-deployment version.
		// This is required to populate the right value inside the versionsInfo map. If a user is requesting information for a worker-deployment version,
		// the full worker-deployment version string is used as an entry in the versionsInfo map. Moreover, to keep things backwards compatible, users requesting
		// information for non-deployment related builds will only see the buildID as an entry in the versionsInfo map.
		bid := v.BuildId()
		if v.Deployment() != nil {
			bid = worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(v.Deployment()))
		}
		versionsInfo[bid] = vInfo

		physicalQueue.MarkAlive() // Count Describe for liveness
	}

	return &matchingservice.DescribeTaskQueuePartitionResponse{
		VersionsInfoInternal: versionsInfo,
	}, nil
}

func cloneTaskQueueStats(in *taskqueuepb.TaskQueueStats) *taskqueuepb.TaskQueueStats {
	if in == nil {
		return &taskqueuepb.TaskQueueStats{ApproximateBacklogAge: durationpb.New(0)}
	}
	age := in.GetApproximateBacklogAge()
	if age == nil {
		age = durationpb.New(0)
	}
	return &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: in.GetApproximateBacklogCount(),
		ApproximateBacklogAge:   durationpb.New(age.AsDuration()),
		TasksAddRate:            in.GetTasksAddRate(),
		TasksDispatchRate:       in.GetTasksDispatchRate(),
	}
}

func cloneStatsByPriority(in map[int32]*taskqueuepb.TaskQueueStats) map[int32]*taskqueuepb.TaskQueueStats {
	out := make(map[int32]*taskqueuepb.TaskQueueStats, len(in))
	for pri, s := range in {
		out[pri] = cloneTaskQueueStats(s)
	}
	return out
}

// splitTaskQueueStatsByRampPercentage splits a task queue stats record into "current" and "ramping" shares.
// This split is applied independently per priority bucket (see splitStatsByPriorityByRampPercentage).
func splitTaskQueueStatsByRampPercentage(
	in *taskqueuepb.TaskQueueStats,
	rampPct float32,
) (currentShare *taskqueuepb.TaskQueueStats, rampShare *taskqueuepb.TaskQueueStats) {
	if in == nil {
		in = &taskqueuepb.TaskQueueStats{ApproximateBacklogAge: durationpb.New(0)}
	}

	total := in.GetApproximateBacklogCount()
	// We intentionally bias towards over-attribution ("safe side"):
	// compute both shares with ceil. This can result in currentCount+rampCount > total.
	rampCount := int64(math.Ceil(float64(total) * float64(rampPct) / 100.0))
	currentCount := int64(math.Ceil(float64(total) * float64(100.0-rampPct) / 100.0))

	// Just being defensive here; should never happen.
	if rampCount < 0 {
		rampCount = 0
	}
	if currentCount < 0 {
		currentCount = 0
	}

	rampAddRate := in.GetTasksAddRate() * rampPct / 100
	rampDispatchRate := in.GetTasksDispatchRate() * rampPct / 100

	age := in.GetApproximateBacklogAge()
	if age == nil {
		age = durationpb.New(0)
	}

	// Rather than splitting the backlog age, we set it to the oldest backlog age for both the current and the ramping shares.
	currentAge := durationpb.New(age.AsDuration())
	rampAge := durationpb.New(age.AsDuration())
	if currentCount == 0 {
		currentAge = durationpb.New(0)
	}
	if rampCount == 0 {
		rampAge = durationpb.New(0)
	}

	currentShare = &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: currentCount,
		ApproximateBacklogAge:   currentAge,
		TasksAddRate:            in.GetTasksAddRate() - rampAddRate,
		TasksDispatchRate:       in.GetTasksDispatchRate() - rampDispatchRate,
	}
	rampShare = &taskqueuepb.TaskQueueStats{
		ApproximateBacklogCount: rampCount,
		ApproximateBacklogAge:   rampAge,
		TasksAddRate:            rampAddRate,
		TasksDispatchRate:       rampDispatchRate,
	}
	return currentShare, rampShare
}

// splitStatsByPriorityByRampPercentage splits each priority bucket independently by ramp percentage.
// This is required so that TaskQueueStatsByPriorityKey aligns with TaskQueueStats (which is derived from it).
func splitStatsByPriorityByRampPercentage(
	statsByPriority map[int32]*taskqueuepb.TaskQueueStats,
	rampPct float32,
) (currentShareByPriority, rampShareByPriority map[int32]*taskqueuepb.TaskQueueStats) {
	currentShareByPriority = make(map[int32]*taskqueuepb.TaskQueueStats, len(statsByPriority))
	rampShareByPriority = make(map[int32]*taskqueuepb.TaskQueueStats, len(statsByPriority))
	for pri, s := range statsByPriority {
		current, ramp := splitTaskQueueStatsByRampPercentage(s, rampPct)
		currentShareByPriority[pri] = current
		rampShareByPriority[pri] = ramp
	}
	return currentShareByPriority, rampShareByPriority
}

func ensureStatsWithAge(stats map[int32]*taskqueuepb.TaskQueueStats, pri int32) {
	if _, ok := stats[pri]; !ok {
		stats[pri] = &taskqueuepb.TaskQueueStats{ApproximateBacklogAge: durationpb.New(0)}
		return
	}
	if stats[pri].GetApproximateBacklogAge() == nil {
		stats[pri].ApproximateBacklogAge = durationpb.New(0)
	}
}

func mergeStatsByPriority(into, from map[int32]*taskqueuepb.TaskQueueStats) {
	if len(from) == 0 {
		return
	}
	for pri, s := range from {
		if s == nil {
			continue
		}
		ensureStatsWithAge(into, pri)
		// mergeStats requires non-nil ApproximateBacklogAge on both inputs.
		s = cloneTaskQueueStats(s)
		mergeStats(into[pri], s)
	}
}

func subtractStats(into *taskqueuepb.TaskQueueStats, sub *taskqueuepb.TaskQueueStats) {
	if into == nil || sub == nil {
		return
	}
	into.ApproximateBacklogCount = max(0, into.GetApproximateBacklogCount()-sub.GetApproximateBacklogCount())
	into.TasksAddRate = max(0, into.GetTasksAddRate()-sub.GetTasksAddRate())
	into.TasksDispatchRate = max(0, into.GetTasksDispatchRate()-sub.GetTasksDispatchRate())

	// Rather than splitting the backlog age, we set it to the oldest backlog age for both the current and the ramping shares.
	// However, if the backlog count is zero, we set the backlog age to zero.
	if into.GetApproximateBacklogCount() == 0 || into.GetApproximateBacklogAge() == nil {
		into.ApproximateBacklogAge = durationpb.New(0)
	}
}

func subtractStatsByPriority(into, sub map[int32]*taskqueuepb.TaskQueueStats) {
	if len(sub) == 0 || len(into) == 0 {
		return
	}
	for pri, s := range sub {
		if s == nil {
			continue
		}
		existing, ok := into[pri]
		if !ok || existing == nil {
			continue
		}
		subtractStats(existing, s)
	}
}

func (pm *taskQueuePartitionManagerImpl) Partition() tqid.Partition {
	return pm.partition
}

func (pm *taskQueuePartitionManagerImpl) PartitionCount() int {
	return max(pm.config.NumWritePartitions(), pm.config.NumReadPartitions())
}

func (pm *taskQueuePartitionManagerImpl) LongPollExpirationInterval() time.Duration {
	return pm.config.LongPollExpirationInterval()
}

func (pm *taskQueuePartitionManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	return headers.SetCallerInfo(ctx, headers.NewBackgroundHighCallerInfo(pm.ns.Name().String()))
}

// ForceLoadAllChildPartitions spins off go routines which make RPC calls to all the
func (pm *taskQueuePartitionManagerImpl) ForceLoadAllChildPartitions() {
	if !pm.partition.IsRoot() {
		return
	}

	partition := pm.partition
	taskQueue := partition.TaskQueue()

	namespaceId := partition.NamespaceId()
	taskQueueName := taskQueue.Name()
	taskQueueType := taskQueue.TaskType()
	partitionTotal := pm.config.NumReadPartitions()

	// record total - 1 as we won't try to forceLoad the Root partition.
	pm.metricsHandler.Counter(metrics.ForceLoadedTaskQueuePartitions.Name()).Record(int64(partitionTotal) - 1)

	for partitionId := 1; partitionId < partitionTotal; partitionId++ {

		go func() {
			ctx := pm.callerInfoContext(context.Background())
			resp, err := pm.matchingClient.ForceLoadTaskQueuePartition(ctx, &matchingservice.ForceLoadTaskQueuePartitionRequest{
				NamespaceId: namespaceId,
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     taskQueueName,
					TaskQueueType: taskQueueType,
					PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(partitionId)},
				},
			})
			if err != nil {
				pm.logger.Error("Failed to force load non-root partition after root partition was loaded",
					tag.Error(err))
				return
			}

			if !resp.WasUnloaded {
				// For the typical TaskQueue with 4 partitions, there is a 1/4 chance
				// that a poller is load balanced to the root partition first.
				// This metric will be used in conjunction with the one called earlier in the function
				// To identify if we are making excessive/unnecessary/wasteful RPCs.
				pm.metricsHandler.Counter(metrics.ForceLoadedTaskQueuePartitionUnnecessarilyCounter.Name()).Record(1)
			}

		}()
	}
}

func (pm *taskQueuePartitionManagerImpl) unloadPhysicalQueue(unloadedDbq physicalTaskQueueManager, unloadCause unloadCause) {
	version := unloadedDbq.QueueKey().Version()

	if !version.IsVersioned() {
		dbq := pm.defaultQueue()
		// this is the default queue, unload the whole partition if it is not healthy
		if dbq != nil && dbq == unloadedDbq {
			pm.unloadFromEngine(unloadCause)
		}
		return
	}

	pm.versionedQueuesLock.Lock()
	foundDbq, ok := pm.versionedQueues[version]
	if !ok || foundDbq != unloadedDbq {
		pm.versionedQueuesLock.Unlock()
		unloadedDbq.Stop(unloadCause)
		return
	}
	delete(pm.versionedQueues, version)
	pm.versionedQueuesLock.Unlock()
	unloadedDbq.Stop(unloadCause)
}

func (pm *taskQueuePartitionManagerImpl) unloadFromEngine(unloadCause unloadCause) {
	pm.engine.unloadTaskQueuePartition(pm, unloadCause)
}

func (pm *taskQueuePartitionManagerImpl) getPhysicalQueue(ctx context.Context, buildId string, deployment *deploymentpb.Deployment) (physicalTaskQueueManager, error) {
	if buildId == "" {
		dbq := pm.defaultQueue()
		if dbq == nil {
			return nil, errDefaultQueueNotInit
		}
		return dbq, nil
	}

	return pm.getVersionedQueue(ctx, "", buildId, deployment, true)
}

// Pass either versionSet or build ID
func (pm *taskQueuePartitionManagerImpl) getVersionedQueue(
	ctx context.Context,
	versionSet string,
	buildId string,
	deployment *deploymentpb.Deployment,
	create bool,
) (physicalTaskQueueManager, error) {
	if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, serviceerror.NewInternal("versioned queues can't be used in sticky partitions")
	}
	if versionSet == "" && buildId == "" && deployment == nil {
		return nil, serviceerror.NewInternal("deployment or build ID or version set should be given for a versioned queue")
	}
	tqm, err := pm.getVersionedQueueNoWait(versionSet, buildId, deployment, create)
	if err != nil || tqm == nil {
		return nil, err
	}
	if err = tqm.WaitUntilInitialized(ctx); err != nil {
		return nil, err
	}
	return tqm, nil
}

// Returns physicalTaskQueueManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This does not block for the task queue to be
// initialized.
// Pass either versionSet or build ID
func (pm *taskQueuePartitionManagerImpl) getVersionedQueueNoWait(
	versionSet string,
	buildId string,
	deployment *deploymentpb.Deployment,
	create bool,
) (physicalTaskQueueManager, error) {
	key := PhysicalTaskQueueVersion{
		versionSet: versionSet,
		buildId:    buildId,
	}
	if deployment != nil {
		key.deploymentSeriesName = deployment.GetSeriesName()
		key.buildId = deployment.GetBuildId()
	}

	pm.versionedQueuesLock.RLock()
	vq, ok := pm.versionedQueues[key]
	pm.versionedQueuesLock.RUnlock()
	if !ok {
		if !create {
			return nil, nil
		}

		// If it gets here, write lock and check again in case a task queue is created between the two locks
		pm.versionedQueuesLock.Lock()
		vq, ok = pm.versionedQueues[key]
		if !ok {
			var err error
			var dbq *PhysicalTaskQueueKey
			if deployment != nil {
				dbq = DeploymentQueueKey(pm.partition, deployment)
			} else if buildId != "" {
				dbq = BuildIdQueueKey(pm.partition, buildId)
			} else {
				dbq = VersionSetQueueKey(pm.partition, versionSet)
			}
			vq, err = newPhysicalTaskQueueManager(pm, dbq)
			if err != nil {
				pm.versionedQueuesLock.Unlock()
				return nil, err
			}
			pm.versionedQueues[key] = vq
		}
		pm.versionedQueuesLock.Unlock()

		if !ok {
			vq.Start()
		}
	}
	return vq, nil
}

func (pm *taskQueuePartitionManagerImpl) getVersionSetForPoll(
	caps *commonpb.WorkerVersionCapabilities,
	versioningData *persistencespb.VersioningData,
) (string, error) {
	primarySetId, demotedSetIds, unknownBuild, err := lookupVersionSetForPoll(versioningData, caps)
	if err != nil {
		return "", err
	}
	if unknownBuild {
		// if the build ID is unknown, we assume user is using the new API
		return "", nil
	}
	pm.loadDemotedSetIds(demotedSetIds)

	return primarySetId, nil
}

func (pm *taskQueuePartitionManagerImpl) loadDemotedSetIds(demotedSetIds []string) {
	// If we have demoted set ids, we need to load all task queues for them because even though
	// no new tasks will be sent to them, they might have old tasks in the db.
	// Also mark them alive, so that their liveness will be roughly synchronized.
	// TODO: once we know a demoted set id has no more tasks, we can remove it from versioning data
	for _, demotedSetId := range demotedSetIds {
		tqm, _ := pm.getVersionedQueueNoWait(demotedSetId, "", nil, true)
		if tqm != nil {
			tqm.MarkAlive()
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) loadUpstreamBuildIds(
	targetBuildId string,
	activeRules []*persistencespb.RedirectRule,
) {
	// We need to load all physical queues for upstream build IDs because even though
	// no new tasks will be sent to them, they might have old tasks in the db.
	// Also mark them alive, so that their liveness will be roughly synchronized.
	// TODO: make this more efficient so it does not MarkAlive on every poll
	for _, buildId := range getUpstreamBuildIds(targetBuildId, activeRules) {
		tqm, _ := pm.getVersionedQueueNoWait("", buildId, nil, true)
		if tqm != nil {
			tqm.MarkAlive()
		}
	}
}

// getPhysicalQueuesForAdd returns two physical queues, first one for spooling the task, second one for sync-match
// spoolQueue will be nil iff the task is forwarded (forwardInfo is nil).
//
//nolint:revive // cognitive complexity will be reduced once old versioning is deleted [cleanup-old-wv]
func (pm *taskQueuePartitionManagerImpl) getPhysicalQueuesForAdd(
	ctx context.Context,
	directive *taskqueuespb.TaskVersionDirective,
	forwardInfo *taskqueuespb.TaskForwardInfo,
	runId string,
	workflowId string,
	isQuery bool,
) (
	spoolQueue physicalTaskQueueManager,
	syncMatchQueue physicalTaskQueueManager,
	userDataChanged <-chan struct{},
	rcRevisionNumber int64,
	targetVersion *deploymentspb.WorkerDeploymentVersion,
	err error,
) {
	// Note: Revision number mechanics are only involved if the dynamic config, UseRevisionNumberForWorkerVersioning, is enabled.
	// Represents the revision number used by the task and is max(taskDirectiveRevisionNumber, routingConfigRevisionNumber) for the task.
	var taskDispatchRevisionNumber, targetDeploymentRevisionNumber int64

	wfBehavior := directive.GetBehavior()
	deployment := worker_versioning.DirectiveDeployment(directive)

	perTypeUserData, userDataChanged, err := pm.getPerTypeUserData()
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}
	deploymentData := perTypeUserData.GetDeploymentData()
	taskDirectiveRevisionNumber := directive.GetRevisionNumber()

	dbq := pm.defaultQueue()
	if dbq == nil {
		return nil, nil, nil, 0, nil, errDefaultQueueNotInit
	}

	current, currentRevisionNumber, _, ramping, _, rampingPercentage, rampingRevisionNumber, _ := worker_versioning.CalculateTaskQueueVersioningInfo(deploymentData)
	targetDeploymentVersion, targetDeploymentRevisionNumber := worker_versioning.FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(current, currentRevisionNumber, ramping, rampingPercentage, rampingRevisionNumber, workflowId)
	targetDeployment := worker_versioning.DeploymentFromDeploymentVersion(targetDeploymentVersion)

	if wfBehavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// TODO (shahab): we can verify the passed deployment matches the last poller's deployment
			return dbq, dbq, userDataChanged, 0, targetDeploymentVersion, nil
		}

		err = worker_versioning.ValidateDeployment(deployment)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}

		// Preventing Query tasks from being dispatched to a drained version with no workers
		if isQuery {
			if err := pm.checkQueryBlackholed(deploymentData, deployment); err != nil {
				return nil, nil, nil, 0, nil, err
			}
		}

		// We ignore the pinned directive if this is an activity task but the activity task queue is
		// not present in the workflow's pinned deployment. Such activities are considered
		// independent activities and are treated as unpinned, sent to their TQ's current deployment.

		var isIndependentPinnedActivity bool
		if pm.partition.TaskType() == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			// We need to check both the deployment data formats to be sure if we can ignore the pinned directive on the activity task.
			if !worker_versioning.HasDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(deployment)) {
				isIndependentPinnedActivity = true
			}
		}

		if !isIndependentPinnedActivity {
			pinnedQueue, err := pm.getVersionedQueue(ctx, "", "", deployment, true)
			if err != nil {
				return nil, nil, nil, 0, nil, err // TODO (Shivam): Please add the comment in the proto to explain that pinned tasks and sticky tasks get 0 for the rev number.
			}
			if forwardInfo == nil {
				// Task is not forwarded, so it can be spooled if sync match fails.
				// Spool queue and sync match queue is the same for pinned workflows.
				return pinnedQueue, pinnedQueue, userDataChanged, 0, targetDeploymentVersion, nil
			} else {
				// Forwarded from child partition - only do sync match.
				return nil, pinnedQueue, userDataChanged, 0, targetDeploymentVersion, nil
			}
		}
	}

	var targetDeploymentQueue physicalTaskQueueManager
	if directive.GetAssignedBuildId() == "" && targetDeployment != nil {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			if !deployment.Equal(targetDeployment) {
				// Current deployment has changed, so the workflow should move to a normal queue to
				// get redirected to the new deployment.
				return nil, nil, nil, 0, nil, serviceerrors.NewStickyWorkerUnavailable()
			}

			// TODO (shahab): we can verify the passed deployment matches the last poller's deployment
			return dbq, dbq, userDataChanged, 0, targetDeploymentVersion, nil
		}

		var err error
		targetDeploymentQueue, taskDispatchRevisionNumber, err = pm.chooseTargetQueueByFlag(
			ctx, deployment, targetDeployment, targetDeploymentRevisionNumber, taskDirectiveRevisionNumber,
		)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
		targetDeploymentVersion = worker_versioning.DeploymentVersionFromDeployment(targetDeploymentQueue.QueueKey().Version().Deployment())

		if forwardInfo == nil {
			// Task is not forwarded, so it can be spooled if sync match fails.
			// Unpinned tasks are spooled in default queue
			return dbq, targetDeploymentQueue, userDataChanged, taskDispatchRevisionNumber, targetDeploymentVersion, err
		} else {
			// Forwarded from child partition - only do sync match.
			return nil, targetDeploymentQueue, userDataChanged, taskDispatchRevisionNumber, targetDeploymentVersion, err
		}
	}

	if forwardInfo != nil {
		// Forwarded from child partition - only do sync match.
		// No need to calculate build ID, just dispatch based on source partition's instructions.
		if forwardInfo.DispatchVersionSet == "" && forwardInfo.DispatchBuildId == "" {
			syncMatchQueue = dbq
		} else {
			syncMatchQueue, err = pm.getVersionedQueue(
				ctx,
				forwardInfo.DispatchVersionSet,
				forwardInfo.DispatchBuildId,
				nil,
				true,
			)
		}
		return nil, syncMatchQueue, nil, taskDispatchRevisionNumber, targetDeploymentVersion, err
	}

	if directive.GetBuildId() == nil {
		// The task belongs to an unversioned execution. Keep using unversioned. But also return
		// userDataChanged so if current deployment is set, the task redirects to that deployment.
		return dbq, dbq, userDataChanged, taskDispatchRevisionNumber, targetDeploymentVersion, nil
	}

	userData, userDataChanged, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, nil, nil, 0, nil, err
	}

	data := userData.GetData().GetVersioningData()

	var buildId, redirectBuildId string
	var versionSet string
	switch dir := directive.GetBuildId().(type) {
	case *taskqueuespb.TaskVersionDirective_UseAssignmentRules:
		// Need to assign build ID. Assignment rules take precedence, fallback to version sets if no matching rule is found
		if len(data.GetAssignmentRules()) > 0 {
			buildId = FindAssignmentBuildId(data.GetAssignmentRules(), runId)
		}
		if buildId == "" {
			versionSet, err = pm.getVersionSetForAdd(directive, data)
			if err != nil {
				return nil, nil, nil, 0, nil, err
			}
		}
	case *taskqueuespb.TaskVersionDirective_AssignedBuildId:
		// Already assigned, need to stay in the same version set or build ID. If TQ has version sets, first try to
		// redirect to the version set based on the build ID. If the build ID does not belong to any version set,
		// assume user wants to use the new API
		if len(data.GetVersionSets()) > 0 {
			versionSet, err = pm.getVersionSetForAdd(directive, data)
			if err != nil {
				return nil, nil, nil, 0, nil, err
			}
		}
		if versionSet == "" {
			buildId = dir.AssignedBuildId
		}
	}

	redirectBuildId = FindRedirectBuildId(buildId, data.GetRedirectRules())

	if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
		// We may kick off this worker if there's a new default build ID in the version set.
		// unknownBuild flag is ignored because we don't have any special logic for it anymore. unknown build can
		// happen in two scenarios:
		// - task queue is switching to the new versioning API and the build ID is not registered in version sets.
		// - task queue is still using the old API but a failover happened before verisoning data fully propagate.
		// the second case is unlikely, and we do not support it anymore considering the old API is deprecated.
		// TODO: [cleanup-old-wv]
		_, err = checkVersionForStickyAdd(data, directive.GetAssignedBuildId())
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
		if buildId != redirectBuildId {
			// redirect rule added for buildId, kick task back to normal queue
			// TODO (shahab): support V3 in here
			return nil, nil, nil, 0, nil, serviceerrors.NewStickyWorkerUnavailable()
		}
		// sticky queues only use default queue
		return dbq, dbq, userDataChanged, 0, targetDeploymentVersion, nil
	}

	if versionSet != "" {
		spoolQueue = pm.defaultQueue()
		if spoolQueue == nil {
			return nil, nil, nil, 0, nil, errDefaultQueueNotInit
		}
		syncMatchQueue, err = pm.getVersionedQueue(ctx, versionSet, "", nil, true)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
	} else {
		syncMatchQueue, err = pm.getPhysicalQueue(ctx, redirectBuildId, nil)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
		// redirect rules are not applied when spooling a task. They'll be applied when dispatching the spool task.
		spoolQueue, err = pm.getPhysicalQueue(ctx, buildId, nil)
		if err != nil {
			return nil, nil, nil, 0, nil, err
		}
	}

	return spoolQueue, syncMatchQueue, userDataChanged, taskDispatchRevisionNumber, targetDeploymentVersion, err
}

// chooseTargetQueueByFlag picks the target queue and dispatch revision number.
// If UseRevisionNumberForWorkerVersioning is enabled, it uses the revision
// comparison; otherwise it always chooses targetDeployment.
func (pm *taskQueuePartitionManagerImpl) chooseTargetQueueByFlag(
	ctx context.Context,
	taskDeployment *deploymentpb.Deployment,
	targetDeployment *deploymentpb.Deployment,
	targetDeploymentRevisionNumber int64,
	taskDirectiveRevisionNumber int64,
) (physicalTaskQueueManager, int64, error) {

	if pm.engine != nil && pm.engine.config.UseRevisionNumberForWorkerVersioning(pm.Namespace().Name().String()) {
		if targetDeployment.GetSeriesName() != taskDeployment.GetSeriesName() || targetDeploymentRevisionNumber >= taskDirectiveRevisionNumber {
			q, err := pm.getVersionedQueue(ctx, "", "", targetDeployment, true)
			return q, targetDeploymentRevisionNumber, err
		}
		q, err := pm.getVersionedQueue(ctx, "", "", taskDeployment, true)
		return q, taskDirectiveRevisionNumber, err
	}

	// When not using revision number mechanics, always choose the targetDeployment.
	q, err := pm.getVersionedQueue(ctx, "", "", targetDeployment, true)
	return q, targetDeploymentRevisionNumber, err
}

func (pm *taskQueuePartitionManagerImpl) getVersionSetForAdd(directive *taskqueuespb.TaskVersionDirective, data *persistencespb.VersioningData) (string, error) {
	var buildId string
	switch dir := directive.GetBuildId().(type) {
	case *taskqueuespb.TaskVersionDirective_UseAssignmentRules:
		// leave buildId = "", lookupVersionSetForAdd understands that to mean "default"
	case *taskqueuespb.TaskVersionDirective_AssignedBuildId:
		buildId = dir.AssignedBuildId
	default:
		// Unversioned task, leave on unversioned queue.
		return "", nil
	}

	versionSet, unknownBuild, err := lookupVersionSetForAdd(data, buildId)
	if errors.Is(err, errEmptyVersioningData) {
		// default was requested for an unversioned queue
		return "", nil
	} else if err != nil {
		return "", err
	}
	if unknownBuild {
		// this could happen in two scenarios:
		// - task queue is switching to the new versioning API and the build ID is not registered in version sets.
		// - task queue is still using the old API but a failover happened before verisoning data fully propagate.
		// the second case is unlikely, and we do not support it anymore considering the old API is deprecated.
		return "", nil
	}

	return versionSet, nil
}

func (pm *taskQueuePartitionManagerImpl) recordUnknownBuildPoll(buildId string) {
	pm.logger.Warn("unknown build ID in poll", tag.BuildId(buildId))
	pm.metricsHandler.Counter(metrics.UnknownBuildPollsCounter.Name()).Record(1)
}

func (pm *taskQueuePartitionManagerImpl) recordUnknownBuildTask(buildId string) {
	pm.logger.Warn("unknown build ID in task", tag.BuildId(buildId))
	pm.metricsHandler.Counter(metrics.UnknownBuildTasksCounter.Name()).Record(1)
}

//nolint:staticcheck // SA1019: [cleanup-wv-3.1]
func (pm *taskQueuePartitionManagerImpl) checkQueryBlackholed(
	deploymentData *persistencespb.DeploymentData,
	deployment *deploymentpb.Deployment,
) error {
	// Check old format
	for _, versionData := range deploymentData.GetVersions() {
		if versionData.GetVersion() != nil && worker_versioning.DeploymentVersionFromDeployment(deployment).Equal(versionData.GetVersion()) {
			if versionData.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED && len(pm.GetAllPollerInfo()) == 0 {
				versionStr := worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment))
				return serviceerror.NewFailedPreconditionf(ErrBlackholedQuery, versionStr, versionStr)
			}
		}
	}

	// Check new format
	if workerDeploymentData, ok := deploymentData.GetDeploymentsData()[deployment.GetSeriesName()]; ok {
		if workerDeploymentData.GetVersions() != nil && workerDeploymentData.GetVersions()[deployment.GetBuildId()] != nil &&
			workerDeploymentData.GetVersions()[deployment.GetBuildId()].GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED && len(pm.GetAllPollerInfo()) == 0 {
			return serviceerror.NewFailedPreconditionf(ErrBlackholedQuery, deployment.GetBuildId(), deployment.GetBuildId())
		}
	}
	return nil
}

func (pm *taskQueuePartitionManagerImpl) getPerTypeUserData() (*persistencespb.TaskQueueTypeUserData, <-chan struct{}, error) {
	userData, userDataChanged, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, nil, err
	}
	perType := userData.GetData().GetPerType()[int32(pm.Partition().TaskType())]
	return perType, userDataChanged, nil
}

func (pm *taskQueuePartitionManagerImpl) userDataChanged(to *persistencespb.VersionedTaskQueueUserData) {
	// Update rateLimits if any change is userData.
	pm.rateLimitManager.UserDataChanged()

	// Do not use defaultQueue() because that treats
	// not being ready as an error, which is expected during bringup here.
	defaultQ, err := pm.defaultQueueFuture.GetIfReady()
	// Initialization error or not ready yet
	if err != nil {
		return
	}

	taskType := int32(pm.Partition().TaskType())
	if to.GetData().GetPerType()[taskType].GetFairnessState() != pm.fairnessState {
		pm.logger.Debug("unloading partitionManager due to change in FairnessState")
		pm.unloadFromEngine(unloadCauseConfigChange)
		return
	}

	// Notify all queues so they can re-evaluate their backlog.
	pm.versionedQueuesLock.RLock()
	for _, vq := range pm.versionedQueues {
		go vq.UserDataChanged()
	}
	pm.versionedQueuesLock.RUnlock()

	// Do this one in this goroutine.
	defaultQ.UserDataChanged()
}
