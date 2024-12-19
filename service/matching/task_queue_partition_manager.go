// The MIT License
//
// Copyright (pm) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (pm) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"context"
	"errors"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
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
		// this is the default (unversioned) DB queue. As of now, some of the matters related to the whole TQ partition
		// is delegated to the defaultQueue.
		defaultQueue physicalTaskQueueManager
		// used for non-sticky versioned queues (one for each version)
		versionedQueues                 map[PhysicalTaskQueueVersion]physicalTaskQueueManager
		versionedQueuesLock             sync.RWMutex // locks mutation of versionedQueues
		userDataManager                 userDataManager
		logger                          log.Logger
		throttledLogger                 log.ThrottledLogger
		matchingClient                  matchingservice.MatchingServiceClient
		metricsHandler                  metrics.Handler                                                          // namespace/taskqueue tagged metric scope
		cachedPhysicalInfoByBuildId     map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo // non-nil for root-partition
		cachedPhysicalInfoByBuildIdLock sync.RWMutex                                                             // locks mutation of cachedPhysicalInfoByBuildId
		lastFanOut                      int64                                                                    // serves as a TTL for cachedPhysicalInfoByBuildId
	}
)

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
	pm := &taskQueuePartitionManagerImpl{
		engine:                      e,
		partition:                   partition,
		ns:                          ns,
		config:                      tqConfig,
		logger:                      logger,
		throttledLogger:             throttledLogger,
		matchingClient:              e.matchingRawClient,
		metricsHandler:              metricsHandler,
		versionedQueues:             make(map[PhysicalTaskQueueVersion]physicalTaskQueueManager),
		userDataManager:             userDataManager,
		cachedPhysicalInfoByBuildId: nil,
	}

	defaultQ, err := newPhysicalTaskQueueManager(pm, UnversionedQueueKey(partition))
	if err != nil {
		return nil, err
	}
	pm.defaultQueue = defaultQ
	return pm, nil
}

func (pm *taskQueuePartitionManagerImpl) Start() {
	pm.engine.updateTaskQueuePartitionGauge(pm, 1)
	pm.userDataManager.Start()
	pm.defaultQueue.Start()
}

// Stop does not unload the partition from matching engine. It is intended to be called by matching engine when
// unloading the partition. For stopping and unloading a partition call unloadFromEngine instead.
func (pm *taskQueuePartitionManagerImpl) Stop(unloadCause unloadCause) {
	pm.versionedQueuesLock.Lock()
	defer pm.versionedQueuesLock.Unlock()
	for _, vq := range pm.versionedQueues {
		vq.Stop(unloadCause)
	}
	pm.defaultQueue.Stop(unloadCause)
	pm.userDataManager.Stop()
	pm.engine.updateTaskQueuePartitionGauge(pm, -1)
}

func (pm *taskQueuePartitionManagerImpl) Namespace() *namespace.Namespace {
	return pm.ns
}

func (pm *taskQueuePartitionManagerImpl) MarkAlive() {
	pm.defaultQueue.MarkAlive()
}

func (pm *taskQueuePartitionManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	err := pm.userDataManager.WaitUntilInitialized(ctx)
	if err != nil {
		return err
	}
	return pm.defaultQueue.WaitUntilInitialized(ctx)
}

func (pm *taskQueuePartitionManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (buildId string, syncMatched bool, err error) {
	var spoolQueue, syncMatchQueue physicalTaskQueueManager
	directive := params.taskInfo.GetVersionDirective()
	// spoolQueue will be nil iff task is forwarded.
	spoolQueue, syncMatchQueue, _, err = pm.getPhysicalQueuesForAdd(ctx, directive, params.forwardInfo, params.taskInfo.GetRunId())
	if err != nil {
		return "", false, err
	}

	syncMatchTask := newInternalTaskForSyncMatch(params.taskInfo, params.forwardInfo)
	if spoolQueue != nil && spoolQueue.QueueKey().Version().BuildId() != syncMatchQueue.QueueKey().Version().BuildId() {
		// Task is not forwarded and build ID is different on the two queues -> redirect rule is being applied.
		// Set redirectInfo in the task as it will be needed if we have to forward the task.
		syncMatchTask.redirectInfo = &taskqueuespb.BuildIdRedirectInfo{
			AssignedBuildId: spoolQueue.QueueKey().Version().BuildId(),
		}
	}

	if pm.defaultQueue != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
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
		}
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
	dbq := pm.defaultQueue
	versionSetUsed := false
	deployment := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities)

	if deployment != nil {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// TODO: reject poller of old sticky queue if newer version exist
		} else {
			// default queue should stay alive even if requests go to other queues
			pm.defaultQueue.MarkAlive()
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
			pm.defaultQueue.MarkAlive()

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

	task, err := dbq.PollTask(ctx, pollMetadata)
	return task, versionSetUsed, err
}

func (pm *taskQueuePartitionManagerImpl) ProcessSpooledTask(
	ctx context.Context,
	task *internalTask,
	assignedBuildId string,
) error {
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	directive := taskInfo.GetVersionDirective()
	if assignedBuildId != "" {
		// construct directive based on the build ID of the spool queue
		directive = worker_versioning.MakeBuildIdDirective(assignedBuildId)
	}
	// Redirect and re-resolve if we're blocked in matcher and user data changes.
	for {
		_, syncMatchQueue, userDataChanged, err := pm.getPhysicalQueuesForAdd(
			ctx,
			directive,
			nil,
			taskInfo.GetRunId(),
		)
		if err != nil {
			return err
		}
		// set redirect info if spoolQueue and syncMatchQueue build ids are different
		if assignedBuildId != syncMatchQueue.QueueKey().Version().BuildId() {
			task.redirectInfo = &taskqueuespb.BuildIdRedirectInfo{
				AssignedBuildId: assignedBuildId,
			}
		} else {
			// make sure to reset redirectInfo in case it was set in a previous loop cycle
			task.redirectInfo = nil
		}
		err = syncMatchQueue.DispatchSpooledTask(ctx, task, userDataChanged)
		if err != errInterrupted {
			return err
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	_, syncMatchQueue, _, err := pm.getPhysicalQueuesForAdd(
		ctx,
		request.VersionDirective,
		// We do not pass forwardInfo because we want the parent partition to make fresh versioning decision. Note that
		// forwarded Query/Nexus task requests do not expire rapidly in contrast to forwarded activity/workflow tasks
		// that only try up to 200ms sync-match. Therefore, to prevent blocking the request on the wrong build ID, its
		// more important to allow the parent partition to make a fresh versioning decision in case the child partition
		// did not have up-to-date User Data when selected a dispatch build ID.
		nil,
		request.GetQueryRequest().GetExecution().GetRunId(),
	)
	if err != nil {
		return nil, err
	}

	if pm.defaultQueue != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	return syncMatchQueue.DispatchQueryTask(ctx, taskID, request)
}

func (pm *taskQueuePartitionManagerImpl) DispatchNexusTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.DispatchNexusTaskRequest,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	_, syncMatchQueue, _, err := pm.getPhysicalQueuesForAdd(
		ctx,
		worker_versioning.MakeUseAssignmentRulesDirective(),
		// We do not pass forwardInfo because we want the parent partition to make fresh versioning decision. Note that
		// forwarded Query/Nexus task requests do not expire rapidly in contrast to forwarded activity/workflow tasks
		// that only try up to 200ms sync-match. Therefore, to prevent blocking the request on the wrong build ID, its
		// more important to allow the parent partition to make a fresh versioning decision in case the child partition
		// did not have up-to-date User Data when selected a dispatch build ID.
		nil,
		"",
	)
	if err != nil {
		return nil, err
	}

	if pm.defaultQueue != syncMatchQueue {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	return syncMatchQueue.DispatchNexusTask(ctx, taskId, request)
}

func (pm *taskQueuePartitionManagerImpl) GetUserDataManager() userDataManager {
	return pm.userDataManager
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (pm *taskQueuePartitionManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	ret := pm.defaultQueue.GetAllPollerInfo()
	pm.versionedQueuesLock.RLock()
	defer pm.versionedQueuesLock.RUnlock()
	for _, vq := range pm.versionedQueues {
		info := vq.GetAllPollerInfo()
		ret = append(ret, info...)
	}
	return ret
}

func (pm *taskQueuePartitionManagerImpl) HasAnyPollerAfter(accessTime time.Time) bool {
	if pm.defaultQueue.HasPollerAfter(accessTime) {
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
		return pm.defaultQueue.HasPollerAfter(accessTime)
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

func (pm *taskQueuePartitionManagerImpl) LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	resp := &matchingservice.DescribeTaskQueueResponse{
		DescResponse: &workflowservice.DescribeTaskQueueResponse{
			Pollers: pm.GetAllPollerInfo(),
		},
	}
	if includeTaskQueueStatus {
		resp.DescResponse.TaskQueueStatus = pm.defaultQueue.LegacyDescribeTaskQueue(true).DescResponse.TaskQueueStatus
	}
	return resp
}

func (pm *taskQueuePartitionManagerImpl) Describe(
	ctx context.Context,
	buildIds map[string]bool,
	includeAllActive, reportStats, reportPollers, internalTaskQueueStatus bool) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	pm.versionedQueuesLock.RLock()
	// Active means that the physical queue for that version is loaded.
	// An empty string refers to the unversioned queue, which is always loaded.
	// In the future, active will mean that the physical queue for that version has had a task added recently or a recent poller.
	if includeAllActive {
		for k := range pm.versionedQueues {
			// TODO: add deployment info to DescribeTaskQueue
			if b := k.BuildId(); b != "" {
				buildIds[b] = true
			}
		}
	}
	pm.versionedQueuesLock.RUnlock()

	versionsInfo := make(map[string]*taskqueuespb.TaskQueueVersionInfoInternal, 0)
	for bid := range buildIds {
		vInfo := &taskqueuespb.TaskQueueVersionInfoInternal{
			PhysicalTaskQueueInfo: &taskqueuespb.PhysicalTaskQueueInfo{},
		}
		physicalQueue, err := pm.getPhysicalQueue(ctx, bid)
		if err != nil {
			return nil, err
		}
		if reportPollers {
			vInfo.PhysicalTaskQueueInfo.Pollers = physicalQueue.GetAllPollerInfo()
		}
		if reportStats {
			vInfo.PhysicalTaskQueueInfo.TaskQueueStats = physicalQueue.GetStats()
		}
		if internalTaskQueueStatus {
			vInfo.PhysicalTaskQueueInfo.InternalTaskQueueStatus = physicalQueue.GetInternalTaskQueueStatus()
		}
		versionsInfo[bid] = vInfo
	}

	return &matchingservice.DescribeTaskQueuePartitionResponse{
		VersionsInfoInternal: versionsInfo,
	}, nil
}

func (pm *taskQueuePartitionManagerImpl) String() string {
	return pm.defaultQueue.String()
}

func (pm *taskQueuePartitionManagerImpl) Partition() tqid.Partition {
	return pm.partition
}

func (pm *taskQueuePartitionManagerImpl) LongPollExpirationInterval() time.Duration {
	return pm.config.LongPollExpirationInterval()
}

func (pm *taskQueuePartitionManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(pm.ns.Name().String()))
}

// ForceLoadAllNonRootPartitions spins off go routines which make RPC calls to all the
func (pm *taskQueuePartitionManagerImpl) ForceLoadAllNonRootPartitions() {
	if !pm.partition.IsRoot() {
		pm.logger.Info("ForceLoadAllNonRootPartitions called on non-root partition. Prevented circular keep alive (loading) of partitions.")
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

func (pm *taskQueuePartitionManagerImpl) TimeSinceLastFanOut() time.Duration {
	pm.cachedPhysicalInfoByBuildIdLock.RLock()
	defer pm.cachedPhysicalInfoByBuildIdLock.RUnlock()

	return time.Since(time.Unix(0, pm.lastFanOut))
}

func (pm *taskQueuePartitionManagerImpl) UpdateTimeSinceLastFanOutAndCache(physicalInfoByBuildId map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo) {
	pm.cachedPhysicalInfoByBuildIdLock.Lock()
	defer pm.cachedPhysicalInfoByBuildIdLock.Unlock()

	pm.lastFanOut = time.Now().UnixNano()
	pm.cachedPhysicalInfoByBuildId = physicalInfoByBuildId
}

func (pm *taskQueuePartitionManagerImpl) GetPhysicalTaskQueueInfoFromCache() map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo {
	pm.cachedPhysicalInfoByBuildIdLock.RLock()
	defer pm.cachedPhysicalInfoByBuildIdLock.RUnlock()

	return pm.cachedPhysicalInfoByBuildId
}

func (pm *taskQueuePartitionManagerImpl) unloadPhysicalQueue(unloadedDbq physicalTaskQueueManager, unloadCause unloadCause) {
	version := unloadedDbq.QueueKey().Version()

	if !version.IsVersioned() {
		// this is the default queue, unload the whole partition if it is not healthy
		if pm.defaultQueue == unloadedDbq {
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

func (pm *taskQueuePartitionManagerImpl) getPhysicalQueue(ctx context.Context, buildId string) (physicalTaskQueueManager, error) {
	if buildId == "" {
		return pm.defaultQueue, nil
	}
	return pm.getVersionedQueue(ctx, "", buildId, nil, true)
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
) (pinnedQueue physicalTaskQueueManager, syncMatchQueue physicalTaskQueueManager, userDataChanged <-chan struct{}, err error) {
	wfBehavior := directive.GetBehavior()
	deployment := directive.GetDeployment()

	if wfBehavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// TODO (shahab): we can verify the passed deployment matches the last poller's deployment
			return pm.defaultQueue, pm.defaultQueue, userDataChanged, nil
		}

		err = worker_versioning.ValidateDeployment(deployment)
		if err != nil {
			return nil, nil, nil, err
		}
		pinnedQueue, err = pm.getVersionedQueue(ctx, "", "", deployment, true)
		if err != nil {
			return nil, nil, nil, err
		}
		if forwardInfo == nil {
			// Task is not forwarded, so it can be spooled if sync match fails.
			// Spool queue and sync match queue is the same for pinned workflows.
			return pinnedQueue, pinnedQueue, nil, nil
		} else {
			// Forwarded from child partition - only do sync match.
			return nil, pinnedQueue, nil, nil
		}
	}

	perTypeUserData, perTypeUserDataChanged, err := pm.getPerTypeUserData()
	if err != nil {
		return nil, nil, nil, err
	}

	currentDeployment := worker_versioning.FindCurrentDeployment(perTypeUserData.GetDeploymentData())
	if currentDeployment != nil &&
		// Make sure the wf is not v1-2 versioned
		directive.GetAssignedBuildId() == "" {
		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			if !deployment.Equal(currentDeployment) {
				// Current deployment has changed, so the workflow should move to a normal queue to
				// get redirected to the new deployment.
				return nil, nil, nil, serviceerrors.NewStickyWorkerUnavailable()
			}

			// TODO (shahab): we can verify the passed deployment matches the last poller's deployment
			return pm.defaultQueue, pm.defaultQueue, perTypeUserDataChanged, nil
		}

		currentDeploymentQueue, err := pm.getVersionedQueue(ctx, "", "", currentDeployment, true)
		if forwardInfo == nil {
			// Task is not forwarded, so it can be spooled if sync match fails.
			// Unpinned tasks are spooled in default queue
			return pm.defaultQueue, currentDeploymentQueue, perTypeUserDataChanged, err
		} else {
			// Forwarded from child partition - only do sync match.
			return nil, currentDeploymentQueue, perTypeUserDataChanged, err
		}
	}

	if forwardInfo != nil {
		// Forwarded from child partition - only do sync match.
		// No need to calculate build ID, just dispatch based on source partition's instructions.
		if forwardInfo.DispatchVersionSet == "" && forwardInfo.DispatchBuildId == "" {
			syncMatchQueue = pm.defaultQueue
		} else {
			syncMatchQueue, err = pm.getVersionedQueue(
				ctx,
				forwardInfo.DispatchVersionSet,
				forwardInfo.DispatchBuildId,
				nil,
				true,
			)
		}
		return nil, syncMatchQueue, nil, err
	}

	userData, userDataChanged, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, nil, nil, err
	}

	if directive.GetBuildId() == nil {
		// The task belongs to an unversioned execution. Keep using unversioned. But also return
		// userDataChanged so if current deployment is set, the task redirects to that deployment.
		return pm.defaultQueue, pm.defaultQueue, userDataChanged, nil
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
				return nil, nil, nil, err
			}
		}
	case *taskqueuespb.TaskVersionDirective_AssignedBuildId:
		// Already assigned, need to stay in the same version set or build ID. If TQ has version sets, first try to
		// redirect to the version set based on the build ID. If the build ID does not belong to any version set,
		// assume user wants to use the new API
		if len(data.GetVersionSets()) > 0 {
			versionSet, err = pm.getVersionSetForAdd(directive, data)
			if err != nil {
				return nil, nil, nil, err
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
			return nil, nil, nil, err
		}
		if buildId != redirectBuildId {
			// redirect rule added for buildId, kick task back to normal queue
			// TODO (shahab): support V3 in here
			return nil, nil, nil, serviceerrors.NewStickyWorkerUnavailable()
		}
		// sticky queues only use default queue
		return pm.defaultQueue, pm.defaultQueue, userDataChanged, nil
	}

	if versionSet != "" {
		pinnedQueue = pm.defaultQueue
		syncMatchQueue, err = pm.getVersionedQueue(ctx, versionSet, "", nil, true)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		syncMatchQueue, err = pm.getPhysicalQueue(ctx, redirectBuildId)
		if err != nil {
			return nil, nil, nil, err
		}
		// redirect rules are not applied when spooling a task. They'll be applied when dispatching the spool task.
		pinnedQueue, err = pm.getPhysicalQueue(ctx, buildId)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return pinnedQueue, syncMatchQueue, userDataChanged, err
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

func (pm *taskQueuePartitionManagerImpl) getPerTypeUserData() (*persistencespb.TaskQueueTypeUserData, <-chan struct{}, error) {
	userData, userDataChanged, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, nil, err
	}
	perType := userData.GetData().GetPerType()[int32(pm.Partition().TaskType())]
	return perType, userDataChanged, nil
}
