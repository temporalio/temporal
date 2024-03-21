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
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

type (
	taskQueuePartitionManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		// Returns the build ID assigned to the task according to the assignment rules (if any),
		// and a boolean indicating if sync-match happened or not.
		AddTask(ctx context.Context, params addTaskParams) (buildId string, syncMatch bool, err error)
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, bool, error)
		// ProcessSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		ProcessSpooledTask(ctx context.Context, task *internalTask, sourceBuildId string) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		GetUserDataManager() userDataManager
		// MarkAlive updates the liveness timer to keep this partition manager alive.
		MarkAlive()
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		// HasPollerAfter checks pollers on the queue associated with the given buildId, or the unversioned queue if an empty string is given
		HasPollerAfter(buildId string, accessTime time.Time) bool
		// HasAnyPollerAfter checks pollers on all versioned and unversioned queues
		HasAnyPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		Partition() tqid.Partition
		LongPollExpirationInterval() time.Duration
	}

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
		versionedQueues      map[string]physicalTaskQueueManager
		versionedQueuesLock  sync.RWMutex // locks mutation of versionedQueues
		userDataManager      userDataManager
		logger               log.Logger
		throttledLogger      log.ThrottledLogger
		matchingClient       matchingservice.MatchingServiceClient
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
	}
)

var _ taskQueuePartitionManager = (*taskQueuePartitionManagerImpl)(nil)

func newTaskQueuePartitionManager(
	e *matchingEngineImpl,
	ns *namespace.Namespace,
	partition tqid.Partition,
	tqConfig *taskQueueConfig,
	userDataManager userDataManager,
) (*taskQueuePartitionManagerImpl, error) {
	nsName := ns.Name().String()
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName))
	throttledLogger := log.With(e.throttledLogger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName))
	taggedMetricsHandler := metrics.GetPerTaskQueueScope(
		e.metricsHandler.WithTags(
			metrics.OperationTag(metrics.MatchingTaskQueuePartitionManagerScope),
			metrics.TaskQueueTypeTag(partition.TaskType())),
		nsName,
		partition.RpcName(),
		partition.Kind(),
	)

	pm := &taskQueuePartitionManagerImpl{
		engine:               e,
		partition:            partition,
		ns:                   ns,
		config:               tqConfig,
		logger:               logger,
		throttledLogger:      throttledLogger,
		matchingClient:       e.matchingRawClient,
		taggedMetricsHandler: taggedMetricsHandler,
		versionedQueues:      make(map[string]physicalTaskQueueManager),
		userDataManager:      userDataManager,
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
func (pm *taskQueuePartitionManagerImpl) Stop() {
	pm.versionedQueuesLock.Lock()
	defer pm.versionedQueuesLock.Unlock()
	for _, vq := range pm.versionedQueues {
		vq.Stop()
	}
	pm.defaultQueue.Stop()
	pm.userDataManager.Stop()
	pm.engine.updateTaskQueuePartitionGauge(pm, -1)
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
	// We don't need the userDataChanged channel here because:
	// - if we sync match, we're done
	// - if we spool to db, we'll re-resolve when it comes out of the db
	directive := params.taskInfo.VersionDirective
	pq, _, err := pm.getPhysicalQueueForAdd(ctx, directive)
	if err != nil {
		return "", false, err
	}

	if pm.defaultQueue != pq {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	if pm.partition.IsRoot() && !pm.HasAnyPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		pm.taggedMetricsHandler.Counter(metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()).Record(1)
	}

	syncMatched, err = pq.AddTask(ctx, params)
	var assignedBuildId string
	if directive.GetUseAssignmentRules() != nil {
		// return build ID only if a new one is assigned.
		if !syncMatched {
			// return build ID only if the task is spooled. The returned build ID is used by History to update
			// mutable state (and visibility) when the first workflow task is spooled.
			// For sync-match case, History has already received the build ID in the Record*TaskStarted call.
			// By omitting the build ID from this response we help History immediately know that no MS update is needed.
			assignedBuildId = pq.QueueKey().BuildId()
		}
	}

	return assignedBuildId, syncMatched, err
}

func (pm *taskQueuePartitionManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, bool, error) {
	dbq := pm.defaultQueue
	versionSetUsed := false
	if pollMetadata.workerVersionCapabilities.GetUseVersioning() {
		userData, _, err := pm.userDataManager.GetUserData()
		if err != nil {
			return nil, false, err
		}

		versioningData := userData.GetData().GetVersioningData()

		if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
			// In the sticky case we always use the unversioned queue
			// For the old API, we may kick off this worker if there's a newer one.
			versionSetUsed, err = checkVersionForStickyPoll(versioningData, pollMetadata.workerVersionCapabilities)
		} else {
			// default queue should stay alive even if requests go to other queues
			pm.defaultQueue.MarkAlive()

			buildId := pollMetadata.workerVersionCapabilities.GetBuildId()
			if buildId == "" {
				return nil, false, serviceerror.NewInvalidArgument("build ID must be provided when using worker versioning")
			}

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
				dbq, err = pm.getVersionedQueue(ctx, versionSet, "", true)
			} else {
				dbq, err = pm.getVersionedQueue(ctx, "", buildId, true)
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
	sourceBuildId string,
) error {
	if sourceBuildId != "" {
		// Task is spooled in a build ID queue. it should be dispatched to the same build ID until
		// we add redirect rules support.
		pq, err := pm.getVersionedQueue(ctx, "", sourceBuildId, true)
		if err != nil {
			return err
		}
		return pq.DispatchSpooledTask(ctx, task, nil)
	}
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	// TODO: in WV2 we should not look at a spooled task directive anymore [cleanup-old-wv]
	directive := taskInfo.GetVersionDirective()
	// Redirect and re-resolve if we're blocked in matcher and user data changes.
	for {
		pq, userDataChanged, err := pm.getPhysicalQueueForAdd(ctx, directive)
		if err != nil {
			return err
		}
		err = pq.DispatchSpooledTask(ctx, task, userDataChanged)
		if err != errInterrupted { // nolint:goerr113
			return err
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	pq, _, err := pm.getPhysicalQueueForAdd(ctx, request.VersionDirective)
	if err != nil {
		return nil, err
	}

	if pm.defaultQueue != pq {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	return pq.DispatchQueryTask(ctx, taskID, request)
}

func (pm *taskQueuePartitionManagerImpl) GetUserDataManager() userDataManager {
	return pm.userDataManager
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (pm *taskQueuePartitionManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	ret := pm.defaultQueue.GetAllPollerInfo()
	pm.versionedQueuesLock.RLock()
	defer pm.versionedQueuesLock.RUnlock()
	for _, ptqm := range pm.versionedQueues {
		info := ptqm.GetAllPollerInfo()
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
	vq, ok := pm.versionedQueues[buildId]
	pm.versionedQueuesLock.RUnlock()
	if !ok {
		return false
	}
	return vq.HasPollerAfter(accessTime)
}

func (pm *taskQueuePartitionManagerImpl) DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	resp := &matchingservice.DescribeTaskQueueResponse{
		Pollers: pm.GetAllPollerInfo(),
	}
	if includeTaskQueueStatus {
		resp.TaskQueueStatus = pm.defaultQueue.DescribeTaskQueue(true).TaskQueueStatus
	}
	return resp
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

func (pm *taskQueuePartitionManagerImpl) unloadPhysicalQueue(unloadedDbq physicalTaskQueueManager) {
	version := unloadedDbq.QueueKey().VersionSet()
	if version == "" {
		version = unloadedDbq.QueueKey().BuildId()
	}

	if version == "" {
		// this is the default queue, unload the whole partition if it is not healthy
		if pm.defaultQueue == unloadedDbq {
			pm.unloadFromEngine()
		}
		return
	}

	pm.versionedQueuesLock.Lock()
	foundDbq, ok := pm.versionedQueues[version]
	if !ok || foundDbq != unloadedDbq {
		pm.versionedQueuesLock.Unlock()
		unloadedDbq.Stop()
		return
	}
	delete(pm.versionedQueues, version)
	pm.versionedQueuesLock.Unlock()
	unloadedDbq.Stop()

}

func (pm *taskQueuePartitionManagerImpl) unloadFromEngine() {
	pm.engine.unloadTaskQueuePartition(pm)
}

// Pass either versionSet or build ID
func (pm *taskQueuePartitionManagerImpl) getVersionedQueue(
	ctx context.Context,
	versionSet string,
	buildId string,
	create bool,
) (physicalTaskQueueManager, error) {
	if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
		return nil, serviceerror.NewInternal("versioned queues can't be used in sticky partitions")
	}
	tqm, err := pm.getVersionedQueueNoWait(versionSet, buildId, create)
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
	create bool,
) (physicalTaskQueueManager, error) {
	key := versionSet
	if buildId != "" {
		key = buildId
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
			if buildId != "" {
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
		tqm, _ := pm.getVersionedQueueNoWait(demotedSetId, "", true)
		if tqm != nil {
			tqm.MarkAlive()
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) getPhysicalQueueForAdd(
	ctx context.Context,
	directive *taskqueuespb.TaskVersionDirective,
) (physicalTaskQueueManager, <-chan struct{}, error) {
	if directive.GetBuildId() == nil {
		// This means the tasks is a middle task belonging to an unversioned execution. Keep using unversioned.
		return pm.defaultQueue, nil, nil
	}

	userData, userDataChanged, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, nil, err
	}

	data := userData.GetData().GetVersioningData()

	if pm.partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY {
		// We may kick off this worker if there's a new default build ID in the version set.
		// unknownBuild flag is ignored because we don't have any special logic for it anymore. unknown build can
		// happen in two scenarios:
		// - task queue is switching to the new versioning API and the build ID is not registered in version sets.
		// - task queue is still using the old API but a failover happened before verisoning data fully propagate.
		// the second case is unlikely, and we do not support it anymore considering the old API is deprecated.
		_, err := checkVersionForStickyAdd(data, directive.GetAssignedBuildId())
		if err != nil {
			return nil, nil, err
		}
		return pm.defaultQueue, userDataChanged, nil
	}

	var buildId string
	var versionSet string
	switch dir := directive.GetBuildId().(type) {
	case *taskqueuespb.TaskVersionDirective_UseAssignmentRules:
		// Need to assign build ID. Assignment rules take precedence, fallback to version sets if no matching rule is found
		if len(data.GetAssignmentRules()) > 0 {
			buildId = FindAssignmentBuildId(data.GetAssignmentRules())
		}
		if buildId == "" {
			versionSet, err = pm.getVersionSetForAdd(directive, data)
			if err != nil {
				return nil, nil, err
			}
		}
	case *taskqueuespb.TaskVersionDirective_AssignedBuildId:
		// Already assigned, need to stay in the same version set or build ID. If TQ has version sets, first try to
		// redirect to the version set based on the build ID. If the build ID does not belong to any version set,
		// assume user wants to use the new API
		if len(data.GetVersionSets()) > 0 {
			versionSet, err = pm.getVersionSetForAdd(directive, data)
			if err != nil {
				return nil, nil, err
			}
		}
		if versionSet == "" {
			buildId = dir.AssignedBuildId
		}
	}

	if buildId == "" && versionSet == "" {
		// We could not find a buildId for assignment, using the unversioned queue
		return pm.defaultQueue, userDataChanged, nil
	}

	dbq, err := pm.getVersionedQueue(ctx, versionSet, buildId, true)
	return dbq, userDataChanged, err
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
	if err == errEmptyVersioningData { // nolint:goerr113
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
	pm.logger.Warn("unknown build id in poll", tag.BuildId(buildId))
	pm.taggedMetricsHandler.Counter(metrics.UnknownBuildPollsCounter.Name()).Record(1)
}

func (pm *taskQueuePartitionManagerImpl) recordUnknownBuildTask(buildId string) {
	pm.logger.Warn("unknown build id in task", tag.BuildId(buildId))
	pm.taggedMetricsHandler.Counter(metrics.UnknownBuildTasksCounter.Name()).Record(1)
}
