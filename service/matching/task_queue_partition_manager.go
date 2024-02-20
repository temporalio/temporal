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
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchSpooledTask(ctx context.Context, task *internalTask) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskID string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		GetUserDataManager() *userDataManager
		MarkAlive()
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
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
		engine        *matchingEngineImpl
		partition     tqid.Partition
		namespaceName namespace.Name
		config        *taskQueueConfig
		// this is the default (unversioned) DB queue. As of now, some of the matters related to the whole TQ partition
		// is delegated to the defaultQueue.
		defaultQueue dbQueueManager
		// used for non-sticky versioned queues (one for each version)
		versionedQueues      map[string]dbQueueManager
		versionedQueuesLock  sync.RWMutex // locks mutation of versionedQueues
		userDataManager      *userDataManager
		namespaceRegistry    namespace.Registry
		logger               log.Logger
		throttledLogger      log.ThrottledLogger
		matchingClient       matchingservice.MatchingServiceClient
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
	}
)

var _ taskQueuePartitionManager = (*taskQueuePartitionManagerImpl)(nil)

func newTaskQueuePartitionManager(
	e *matchingEngineImpl,
	partition tqid.Partition,
	config *Config,
) (taskQueuePartitionManager, error) {
	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(partition.NamespaceId())
	if err != nil {
		return nil, err
	}
	nsName := namespaceEntry.Name()
	taskQueueConfig := newTaskQueueConfig(partition, config, nsName)

	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName.String()))
	throttledLogger := log.With(e.throttledLogger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName.String()))
	taggedMetricsHandler := metrics.GetPerTaskQueueScope(
		e.metricsHandler.WithTags(
			metrics.OperationTag(metrics.MatchingTaskQueuePartitionManagerScope),
			metrics.TaskQueueTypeTag(partition.TaskType())),
		nsName.String(),
		partition.RpcName(),
		partition.Kind(),
	)

	pm := &taskQueuePartitionManagerImpl{
		engine:               e,
		partition:            partition,
		config:               taskQueueConfig,
		namespaceRegistry:    e.namespaceRegistry,
		logger:               logger,
		throttledLogger:      throttledLogger,
		matchingClient:       e.matchingRawClient,
		taggedMetricsHandler: taggedMetricsHandler,
		versionedQueues:      make(map[string]dbQueueManager),
		userDataManager:      newUserDataManager(e.taskManager, e.matchingRawClient, partition, taskQueueConfig, e.logger, e.namespaceRegistry),
	}

	defaultQ, err := newTaskQueueManager(pm, UnversionedDBQueue(partition))
	if err != nil {
		return nil, err
	}

	pm.defaultQueue = defaultQ
	return pm, nil
}

func (pm *taskQueuePartitionManagerImpl) Start() {
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
}

func (pm *taskQueuePartitionManagerImpl) MarkAlive() {
	pm.defaultQueue.MarkAlive()
}

func (pm *taskQueuePartitionManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	err := pm.userDataManager.WaitUntilInitialized(ctx)
	if err != nil {
		pm.unloadFromEngine()
		return err
	}
	return pm.defaultQueue.WaitUntilInitialized(ctx)
}

func (pm *taskQueuePartitionManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (bool, error) {
	dbq, err := pm.getDbQueueForAdd(ctx, params.taskInfo.VersionDirective)
	if err != nil {
		return false, err
	}

	if pm.defaultQueue != dbq {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	if pm.partition.IsRoot() && !pm.HasPollerAfter(time.Now().Add(-noPollerThreshold)) {
		// Only checks recent pollers in the root partition
		pm.taggedMetricsHandler.Counter(metrics.NoRecentPollerTasksPerTaskQueueCounter.Name()).Record(1)
	}

	return dbq.AddTask(ctx, params)
}

func (pm *taskQueuePartitionManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	dbq := pm.defaultQueue
	if pollMetadata.workerVersionCapabilities.GetUseVersioning() {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
		// We don't need the userDataChanged channel here because:
		// - if we sync match, we're done
		// - if we spool to db, we'll re-resolve when it comes out of the db
		userData, _, err := pm.userDataManager.GetUserData()
		if err != nil {
			return nil, err
		}
		dbq, err = pm.redirectToVersionSetQueueForPoll(ctx, pollMetadata.workerVersionCapabilities, userData)
		if err != nil {
			return nil, err
		}
	}

	if identity, ok := ctx.Value(identityKey).(string); ok && identity != "" {
		pm.defaultQueue.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
		// update timestamp when long poll ends
		defer pm.defaultQueue.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
	}
	return dbq.PollTask(ctx, pollMetadata)
}

func (pm *taskQueuePartitionManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	directive := taskInfo.GetVersionDirective()
	// Redirect and re-resolve if we're blocked in matcher and user data changes.
	for {
		dbq := pm.defaultQueue
		var userDataChanged <-chan struct{}
		if directive.GetValue() != nil {
			userData, userDataCh, err := pm.userDataManager.GetUserData()
			userDataChanged = userDataCh
			if err != nil {
				return err
			}
			dbq, err = pm.redirectToVersionSetQueueForAdd(ctx, directive, userData)
			if err != nil {
				return err
			}
		}
		err := dbq.DispatchSpooledTask(ctx, task, userDataChanged)
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
	dbq, err := pm.getDbQueueForAdd(ctx, request.VersionDirective)
	if err != nil {
		return nil, err
	}

	if pm.defaultQueue != dbq {
		// default queue should stay alive even if requests go to other queues
		pm.defaultQueue.MarkAlive()
	}

	return dbq.DispatchQueryTask(ctx, taskID, request)
}

func (pm *taskQueuePartitionManagerImpl) GetUserDataManager() *userDataManager {
	return pm.userDataManager
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (pm *taskQueuePartitionManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	return pm.defaultQueue.GetAllPollerInfo()
}

func (pm *taskQueuePartitionManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	return pm.defaultQueue.HasPollerAfter(accessTime)
}

func (pm *taskQueuePartitionManagerImpl) DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	return pm.defaultQueue.DescribeTaskQueue(includeTaskQueueStatus)
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
	ns, _ := pm.namespaceRegistry.GetNamespaceName(pm.partition.NamespaceId())
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(ns.String()))
}

func (pm *taskQueuePartitionManagerImpl) unloadDbQueue(unloadedDbq dbQueueManager) {
	version := unloadedDbq.DBQueue().VersionSet()
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
	pm.engine.updateTaskQueueGauge(pm, true, -1)
}

func (pm *taskQueuePartitionManagerImpl) unloadFromEngine() {
	pm.engine.unloadTaskQueuePartition(pm)
}

func (pm *taskQueuePartitionManagerImpl) getVersionedQueue(
	ctx context.Context,
	version string,
	create bool,
) (dbQueueManager, error) {
	tqm, err := pm.getVersionedQueueNoWait(version, create)
	if err != nil || tqm == nil {
		return nil, err
	}
	if err = tqm.WaitUntilInitialized(ctx); err != nil {
		return nil, err
	}
	return tqm, nil
}

// Returns taskQueuePartitionManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This does not block for the task queue to be
// initialized.
func (pm *taskQueuePartitionManagerImpl) getVersionedQueueNoWait(
	version string,
	create bool,
) (dbQueueManager, error) {
	pm.versionedQueuesLock.RLock()
	vq, ok := pm.versionedQueues[version]
	pm.versionedQueuesLock.RUnlock()
	if !ok {
		if !create {
			return nil, nil
		}

		// If it gets here, write lock and check again in case a task queue is created between the two locks
		pm.versionedQueuesLock.Lock()
		vq, ok = pm.versionedQueues[version]
		if !ok {
			var err error
			vq, err = newTaskQueueManager(pm, VersionSetDBQueue(pm.partition, version))
			if err != nil {
				pm.versionedQueuesLock.Unlock()
				return nil, err
			}
			pm.versionedQueues[version] = vq
		}
		pm.versionedQueuesLock.Unlock()

		if !ok {
			vq.Start()
			pm.engine.updateTaskQueueGauge(pm, true, 1)
		}
	}
	return vq, nil
}

func (pm *taskQueuePartitionManagerImpl) redirectToVersionSetQueueForPoll(
	ctx context.Context,
	caps *commonpb.WorkerVersionCapabilities,
	userData *persistencespb.VersionedTaskQueueUserData,
) (dbQueueManager, error) {
	data := userData.GetData().GetVersioningData()

	if pm.partition.IsSticky() {
		// In the sticky case we don't redirect, but we may kick off this worker if there's a newer one.
		unknownBuild, err := checkVersionForStickyPoll(data, caps)
		if err != nil {
			return nil, err
		}
		if unknownBuild {
			pm.recordUnknownBuildPoll(caps.BuildId)
		}
		return pm.defaultQueue, nil
	}

	primarySetId, demotedSetIds, unknownBuild, err := lookupVersionSetForPoll(data, caps)
	if err != nil {
		return nil, err
	}
	if unknownBuild {
		pm.recordUnknownBuildPoll(caps.BuildId)
	}
	pm.loadDemotedSetIds(demotedSetIds)

	return pm.getVersionedQueue(ctx, primarySetId, true)
}

func (pm *taskQueuePartitionManagerImpl) loadDemotedSetIds(demotedSetIds []string) {
	// If we have demoted set ids, we need to load all task queues for them because even though
	// no new tasks will be sent to them, they might have old tasks in the db.
	// Also mark them alive, so that their liveness will be roughly synchronized.
	// TODO: once we know a demoted set id has no more tasks, we can remove it from versioning data
	for _, demotedSetId := range demotedSetIds {
		tqm, _ := pm.getVersionedQueueNoWait(demotedSetId, true)
		if tqm != nil {
			tqm.MarkAlive()
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) getDbQueueForAdd(
	ctx context.Context,
	directive *taskqueuespb.TaskVersionDirective,
) (dbQueueManager, error) {
	if directive.GetValue() == nil {
		return pm.defaultQueue, nil
	}

	// We don't need the userDataChanged channel here because:
	// - if we sync match, we're done
	// - if we spool to db, we'll re-resolve when it comes out of the db
	userData, _, err := pm.userDataManager.GetUserData()
	if err != nil {
		return nil, err
	}
	return pm.redirectToVersionSetQueueForAdd(ctx, directive, userData)
}

func (pm *taskQueuePartitionManagerImpl) redirectToVersionSetQueueForAdd(
	ctx context.Context,
	directive *taskqueuespb.TaskVersionDirective,
	userData *persistencespb.VersionedTaskQueueUserData,
) (dbQueueManager, error) {
	var buildId string
	switch dir := directive.GetValue().(type) {
	case *taskqueuespb.TaskVersionDirective_UseDefault:
		// leave buildId = "", lookupVersionSetForAdd understands that to mean "default"
	case *taskqueuespb.TaskVersionDirective_BuildId:
		buildId = dir.BuildId
	default:
		// Unversioned task, leave on unversioned queue.
		return pm.defaultQueue, nil
	}

	data := userData.GetData().GetVersioningData()

	if pm.partition.IsSticky() {
		// In the sticky case we don't redirect, but we may kick off this worker if there's a newer one.
		unknownBuild, err := checkVersionForStickyAdd(data, buildId)
		if err != nil {
			return nil, err
		}
		if unknownBuild {
			pm.recordUnknownBuildTask(buildId)
			// Don't bother persisting the unknown build id in this case: sticky tasks have a
			// short timeout, so it doesn't matter if they get lost.
		}
		return pm.defaultQueue, nil
	}

	versionSet, unknownBuild, err := lookupVersionSetForAdd(data, buildId)
	if err == errEmptyVersioningData { // nolint:goerr113
		// default was requested for an unversioned queue
		return pm.defaultQueue, nil
	} else if err != nil {
		return nil, err
	}
	if unknownBuild {
		pm.recordUnknownBuildTask(buildId)
		// Send rpc to root partition to persist the unknown build id before we return success.
		_, err = pm.matchingClient.UpdateWorkerBuildIdCompatibility(ctx, &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: pm.partition.NamespaceId().String(),
			TaskQueue:   pm.partition.TaskQueue().Family().TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition().RpcName(),
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
				PersistUnknownBuildId: buildId,
			},
		})
		if err != nil {
			return nil, err
		}
	}

	return pm.getVersionedQueue(ctx, versionSet, true)
}

func (pm *taskQueuePartitionManagerImpl) recordUnknownBuildPoll(buildId string) {
	pm.logger.Warn("unknown build id in poll", tag.BuildId(buildId))
	pm.taggedMetricsHandler.Counter(metrics.UnknownBuildPollsCounter.Name()).Record(1)
}

func (pm *taskQueuePartitionManagerImpl) recordUnknownBuildTask(buildId string) {
	pm.logger.Warn("unknown build id in task", tag.BuildId(buildId))
	pm.taggedMetricsHandler.Counter(metrics.UnknownBuildTasksCounter.Name()).Record(1)
}
