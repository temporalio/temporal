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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/internal/goro"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqname"
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
		// GetUserData returns the versioned user data for this task queue
		GetUserData() (*persistencespb.VersionedTaskQueueUserData, <-chan struct{}, error)
		// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
		// Extra care should be taken to avoid mutating the existing data in the update function.
		UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// DescribeTaskQueue returns information about the target task queue
		DescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		String() string
		QueueID() *taskQueueID
		TaskQueueKind() enumspb.TaskQueueKind
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
		engine      *matchingEngineImpl
		taskQueueID *taskQueueID
		stickyInfo
		namespaceName namespace.Name
		config        *taskQueueConfig
		// this is the default (unversioned) DB queue. As of now, some of the matters related to the whole TQ partition
		// is delegated to the defaultQueue. The plan is to eventually rename taskQueueManager to dbQueueManager and
		// bring all the partition-level logic to taskQueuePartitionManager.
		defaultQueue taskQueueManager
		// used for non-sticky versioned queues (one for each version)
		versionedQueues      map[string]taskQueueManager
		versionedQueuesLock  sync.RWMutex // locks mutation of versionedQueues
		db                   *taskQueueDB
		namespaceRegistry    namespace.Registry
		logger               log.Logger
		matchingClient       matchingservice.MatchingServiceClient
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
		goroGroup            goro.Group
		// userDataReady is fulfilled once versioning data is fetched from the root partition. If this TQ is
		// the root partition, it is fulfilled as soon as it is fetched from db.
		userDataReady *future.FutureImpl[struct{}]
	}
)

var _ taskQueuePartitionManager = (*taskQueuePartitionManagerImpl)(nil)

func newTaskQueuePartitionManager(
	e *matchingEngineImpl,
	taskQueue *taskQueueID,
	stickyInfo stickyInfo,
	config *Config,
) (taskQueuePartitionManager, error) {
	if taskQueue.VersionSet() != "" {
		return nil, serviceerror.NewInvalidArgument("task queue ID cannot be versioned")
	}

	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(taskQueue.namespaceID)
	if err != nil {
		return nil, err
	}
	nsName := namespaceEntry.Name()

	taskQueueConfig := newTaskQueueConfig(taskQueue, config, nsName)

	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(taskQueue.FullName()),
		tag.WorkflowTaskQueueType(taskQueue.taskType),
		tag.WorkflowNamespace(nsName.String()))
	taggedMetricsHandler := metrics.GetPerTaskQueueScope(
		e.metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingTaskQueuePartitionManagerScope), metrics.TaskQueueTypeTag(taskQueue.taskType)),
		nsName.String(),
		taskQueue.FullName(),
		stickyInfo.kind,
	)

	pm := &taskQueuePartitionManagerImpl{
		engine:               e,
		taskQueueID:          taskQueue,
		stickyInfo:           stickyInfo,
		config:               taskQueueConfig,
		namespaceRegistry:    e.namespaceRegistry,
		logger:               logger,
		matchingClient:       e.matchingRawClient,
		taggedMetricsHandler: taggedMetricsHandler,
		userDataReady:        future.NewFuture[struct{}](),
		versionedQueues:      make(map[string]taskQueueManager),
	}

	defaultQ, err := newTaskQueueManager(pm, taskQueue)
	if err != nil {
		return nil, err
	}

	pm.defaultQueue = defaultQ
	// TODO: separate DB client for task vs userData. partition manage only needs userData persistence
	pm.db = defaultQ.db
	return pm, nil
}

func (pm *taskQueuePartitionManagerImpl) Start() {
	if pm.db.DbStoresUserData() {
		pm.goroGroup.Go(pm.loadUserData)
	} else {
		pm.goroGroup.Go(pm.fetchUserData)
	}
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
	pm.goroGroup.Cancel()
}

func (pm *taskQueuePartitionManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	_, err := pm.userDataReady.Get(ctx)
	if err != nil {
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
		userData, _, err := pm.GetUserData()
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
			userData, userDataCh, err := pm.GetUserData()
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

// GetUserData returns the user data for the task queue if any.
// Note: can return nil value with no error.
func (pm *taskQueuePartitionManagerImpl) GetUserData() (*persistencespb.VersionedTaskQueueUserData, <-chan struct{}, error) {
	// mark alive so that it doesn't unload while a child partition is doing a long poll
	pm.defaultQueue.MarkAlive()
	return pm.db.GetUserData()
}

// UpdateUserData updates user data for this task queue and replicates across clusters if necessary.
func (pm *taskQueuePartitionManagerImpl) UpdateUserData(ctx context.Context, options UserDataUpdateOptions, updateFn UserDataUpdateFunc) error {
	newData, shouldReplicate, err := pm.db.UpdateUserData(ctx, updateFn, options.KnownVersion, options.TaskQueueLimitPerBuildId)
	if err != nil {
		return err
	}
	if !shouldReplicate {
		return nil
	}

	// Only replicate if namespace is global and has at least 2 clusters registered.
	ns, err := pm.namespaceRegistry.GetNamespaceByID(pm.db.namespaceID)
	if err != nil {
		return err
	}
	if ns.ReplicationPolicy() != namespace.ReplicationPolicyMultiCluster {
		return nil
	}

	_, err = pm.matchingClient.ReplicateTaskQueueUserData(ctx, &matchingservice.ReplicateTaskQueueUserDataRequest{
		NamespaceId: pm.db.namespaceID.String(),
		TaskQueue:   pm.taskQueueID.BaseNameString(),
		UserData:    newData.GetData(),
	})
	if err != nil {
		pm.logger.Error("Failed to publish a replication task after updating task queue user data", tag.Error(err))
		return serviceerror.NewUnavailable("storing task queue user data succeeded but publishing to the namespace replication queue failed, please try again")
	}
	return err
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

func (pm *taskQueuePartitionManagerImpl) QueueID() *taskQueueID {
	return pm.taskQueueID
}

func (pm *taskQueuePartitionManagerImpl) TaskQueueKind() enumspb.TaskQueueKind {
	return pm.kind
}

func (pm *taskQueuePartitionManagerImpl) LongPollExpirationInterval() time.Duration {
	return pm.config.LongPollExpirationInterval()
}

func (pm *taskQueuePartitionManagerImpl) callerInfoContext(ctx context.Context) context.Context {
	ns, _ := pm.namespaceRegistry.GetNamespaceName(pm.taskQueueID.namespaceID)
	return headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(ns.String()))
}

func (pm *taskQueuePartitionManagerImpl) unloadDbQueue(unloadedDbq taskQueueManager) {
	version := unloadedDbq.QueueID().VersionSet()
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

// Sets user data enabled/disabled and marks the future ready (if it's not ready yet).
// userDataState controls whether GetUserData return an error, and which.
// futureError is the error to set on the ready future. If this is non-nil, the task queue will
// be unloaded.
// Note that this must only be called from a single goroutine since the Ready/Set sequence is
// potentially racy otherwise.
func (pm *taskQueuePartitionManagerImpl) SetUserDataState(userDataState userDataState, futureError error) {
	// Always set state enabled/disabled even if we're not setting the future since we only set
	// the future once but the enabled/disabled state may change over time.
	pm.db.setUserDataState(userDataState)

	if !pm.userDataReady.Ready() {
		pm.userDataReady.Set(struct{}{}, futureError)
		if futureError != nil {
			pm.unloadFromEngine()
		}
	}
}

func (pm *taskQueuePartitionManagerImpl) loadUserData(ctx context.Context) error {
	ctx = pm.callerInfoContext(ctx)
	err := pm.db.loadUserData(ctx)
	pm.SetUserDataState(userDataEnabled, err)
	return nil
}

func (pm *taskQueuePartitionManagerImpl) userDataFetchSource() (string, error) {
	if pm.kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// Sticky queues get data from their corresponding normal queue
		if pm.normalName == "" {
			// Older SDKs don't send the normal name. That's okay, they just can't use versioning.
			return "", errMissingNormalQueueName
		}
		return pm.normalName, nil
	}

	degree := pm.config.ForwarderMaxChildrenPerNode()
	parent, err := pm.taskQueueID.Parent(degree)
	if err == tqname.ErrNoParent { // nolint:goerr113
		// we're the root activity task queue, ask the root workflow task queue
		return pm.taskQueueID.FullName(), nil
	} else if err != nil {
		// invalid degree
		return "", err
	}
	return parent.FullName(), nil
}

func (pm *taskQueuePartitionManagerImpl) fetchUserData(ctx context.Context) error {
	ctx = pm.callerInfoContext(ctx)

	// fetch from parent partition
	fetchSource, err := pm.userDataFetchSource()
	if err != nil {
		if err == errMissingNormalQueueName { // nolint:goerr113
			// pretend we have no user data. this is a sticky queue so the only effect is that we can't
			// kick off versioned pollers.
			pm.SetUserDataState(userDataEnabled, nil)
		}
		return err
	}

	// hasFetchedUserData is true if we have gotten a successful reply to GetTaskQueueUserData.
	// It's used to control whether we do a long poll or a simple get.
	hasFetchedUserData := false

	op := func(ctx context.Context) error {
		knownUserData, _, _ := pm.GetUserData()

		callCtx, cancel := context.WithTimeout(ctx, pm.config.GetUserDataLongPollTimeout())
		defer cancel()

		res, err := pm.matchingClient.GetTaskQueueUserData(callCtx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              pm.taskQueueID.namespaceID.String(),
			TaskQueue:                fetchSource,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: knownUserData.GetVersion(),
			WaitNewData:              hasFetchedUserData,
		})
		if err != nil {
			var unimplErr *serviceerror.Unimplemented
			if errors.As(err, &unimplErr) {
				// This might happen during a deployment. The older version couldn't have had any user data,
				// so we act as if it just returned an empty response and set ourselves ready.
				// Return the error so that we backoff with retry, and do not set hasFetchedUserData so that
				// we don't do a long poll next time.
				pm.SetUserDataState(userDataEnabled, nil)
			}
			return err
		}
		// If the root partition returns nil here, then that means our data matched, and we don't need to update.
		// If it's nil because it never existed, then we'd never have any data.
		// It can't be nil due to removing versions, as that would result in a non-nil container with
		// nil inner fields.
		if res.GetUserData() != nil {
			pm.db.setUserDataForNonOwningPartition(res.GetUserData())
		}
		hasFetchedUserData = true
		pm.SetUserDataState(userDataEnabled, nil)
		return nil
	}

	minWaitTime := pm.config.GetUserDataMinWaitTime

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, op, pm.config.GetUserDataRetryPolicy, nil)
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, pm.config.GetUserDataLongPollTimeout()/2)
		} else {
			minWaitTime = pm.config.GetUserDataMinWaitTime
		}
	}

	return ctx.Err()
}

func (pm *taskQueuePartitionManagerImpl) getVersionedQueue(
	ctx context.Context,
	version string,
	create bool,
) (taskQueueManager, error) {
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
) (taskQueueManager, error) {
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
			vq, err = newTaskQueueManager(pm, newTaskQueueIDWithVersionSet(pm.taskQueueID, version))
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
) (taskQueueManager, error) {
	data := userData.GetData().GetVersioningData()

	if pm.kind == enumspb.TASK_QUEUE_KIND_STICKY {
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
) (taskQueueManager, error) {
	if directive.GetValue() == nil {
		return pm.defaultQueue, nil
	}

	// We don't need the userDataChanged channel here because:
	// - if we sync match, we're done
	// - if we spool to db, we'll re-resolve when it comes out of the db
	userData, _, err := pm.GetUserData()
	if err != nil {
		return nil, err
	}
	dbq, err := pm.redirectToVersionSetQueueForAdd(ctx, directive, userData)
	if err != nil {
		return nil, err
	}
	return dbq, nil
}

func (pm *taskQueuePartitionManagerImpl) redirectToVersionSetQueueForAdd(
	ctx context.Context,
	directive *taskqueuespb.TaskVersionDirective,
	userData *persistencespb.VersionedTaskQueueUserData,
) (taskQueueManager, error) {
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

	if pm.kind == enumspb.TASK_QUEUE_KIND_STICKY {
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
			NamespaceId: pm.taskQueueID.namespaceID.String(),
			TaskQueue:   pm.taskQueueID.Root().FullName(),
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
