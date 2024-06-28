// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/pborman/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
)

const (
	// If sticky poller is not seem in last 10s, we treat it as sticky worker unavailable
	// This seems aggressive, but the default sticky schedule_to_start timeout is 5s, so 10s seems reasonable.
	stickyPollerUnavailableWindow = 10 * time.Second
	// If a compatible poller hasn't been seen for this time, we fail the CommitBuildId
	// Set to 70s so that it's a little over the max time a poller should be kept waiting.
	versioningPollerSeenWindow        = 70 * time.Second
	recordTaskStartedDefaultTimeout   = 10 * time.Second
	recordTaskStartedSyncMatchTimeout = 1 * time.Second
)

type (
	pollerIDCtxKey string
	identityCtxKey string

	taskQueueCounterKey struct {
		namespaceID   namespace.ID
		taskType      enumspb.TaskQueueType
		partitionType enumspb.TaskQueueKind
		versioned     string // one of these values: "unversioned", "versionSet", "buildId"
	}

	pollMetadata struct {
		ratePerSecond             *float64
		workerVersionCapabilities *commonpb.WorkerVersionCapabilities
		forwardedFrom             string
	}

	namespaceUpdateLocks struct {
		updateLock      sync.Mutex
		replicationLock sync.Mutex
	}

	gaugeMetrics struct {
		loadedTaskQueueFamilyCount    map[taskQueueCounterKey]int
		loadedTaskQueueCount          map[taskQueueCounterKey]int
		loadedTaskQueuePartitionCount map[taskQueueCounterKey]int
		loadedPhysicalTaskQueueCount  map[taskQueueCounterKey]int
		lock                          sync.Mutex
	}

	// Implements matching.Engine
	matchingEngineImpl struct {
		status                        int32
		taskManager                   persistence.TaskManager
		historyClient                 resource.HistoryClient
		matchingRawClient             resource.MatchingRawClient
		tokenSerializer               common.TaskTokenSerializer
		historySerializer             serialization.Serializer
		logger                        log.Logger
		throttledLogger               log.ThrottledLogger
		namespaceRegistry             namespace.Registry
		hostInfoProvider              membership.HostInfoProvider
		serviceResolver               membership.ServiceResolver
		membershipChangedCh           chan *membership.ChangedEvent
		clusterMeta                   cluster.Metadata
		timeSource                    clock.TimeSource
		visibilityManager             manager.VisibilityManager
		nexusEndpointClient           *nexusEndpointClient
		nexusEndpointsOwnershipLostCh chan struct{}
		metricsHandler                metrics.Handler
		partitionsLock                sync.RWMutex // locks mutation of partitions
		partitions                    map[tqid.PartitionKey]taskQueuePartitionManager
		gaugeMetrics                  gaugeMetrics // per-namespace task queue counters
		config                        *Config
		// queryResults maps query TaskID (which is a UUID generated in QueryWorkflow() call) to a channel
		// that QueryWorkflow() will block on. The channel is unblocked either by worker sending response through
		// RespondQueryTaskCompleted() or through an internal service error causing temporal to be unable to dispatch
		// query task to workflow worker.
		queryResults collection.SyncMap[string, chan *queryResult]
		// nexusResults maps nexus TaskID (which is a UUID generated in the DispatchNexusTask() call) to
		// a channel that DispatchNexusTask() blocks on. The channel is unblocked either by worker responding
		// via RespondNexusTaskCompleted() or RespondNexusTaskFailed(), or through an internal service error.
		nexusResults collection.SyncMap[string, chan *nexusResult]
		// outstandingPollers is needed to keep track of all outstanding pollers for a particular
		// taskqueue.  PollerID generated by frontend is used as the key and CancelFunc is the
		// value.  This is used to cancel the context to unblock any outstanding poller when
		// the frontend detects client connection is closed to prevent tasks being dispatched
		// to zombie pollers.
		outstandingPollers collection.SyncMap[string, context.CancelFunc]
		// Only set if global namespaces are enabled on the cluster.
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		// Disables concurrent task queue user data updates and replication requests (due to a cassandra limitation)
		namespaceUpdateLockMap map[string]*namespaceUpdateLocks
		// Serializes access to the per namespace lock map
		namespaceUpdateLockMapLock sync.Mutex
		// Stores results of reachability queries to visibility
		reachabilityCache reachabilityCache
	}
)

var (
	// EmptyPollWorkflowTaskQueueResponse is the response when there are no workflow tasks to hand out
	emptyPollWorkflowTaskQueueResponse = &matchingservice.PollWorkflowTaskQueueResponse{}
	// EmptyPollActivityTaskQueueResponse is the response when there are no activity tasks to hand out
	emptyPollActivityTaskQueueResponse = &matchingservice.PollActivityTaskQueueResponse{}

	errNoTasks = errors.New("no tasks")

	pollerIDKey pollerIDCtxKey = "pollerID"
	identityKey identityCtxKey = "identity"

	// The routing key for the single partition used to route Nexus endpoints CRUD RPCs to.
	nexusEndpointsTablePartitionRoutingKey = tqid.MustNormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED).RoutingKey()
)

var _ Engine = (*matchingEngineImpl)(nil) // Asserts that interface is indeed implemented

// NewEngine creates an instance of matching engine
func NewEngine(
	taskManager persistence.TaskManager,
	historyClient resource.HistoryClient,
	matchingRawClient resource.MatchingRawClient,
	config *Config,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	hostInfoProvider membership.HostInfoProvider,
	resolver membership.ServiceResolver,
	clusterMeta cluster.Metadata,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityManager manager.VisibilityManager,
	nexusEndpointManager persistence.NexusEndpointManager,
) Engine {
	scopedMetricsHandler := metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingEngineScope))
	e := &matchingEngineImpl{
		status:                        common.DaemonStatusInitialized,
		taskManager:                   taskManager,
		historyClient:                 historyClient,
		matchingRawClient:             matchingRawClient,
		tokenSerializer:               common.NewProtoTaskTokenSerializer(),
		historySerializer:             serialization.NewSerializer(),
		logger:                        log.With(logger, tag.ComponentMatchingEngine),
		throttledLogger:               log.With(throttledLogger, tag.ComponentMatchingEngine),
		namespaceRegistry:             namespaceRegistry,
		hostInfoProvider:              hostInfoProvider,
		serviceResolver:               resolver,
		membershipChangedCh:           make(chan *membership.ChangedEvent, 1), // allow one signal to be buffered while we're working
		clusterMeta:                   clusterMeta,
		timeSource:                    clock.NewRealTimeSource(), // No need to mock this at the moment
		visibilityManager:             visibilityManager,
		nexusEndpointClient:           newEndpointClient(nexusEndpointManager),
		nexusEndpointsOwnershipLostCh: make(chan struct{}),
		metricsHandler:                scopedMetricsHandler,
		partitions:                    make(map[tqid.PartitionKey]taskQueuePartitionManager),
		gaugeMetrics: gaugeMetrics{
			loadedTaskQueueFamilyCount:    make(map[taskQueueCounterKey]int),
			loadedTaskQueueCount:          make(map[taskQueueCounterKey]int),
			loadedTaskQueuePartitionCount: make(map[taskQueueCounterKey]int),
			loadedPhysicalTaskQueueCount:  make(map[taskQueueCounterKey]int),
		},
		config:                    config,
		queryResults:              collection.NewSyncMap[string, chan *queryResult](),
		nexusResults:              collection.NewSyncMap[string, chan *nexusResult](),
		outstandingPollers:        collection.NewSyncMap[string, context.CancelFunc](),
		namespaceReplicationQueue: namespaceReplicationQueue,
		namespaceUpdateLockMap:    make(map[string]*namespaceUpdateLocks),
	}
	e.reachabilityCache = newReachabilityCache(
		metrics.NoopMetricsHandler,
		visibilityManager,
		e.config.ReachabilityCacheOpenWFsTTL(),
		e.config.ReachabilityCacheClosedWFsTTL())
	return e
}

func (e *matchingEngineImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go e.watchMembership()
	_ = e.serviceResolver.AddListener(e.listenerKey(), e.membershipChangedCh)
}

func (e *matchingEngineImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	_ = e.serviceResolver.RemoveListener(e.listenerKey())
	close(e.membershipChangedCh)

	for _, l := range e.getTaskQueuePartitions(math.MaxInt32) {
		l.Stop(unloadCauseShuttingDown)
	}
}

func (e *matchingEngineImpl) listenerKey() string {
	return fmt.Sprintf("matchingEngine[%p]", e)
}

func (e *matchingEngineImpl) watchMembership() {
	self := e.hostInfoProvider.HostInfo().Identity()

	for range e.membershipChangedCh {
		delay := e.config.MembershipUnloadDelay()
		if delay == 0 {
			continue
		}

		e.notifyIfNexusEndpointsOwnershipLost()

		// Check all our loaded partitions to see if we lost ownership of any of them.
		e.partitionsLock.RLock()
		partitions := make([]tqid.Partition, 0, len(e.partitions))
		for _, pm := range e.partitions {
			partitions = append(partitions, pm.Partition())
		}
		e.partitionsLock.RUnlock()

		partitions = util.FilterSlice(partitions, func(p tqid.Partition) bool {
			owner, err := e.serviceResolver.Lookup(p.RoutingKey())
			return err == nil && owner.Identity() != self
		})

		const batchSize = 100
		for i := 0; i < len(partitions); i += batchSize {
			// We don't own these anymore, but don't unload them immediately, wait a few seconds to ensure
			// the membership update has propagated everywhere so that they won't get immediately re-loaded.
			// Note that we don't verify ownership at load time, so this is the only guard against a task
			// queue bouncing back and forth due to long membership propagation time.
			batch := partitions[i:min(len(partitions), i+batchSize)]
			wait := backoff.Jitter(delay, 0.1)
			time.AfterFunc(wait, func() {
				// maybe the whole engine stopped
				if atomic.LoadInt32(&e.status) != common.DaemonStatusStarted {
					return
				}
				for _, p := range batch {
					// maybe ownership changed again
					owner, err := e.serviceResolver.Lookup(p.RoutingKey())
					if err != nil || owner.Identity() == self {
						return
					}
					// now we can unload
					e.unloadTaskQueuePartitionByKey(p, nil, unloadCauseMembership)
				}
			})
		}
	}
}

func (e *matchingEngineImpl) getTaskQueuePartitions(maxCount int) (lists []taskQueuePartitionManager) {
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	lists = make([]taskQueuePartitionManager, 0, len(e.partitions))
	count := 0
	for _, tlMgr := range e.partitions {
		lists = append(lists, tlMgr)
		count++
		if count >= maxCount {
			break
		}
	}
	return
}

func (e *matchingEngineImpl) String() string {
	// Executes taskQueue.String() on each task queue outside of lock
	buf := new(bytes.Buffer)
	for _, l := range e.getTaskQueuePartitions(1000) {
		fmt.Fprintf(buf, "\n%s", l.String())
	}
	return buf.String()
}

// Returns taskQueuePartitionManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This blocks (up to the context deadline) for the
// task queue to be initialized.
//
// Note that task queue kind (sticky vs normal) and normal name for sticky task queues is not used as
// part of the task queue identity. That means that if getTaskQueuePartitionManager
// is called twice with the same task queue but different sticky info, the
// properties of the taskQueuePartitionManager will depend on which call came first. In general, we can
// rely on kind being the same for all calls now, but normalName was a later addition to the
// protocol and is not always set consistently. normalName is only required when using
// versioning, and SDKs that support versioning will always set it. The current server version
// will also set it when adding tasks from history. So that particular inconsistency is okay.
func (e *matchingEngineImpl) getTaskQueuePartitionManager(
	ctx context.Context,
	partition tqid.Partition,
	create bool,
	loadCause loadCause,
) (taskQueuePartitionManager, error) {
	pm, err := e.getTaskQueuePartitionManagerNoWait(partition, create, loadCause)
	if err != nil || pm == nil {
		return nil, err
	}
	if err = pm.WaitUntilInitialized(ctx); err != nil {
		e.unloadTaskQueuePartition(pm, unloadCauseInitError)
		return nil, err
	}
	return pm, nil
}

// Returns taskQueuePartitionManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This does not block for the task queue to be initialized.
func (e *matchingEngineImpl) getTaskQueuePartitionManagerNoWait(
	partition tqid.Partition,
	create bool,
	loadCause loadCause,
) (taskQueuePartitionManager, error) {
	key := partition.Key()
	e.partitionsLock.RLock()
	pm, ok := e.partitions[key]
	e.partitionsLock.RUnlock()
	if !ok {
		if !create {
			return nil, nil
		}

		// If it gets here, write lock and check again in case a task queue is created between the two locks
		e.partitionsLock.Lock()
		pm, ok = e.partitions[key]
		if !ok {
			namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(partition.NamespaceId())
			if err != nil {
				e.partitionsLock.Unlock()
				return nil, err
			}
			nsName := namespaceEntry.Name()
			tqConfig := newTaskQueueConfig(partition.TaskQueue(), e.config, nsName)
			tqConfig.loadCause = loadCause
			userDataManager := newUserDataManager(e.taskManager, e.matchingRawClient, partition, tqConfig, e.logger, e.namespaceRegistry)
			pm, err = newTaskQueuePartitionManager(e, namespaceEntry, partition, tqConfig, userDataManager)
			if err != nil {
				e.partitionsLock.Unlock()
				return nil, err
			}
			e.partitions[key] = pm
		}
		e.partitionsLock.Unlock()

		if !ok {
			pm.Start()
		}
	}
	return pm, nil
}

// For use in tests
func (e *matchingEngineImpl) updateTaskQueue(partition tqid.Partition, mgr taskQueuePartitionManager) {
	e.partitionsLock.Lock()
	defer e.partitionsLock.Unlock()
	e.partitions[partition.Key()] = mgr
}

// AddWorkflowTask either delivers task directly to waiting poller or saves it into task queue persistence.
func (e *matchingEngineImpl) AddWorkflowTask(
	ctx context.Context,
	addRequest *matchingservice.AddWorkflowTaskRequest,
) (buildId string, syncMatch bool, err error) {
	partition, err := tqid.PartitionFromProto(addRequest.TaskQueue, addRequest.NamespaceId, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return "", false, err
	}

	sticky := partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky, loadCauseTask)
	if err != nil {
		return "", false, err
	} else if sticky && !stickyWorkerAvailable(pm) {
		return "", false, serviceerrors.NewStickyWorkerUnavailable()
	}

	// This needs to move to history see - https://go.temporal.io/server/issues/181
	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := addRequest.GetScheduleToStartTimeout().AsDuration()
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      addRequest.NamespaceId,
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		ExpiryTime:       expirationTime,
		CreateTime:       timestamppb.New(now),
		VersionDirective: addRequest.VersionDirective,
	}

	return pm.AddTask(ctx, addTaskParams{
		taskInfo:    taskInfo,
		directive:   addRequest.VersionDirective,
		forwardInfo: addRequest.ForwardInfo,
	})
}

// AddActivityTask either delivers task directly to waiting poller or save it into task queue persistence.
func (e *matchingEngineImpl) AddActivityTask(
	ctx context.Context,
	addRequest *matchingservice.AddActivityTaskRequest,
) (buildId string, syncMatch bool, err error) {
	partition, err := tqid.PartitionFromProto(addRequest.TaskQueue, addRequest.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return "", false, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseTask)
	if err != nil {
		return "", false, err
	}

	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := timestamp.DurationValue(addRequest.GetScheduleToStartTimeout())
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      addRequest.NamespaceId,
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		CreateTime:       timestamppb.New(now),
		ExpiryTime:       expirationTime,
		VersionDirective: addRequest.VersionDirective,
	}

	return pm.AddTask(ctx, addTaskParams{
		taskInfo:    taskInfo,
		directive:   addRequest.VersionDirective,
		forwardInfo: addRequest.ForwardInfo,
	})
}

// PollWorkflowTaskQueue tries to get the workflow task using exponential backoff.
func (e *matchingEngineImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	req *matchingservice.PollWorkflowTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return nil, err
		}
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			forwardedFrom:             req.GetForwardedSource(),
		}
		task, versionSetUsed, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return emptyPollWorkflowTaskQueueResponse, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollWorkflowTaskQueueResponse(), nil
		}

		if task.isQuery() {
			task.finish(nil) // this only means query task sync match succeed.

			// for query task, we don't need to update history to record workflow task started. but we need to know
			// the NextEventID and the currently set sticky task queue.
			// TODO: in theory we only need this lookup for non-sticky queries (to get NextEventID for populating
			//		partial history in the response), but we need a new history API to to determine whether the query
			//		is sticky or not without this call
			mutableStateResp, err := e.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
				NamespaceId: req.GetNamespaceId(),
				Execution:   task.workflowExecution(),
			})
			if err != nil {
				// will notify query client that the query task failed
				_ = e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err})
				return emptyPollWorkflowTaskQueueResponse, nil
			}

			// A non-sticky poll may get task for a workflow that has sticky still set in its mutable state after
			// their sticky worker is dead for longer than 10s. In such case, we should set this to false so that
			// we return full history.
			isStickyEnabled := taskQueueName == mutableStateResp.StickyTaskQueue.GetName()

			hist, nextPageToken, err := e.getHistoryForQueryTask(ctx, namespaceID, task, isStickyEnabled)

			if err != nil {
				// will notify query client that the query task failed
				_ = e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err})
				return emptyPollWorkflowTaskQueueResponse, nil
			}

			resp := &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId:     mutableStateResp.PreviousStartedEventId,
				NextEventId:                mutableStateResp.NextEventId,
				WorkflowType:               mutableStateResp.WorkflowType,
				StickyExecutionEnabled:     isStickyEnabled,
				WorkflowExecutionTaskQueue: mutableStateResp.TaskQueue,
				BranchToken:                mutableStateResp.CurrentBranchToken,
				StartedEventId:             common.EmptyEventID,
				Attempt:                    1,
				History:                    hist,
				NextPageToken:              nextPageToken,
			}

			return e.createPollWorkflowTaskQueueResponse(task, resp, opMetrics), nil
		}

		requestClone := request
		if versionSetUsed {
			// We remove build ID from workerVersionCapabilities so History can differentiate between
			// old and new versioning in Record*TaskStart.
			// TODO: remove this block after old versioning cleanup. [cleanup-old-wv]
			requestClone = common.CloneProto(request)
			requestClone.WorkerVersionCapabilities.BuildId = ""
		}
		resp, err := e.recordWorkflowTaskStarted(ctx, requestClone, task)
		if err != nil {
			switch err.(type) {
			case *serviceerror.NotFound: // mutable state not found, workflow not running or workflow task not found
				e.logger.Info("Workflow task not found",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.WorkflowEventID(task.event.Data.GetScheduledEventId()),
					tag.Error(err),
				)
				task.finish(nil)
			case *serviceerrors.TaskAlreadyStarted:
				e.logger.Debug("Duplicated workflow task", tag.WorkflowTaskQueueName(taskQueueName), tag.TaskID(task.event.GetTaskId()))
				task.finish(nil)
			case *serviceerrors.ObsoleteDispatchBuildId:
				// history should've scheduled another task on the right build ID. dropping this one.
				e.logger.Info("dropping workflow task due to invalid build ID",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.BuildId(requestClone.WorkerVersionCapabilities.GetBuildId()),
				)
				task.finish(nil)
			default:
				task.finish(err)
				if err.Error() == common.ErrNamespaceHandover.Error() {
					// do not keep polling new tasks when namespace is in handover state
					// as record start request will be rejected by history service
					return nil, err
				}
			}

			continue pollLoop
		}

		task.finish(nil)
		return e.createPollWorkflowTaskQueueResponse(task, resp, opMetrics), nil
	}
}

// getHistoryForQueryTask retrieves history associated with a query task returned
// by PollWorkflowTaskQueue. Returns empty history for sticky query and full history for non-sticky
func (e *matchingEngineImpl) getHistoryForQueryTask(
	ctx context.Context,
	nsID namespace.ID,
	task *internalTask,
	isStickyEnabled bool,
) (*history.History, []byte, error) {
	if isStickyEnabled {
		return &history.History{Events: []*history.HistoryEvent{}}, nil, nil
	}

	maxPageSize := int32(e.config.HistoryMaxPageSize(task.namespace.String()))
	resp, err := e.historyClient.GetWorkflowExecutionHistory(ctx,
		&historyservice.GetWorkflowExecutionHistoryRequest{
			NamespaceId: nsID.String(),
			Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       task.namespace.String(),
				Execution:       task.workflowExecution(),
				MaximumPageSize: maxPageSize,
				WaitNewEvent:    false,
				SkipArchival:    true,
			},
		})
	if err != nil {
		return nil, nil, err
	}

	hist := resp.GetResponse().GetHistory()
	if resp.GetResponse().GetRawHistory() != nil {
		historyEvents := make([]*history.HistoryEvent, 0, maxPageSize)
		for _, blob := range resp.GetResponse().GetRawHistory() {
			events, err := e.historySerializer.DeserializeEvents(blob)
			if err != nil {
				return nil, nil, err
			}
			historyEvents = append(historyEvents, events...)
		}
		hist = &history.History{Events: historyEvents}
	}

	return hist, resp.GetResponse().GetNextPageToken(), err
}

// PollActivityTaskQueue takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollActivityTaskQueue(
	ctx context.Context,
	req *matchingservice.PollActivityTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}

		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		if err != nil {
			return nil, err
		}

		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			forwardedFrom:             req.GetForwardedSource(),
		}
		if request.TaskQueueMetadata != nil && request.TaskQueueMetadata.MaxTasksPerSecond != nil {
			pollMetadata.ratePerSecond = &request.TaskQueueMetadata.MaxTasksPerSecond.Value
		}
		task, versionSetUsed, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return emptyPollActivityTaskQueueResponse, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollActivityTaskQueueResponse(), nil
		}
		requestClone := request
		if versionSetUsed {
			// We remove build ID from workerVersionCapabilities so History can differentiate between
			// old and new versioning in Record*TaskStart.
			// TODO: remove this block after old versioning cleanup. [cleanup-old-wv]
			requestClone = common.CloneProto(request)
			requestClone.WorkerVersionCapabilities.BuildId = ""
		}
		resp, err := e.recordActivityTaskStarted(ctx, requestClone, task)
		if err != nil {
			switch err.(type) {
			case *serviceerror.NotFound: // mutable state not found, workflow not running or activity info not found
				e.logger.Info("Activity task not found",
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.WorkflowEventID(task.event.Data.GetScheduledEventId()),
					tag.Error(err),
				)
				task.finish(nil)
			case *serviceerrors.TaskAlreadyStarted:
				e.logger.Debug("Duplicated activity task", tag.WorkflowTaskQueueName(taskQueueName), tag.TaskID(task.event.GetTaskId()))
				task.finish(nil)
			case *serviceerrors.ObsoleteDispatchBuildId:
				// history should've scheduled another task on the right build ID. dropping this one.
				e.logger.Info("dropping activity task due to invalid build ID",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.BuildId(requestClone.WorkerVersionCapabilities.GetBuildId()),
				)
				task.finish(nil)
			default:
				task.finish(err)
				if err.Error() == common.ErrNamespaceHandover.Error() {
					// do not keep polling new tasks when namespace is in handover state
					// as record start request will be rejected by history service
					return nil, err
				}
			}

			continue pollLoop
		}
		task.finish(nil)
		return e.createPollActivityTaskQueueResponse(task, resp, opMetrics), nil
	}
}

type queryResult struct {
	workerResponse *matchingservice.RespondQueryTaskCompletedRequest
	internalError  error
}

// QueryWorkflow creates a WorkflowTask with query data, send it through sync match channel, wait for that WorkflowTask
// to be processed by worker, and then return the query result.
func (e *matchingEngineImpl) QueryWorkflow(
	ctx context.Context,
	queryRequest *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	partition, err := tqid.PartitionFromProto(queryRequest.TaskQueue, queryRequest.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	sticky := partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky, loadCauseQuery)
	if err != nil {
		return nil, err
	} else if sticky && !stickyWorkerAvailable(pm) {
		return nil, serviceerrors.NewStickyWorkerUnavailable()
	}

	taskID := uuid.New()
	resp, err := pm.DispatchQueryTask(ctx, taskID, queryRequest)

	// if we get a response or error it means that query task was handled by forwarding to another matching host
	// this remote host's result can be returned directly
	if resp != nil || err != nil {
		return resp, err
	}

	// if we get here it means that dispatch of query task has occurred locally
	// must wait on result channel to get query result
	queryResultCh := make(chan *queryResult, 1)
	e.queryResults.Set(taskID, queryResultCh)
	defer e.queryResults.Delete(taskID)

	select {
	case result := <-queryResultCh:
		if result.internalError != nil {
			return nil, result.internalError
		}

		workerResponse := result.workerResponse
		switch workerResponse.GetCompletedRequest().GetCompletedType() {
		case enumspb.QUERY_RESULT_TYPE_ANSWERED:
			return &matchingservice.QueryWorkflowResponse{QueryResult: workerResponse.GetCompletedRequest().GetQueryResult()}, nil
		case enumspb.QUERY_RESULT_TYPE_FAILED:
			return nil, serviceerror.NewQueryFailed(workerResponse.GetCompletedRequest().GetErrorMessage())
		default:
			return nil, serviceerror.NewInternal("unknown query completed type")
		}
	case <-ctx.Done():
		// task timed out. log (optionally) and return the timeout error
		ns, err := e.namespaceRegistry.GetNamespaceByID(partition.NamespaceId())
		if err != nil {
			e.logger.Error("Failed to get the namespace by ID",
				tag.WorkflowNamespaceID(partition.NamespaceId().String()),
				tag.Error(err))
		} else {
			sampleRate := e.config.QueryWorkflowTaskTimeoutLogRate(ns.Name().String(), partition.TaskQueue().Name(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			if rand.Float64() < sampleRate {
				e.logger.Info("Workflow Query Task timed out",
					tag.WorkflowNamespaceID(ns.ID().String()),
					tag.WorkflowNamespace(ns.Name().String()),
					tag.WorkflowID(queryRequest.GetQueryRequest().GetExecution().GetWorkflowId()),
					tag.WorkflowRunID(queryRequest.GetQueryRequest().GetExecution().GetRunId()),
					tag.WorkflowTaskRequestId(taskID),
					tag.WorkflowTaskQueueName(partition.TaskQueue().Name()))
			}
		}
		return nil, ctx.Err()
	}
}

func (e *matchingEngineImpl) RespondQueryTaskCompleted(
	_ context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opMetrics metrics.Handler,
) error {
	if err := e.deliverQueryResult(request.GetTaskId(), &queryResult{workerResponse: request}); err != nil {
		metrics.RespondQueryTaskFailedPerTaskQueueCounter.With(opMetrics).Record(1)
		return err
	}
	return nil
}

func (e *matchingEngineImpl) deliverQueryResult(taskID string, queryResult *queryResult) error {
	queryResultCh, ok := e.queryResults.Pop(taskID)
	if !ok {
		return serviceerror.NewNotFound("query task not found, or already expired")
	}
	queryResultCh <- queryResult
	return nil
}

func (e *matchingEngineImpl) CancelOutstandingPoll(
	_ context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
) error {
	cancel, ok := e.outstandingPollers.Pop(request.PollerId)
	if ok {
		cancel()
	}
	return nil
}

func (e *matchingEngineImpl) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (*matchingservice.DescribeTaskQueueResponse, error) {
	req := request.GetDescRequest()
	if req.ApiMode == enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED {
		rootPartition, err := tqid.PartitionFromProto(req.GetTaskQueue(), request.GetNamespaceId(), req.GetTaskQueueType())
		if err != nil {
			return nil, err
		}
		if !rootPartition.IsRoot() || rootPartition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY || rootPartition.TaskType() != enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			return nil, serviceerror.NewInvalidArgument("DescribeTaskQueue must be called on the root partition of workflow task queue if api mode is DESCRIBE_TASK_QUEUE_MODE_ENHANCED")
		}
		userData, err := e.getUserDataClone(ctx, rootPartition, loadCauseDescribe)
		if err != nil {
			return nil, err
		}
		if req.GetVersions() == nil {
			defaultBuildId := getDefaultBuildId(userData.GetVersioningData().GetAssignmentRules())
			req.Versions = &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{defaultBuildId}}
		}
		// collect internal info
		physicalInfoByBuildId := make(map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
		for _, taskQueueType := range req.TaskQueueTypes {
			for i := 0; i < e.config.NumTaskqueueWritePartitions(req.Namespace, req.TaskQueue.Name, taskQueueType); i++ {
				partitionResp, err := e.matchingRawClient.DescribeTaskQueuePartition(ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
					NamespaceId: request.GetNamespaceId(),
					TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
						TaskQueue:     req.TaskQueue.Name,
						TaskQueueType: taskQueueType,
						PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(i)},
					},
					Versions:      req.GetVersions(),
					ReportStats:   req.GetReportStats(),
					ReportPollers: req.GetReportPollers(),
				})
				if err != nil {
					return nil, err
				}
				for buildId, vii := range partitionResp.VersionsInfoInternal {
					if _, ok := physicalInfoByBuildId[buildId]; !ok {
						physicalInfoByBuildId[buildId] = make(map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
					}
					if physInfo, ok := physicalInfoByBuildId[buildId][taskQueueType]; !ok {
						physicalInfoByBuildId[buildId][taskQueueType] = vii.PhysicalTaskQueueInfo
					} else {
						var mergedStats *taskqueuepb.TaskQueueStats

						// only report Task Queue Statistics if requested.
						if req.GetReportStats() {
							totalStats := physicalInfoByBuildId[buildId][taskQueueType].TaskQueueStats
							partitionStats := vii.PhysicalTaskQueueInfo.TaskQueueStats

							mergedStats = &taskqueuepb.TaskQueueStats{
								ApproximateBacklogCount: totalStats.ApproximateBacklogCount + partitionStats.ApproximateBacklogCount,
								ApproximateBacklogAge:   largerBacklogAge(totalStats.ApproximateBacklogAge, partitionStats.ApproximateBacklogAge),
								TasksAddRate:            totalStats.TasksAddRate + partitionStats.TasksAddRate,
								TasksDispatchRate:       totalStats.TasksDispatchRate + partitionStats.TasksDispatchRate,
							}
						}
						merged := &taskqueuespb.PhysicalTaskQueueInfo{
							Pollers:        dedupPollers(append(physInfo.GetPollers(), vii.PhysicalTaskQueueInfo.GetPollers()...)),
							TaskQueueStats: mergedStats,
						}
						physicalInfoByBuildId[buildId][taskQueueType] = merged
					}
				}
			}
		}
		// smush internal info into versions info
		versionsInfo := make(map[string]*taskqueuepb.TaskQueueVersionInfo, 0)
		for bid, typeMap := range physicalInfoByBuildId {
			typesInfo := make(map[int32]*taskqueuepb.TaskQueueTypeInfo, 0)
			for taskQueueType, physicalInfo := range typeMap {
				typesInfo[int32(taskQueueType)] = &taskqueuepb.TaskQueueTypeInfo{
					Pollers: physicalInfo.Pollers,
					Stats:   physicalInfo.TaskQueueStats,
				}
			}
			var reachability enumspb.BuildIdTaskReachability
			if req.GetReportTaskReachability() {
				reachability, err = getBuildIdTaskReachability(ctx,
					newReachabilityCalculator(
						userData.GetVersioningData(),
						e.reachabilityCache,
						request.GetNamespaceId(),
						req.GetNamespace(),
						req.GetTaskQueue().GetName(),
						e.config.ReachabilityBuildIdVisibilityGracePeriod(req.GetNamespace()),
					),
					e.metricsHandler,
					e.logger,
					bid,
				)
				if err != nil {
					return nil, err
				}
			}
			versionsInfo[bid] = &taskqueuepb.TaskQueueVersionInfo{
				TypesInfo:        typesInfo,
				TaskReachability: reachability,
			}
		}
		return &matchingservice.DescribeTaskQueueResponse{
			DescResponse: &workflowservice.DescribeTaskQueueResponse{
				VersionsInfo: versionsInfo,
			},
		}, nil
	}
	// Otherwise, do legacy DescribeTaskQueue
	partition, err := tqid.PartitionFromProto(req.TaskQueue, request.GetNamespaceId(), req.TaskQueueType)
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseDescribe)
	if err != nil {
		return nil, err
	}
	return pm.LegacyDescribeTaskQueue(req.GetIncludeTaskQueueStatus()), nil
}

func dedupPollers(pollerInfos []*taskqueuepb.PollerInfo) []*taskqueuepb.PollerInfo {
	allKeys := make(map[string]bool)
	var list []*taskqueuepb.PollerInfo
	for _, item := range pollerInfos {
		if _, value := allKeys[item.GetIdentity()]; !value {
			allKeys[item.GetIdentity()] = true
			list = append(list, item)
		}
	}
	return list
}

func (e *matchingEngineImpl) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	if request.GetVersions() == nil {
		return nil, serviceerror.NewInvalidArgument("versions must not be nil, to describe the default queue, pass the default build ID as a member of the BuildIds list")
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, tqid.PartitionFromPartitionProto(request.GetTaskQueuePartition(), request.GetNamespaceId()), true, loadCauseDescribe)
	if err != nil {
		return nil, err
	}
	buildIds, err := e.getBuildIds(request.GetVersions())
	if err != nil {
		return nil, err
	}
	return pm.Describe(buildIds, request.GetVersions().GetAllActive(), request.GetReportStats(), request.GetReportPollers())
}

func (e *matchingEngineImpl) getBuildIds(versions *taskqueuepb.TaskQueueVersionSelection) (map[string]bool, error) {
	buildIds := make(map[string]bool)
	if versions != nil {
		for _, bid := range versions.GetBuildIds() {
			buildIds[bid] = true
		}
		if versions.GetUnversioned() {
			buildIds[""] = true
		}
	}
	return buildIds, nil
}

func (e *matchingEngineImpl) ListTaskQueuePartitions(
	_ context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
) (*matchingservice.ListTaskQueuePartitionsResponse, error) {
	activityTaskQueueInfo, err := e.listTaskQueuePartitions(request, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return nil, err
	}
	workflowTaskQueueInfo, err := e.listTaskQueuePartitions(request, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	resp := matchingservice.ListTaskQueuePartitionsResponse{
		ActivityTaskQueuePartitions: activityTaskQueueInfo,
		WorkflowTaskQueuePartitions: workflowTaskQueueInfo,
	}
	return &resp, nil
}

func (e *matchingEngineImpl) listTaskQueuePartitions(request *matchingservice.ListTaskQueuePartitionsRequest, taskQueueType enumspb.TaskQueueType) ([]*taskqueuepb.TaskQueuePartitionMetadata, error) {
	partitions, err := e.getAllPartitionRpcNames(
		namespace.Name(request.GetNamespace()),
		request.TaskQueue,
		taskQueueType,
	)

	if err != nil {
		return nil, err
	}

	partitionHostInfo := make([]*taskqueuepb.TaskQueuePartitionMetadata, len(partitions))
	for i, partition := range partitions {
		host, err := e.getHostInfo(partition)
		if err != nil {
			return nil, err
		}

		partitionHostInfo[i] = &taskqueuepb.TaskQueuePartitionMetadata{
			Key:           partition,
			OwnerHostName: host,
		}
	}

	return partitionHostInfo, nil
}

func (e *matchingEngineImpl) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.UpdateWorkerVersioningRulesRequest,
) (*matchingservice.UpdateWorkerVersioningRulesResponse, error) {
	req := request.GetRequest()
	ns, err := e.namespaceRegistry.GetNamespace(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	if ns.ID().String() != request.GetNamespaceId() {
		return nil, serviceerror.NewInternal("Namespace ID does not match Namespace in wrapped command")
	}
	if req.GetTaskQueue() != request.GetTaskQueue() {
		return nil, serviceerror.NewInternal("Task Queue does not match Task Queue in wrapped command")
	}

	// We only expect to receive task queue family name (root partition) here.
	taskQueueFamily, err := tqid.NewTaskQueueFamily(ns.ID().String(), req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}

	// we don't set updateOptions.TaskQueueLimitPerBuildId, because the Versioning Rule limits will be checked separately
	// we don't set updateOptions.KnownVersion, because we handle external API call ordering with conflictToken
	updateOptions := UserDataUpdateOptions{}
	cT := req.GetConflictToken()
	var getResp *matchingservice.GetWorkerVersioningRulesResponse
	var maxUpstreamBuildIDs int

	err = tqMgr.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		clk := data.GetClock()
		if clk == nil {
			clk = hlc.Zero(e.clusterMeta.GetClusterID())
		}

		prevCT, err := clk.Marshal()
		if err != nil {
			return nil, false, err
		}
		if !bytes.Equal(cT, prevCT) {
			return nil, false, serviceerror.NewFailedPrecondition(
				fmt.Sprintf("provided conflict token '%v' does not match existing one '%v'", cT, prevCT),
			)
		}

		updatedClock := hlc.Next(clk, e.timeSource)
		var versioningData *persistencespb.VersioningData
		switch req.GetOperation().(type) {
		case *workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule:
			versioningData, err = InsertAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetInsertAssignmentRule(),
				e.config.AssignmentRuleLimitPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceAssignmentRule:
			versioningData, err = ReplaceAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetReplaceAssignmentRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteAssignmentRule:
			versioningData, err = DeleteAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetDeleteAssignmentRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule:
			versioningData, err = AddCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetAddCompatibleRedirectRule(),
				e.config.RedirectRuleLimitPerQueue(ns.Name().String()),
				e.config.RedirectRuleMaxUpstreamBuildIDsPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleRedirectRule:
			versioningData, err = ReplaceCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetReplaceCompatibleRedirectRule(),
				e.config.RedirectRuleMaxUpstreamBuildIDsPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleRedirectRule:
			versioningData, err = DeleteCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetDeleteCompatibleRedirectRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId_:
			versioningData, err = CommitBuildID(
				updatedClock,
				data.GetVersioningData(),
				req.GetCommitBuildId(),
				tqMgr.HasPollerAfter(req.GetCommitBuildId().GetTargetBuildId(), time.Now().Add(-versioningPollerSeenWindow)),
				e.config.AssignmentRuleLimitPerQueue(ns.Name().String()),
			)
		}
		if err != nil {
			// operation can't be completed due to failed validation. no action, do not replicate, report error
			return nil, false, err
		}

		// Get versioning data formatted for response
		getResp, err = GetTimestampedWorkerVersioningRules(versioningData, updatedClock)
		if err != nil {
			return nil, false, err
		}
		// Get max upstream build IDs (min is 0, because we count number of upstream nodes)
		activeRedirectRules := getActiveRedirectRules(versioningData.GetRedirectRules())
		for _, rule := range activeRedirectRules {
			upstream := getUpstreamBuildIds(rule.GetRule().GetTargetBuildId(), activeRedirectRules)
			if len(upstream)+1 > maxUpstreamBuildIDs {
				maxUpstreamBuildIDs = len(upstream) + 1
			}
		}

		// Clean up tombstones after all fallible tasks are complete, once we know we are committing and replicating the changes.
		// We can replicate tombstone cleanup, because it's just based on DeletionTimestamp, so no need to only do it locally.
		versioningData = CleanupRuleTombstones(versioningData, e.config.DeletedRuleRetentionTime(ns.Name().String()))

		// Avoid mutation
		ret := common.CloneProto(data)
		ret.Clock = updatedClock
		ret.VersioningData = versioningData
		return ret, true, nil
	})

	if err != nil {
		return nil, err
	}

	// log resulting rule counts
	assignmentRules := getResp.GetResponse().GetAssignmentRules()
	redirectRules := getResp.GetResponse().GetCompatibleRedirectRules()

	e.logger.Info("UpdateWorkerVersioningRules completed",
		tag.WorkerVersioningRedirectRuleCount(len(redirectRules)),
		tag.WorkerVersioningAssignmentRuleCount(len(assignmentRules)),
		tag.WorkerVersioningMaxUpstreamBuildIDs(maxUpstreamBuildIDs))

	return &matchingservice.UpdateWorkerVersioningRulesResponse{Response: &workflowservice.UpdateWorkerVersioningRulesResponse{
		AssignmentRules:         assignmentRules,
		CompatibleRedirectRules: redirectRules,
		ConflictToken:           getResp.GetResponse().GetConflictToken(),
	}}, nil
}

func (e *matchingEngineImpl) GetWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.GetWorkerVersioningRulesRequest,
) (*matchingservice.GetWorkerVersioningRulesResponse, error) {
	req := request.GetRequest()
	ns, err := e.namespaceRegistry.GetNamespace(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	if ns.ID().String() != request.GetNamespaceId() {
		return nil, serviceerror.NewInternal("Namespace ID does not match Namespace in wrapped command")
	}
	if req.GetTaskQueue() != request.GetTaskQueue() {
		return nil, serviceerror.NewInternal("Task Queue does not match Task Queue in wrapped command")
	}

	// We only expect to receive task queue family name (root partition) here.
	taskQueueFamily, err := tqid.NewTaskQueueFamily(ns.ID().String(), req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	userData, err := e.getUserDataClone(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), loadCauseOtherRead)
	if err != nil {
		return nil, err
	}
	clk := userData.GetClock()
	if clk == nil {
		clk = hlc.Zero(e.clusterMeta.GetClusterID())
	}
	return GetTimestampedWorkerVersioningRules(userData.GetVersioningData(), clk)
}

func (e *matchingEngineImpl) getUserDataClone(
	ctx context.Context,
	rootPartition tqid.Partition,
	loadCause loadCause,
) (*persistencespb.TaskQueueUserData, error) {
	rootPartitionMgr, err := e.getTaskQueuePartitionManager(ctx, rootPartition, true, loadCause)
	if err != nil {
		return nil, err
	}
	userData, _, err := rootPartitionMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	if userData == nil {
		userData = &persistencespb.VersionedTaskQueueUserData{Data: &persistencespb.TaskQueueUserData{}}
	} else {
		userData = common.CloneProto(userData)
	}
	return userData.GetData(), nil
}

func (e *matchingEngineImpl) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	req *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	taskQueue, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueue.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}
	updateOptions := UserDataUpdateOptions{}
	operationCreatedTombstones := false
	switch req.GetOperation().(type) {
	case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_:
		// Only apply the limit when request is initiated by a user.
		updateOptions.TaskQueueLimitPerBuildId = e.config.TaskQueueLimitPerBuildId(ns.Name().String())
	case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_:
		updateOptions.KnownVersion = req.GetRemoveBuildIds().GetKnownUserDataVersion()
	}

	err = pm.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		clk := data.GetClock()
		if clk == nil {
			tmp := hlc.Zero(e.clusterMeta.GetClusterID())
			clk = tmp
		}
		updatedClock := hlc.Next(clk, e.timeSource)
		var versioningData *persistencespb.VersioningData
		switch req.GetOperation().(type) {
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_:
			var err error
			versioningData, err = UpdateVersionSets(
				updatedClock,
				data.GetVersioningData(),
				req.GetApplyPublicRequest().GetRequest(),
				e.config.VersionCompatibleSetLimitPerQueue(ns.Name().String()),
				e.config.VersionBuildIdLimitPerQueue(ns.Name().String()),
			)
			if err != nil {
				return nil, false, err
			}
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_:
			versioningData = RemoveBuildIds(
				updatedClock,
				data.GetVersioningData(),
				req.GetRemoveBuildIds().GetBuildIds(),
			)
			if ns.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster {
				operationCreatedTombstones = true
			} else {
				// We don't need to keep the tombstones around if we're not replicating them.
				versioningData = ClearTombstones(versioningData)
			}
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId:
			versioningData = PersistUnknownBuildId(
				updatedClock,
				data.GetVersioningData(),
				req.GetPersistUnknownBuildId(),
			)
		default:
			return nil, false, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid operation: %v", req.GetOperation()))
		}
		// Avoid mutation
		ret := common.CloneProto(data)
		ret.Clock = updatedClock
		ret.VersioningData = versioningData
		return ret, true, nil
	})
	if err != nil {
		return nil, err
	}

	// Only clear tombstones after they have been replicated.
	if operationCreatedTombstones {
		err = pm.GetUserDataManager().UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
			updatedClock := hlc.Next(data.GetClock(), e.timeSource)
			// Avoid mutation
			ret := common.CloneProto(data)
			ret.Clock = updatedClock
			ret.VersioningData = ClearTombstones(data.VersioningData)
			return ret, false, nil // Do not replicate the deletion of tombstones
		})
		if err != nil {
			return nil, err
		}
	}
	return &matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil
}

func (e *matchingEngineImpl) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	req *matchingservice.GetWorkerBuildIdCompatibilityRequest,
) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	taskQueueFamily, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.Request.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherRead)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			return &matchingservice.GetWorkerBuildIdCompatibilityResponse{}, nil
		}
		return nil, err
	}
	userData, _, err := pm.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	return &matchingservice.GetWorkerBuildIdCompatibilityResponse{
		Response: ToBuildIdOrderingResponse(userData.GetData().GetVersioningData(), int(req.GetRequest().GetMaxSets())),
	}, nil
}

func (e *matchingEngineImpl) GetTaskQueueUserData(
	ctx context.Context,
	req *matchingservice.GetTaskQueueUserDataRequest,
) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	partition, err := tqid.PartitionFromProto(&taskqueuepb.TaskQueue{Name: req.GetTaskQueue()}, req.NamespaceId, req.TaskQueueType)
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseUserData)
	if err != nil {
		return nil, err
	}
	version := req.GetLastKnownUserDataVersion()
	if version < 0 {
		return nil, serviceerror.NewInvalidArgument("last_known_user_data_version must not be negative")
	}

	if req.WaitNewData {
		var cancel context.CancelFunc
		ctx, cancel = newChildContext(ctx, e.config.GetUserDataLongPollTimeout(), returnEmptyTaskTimeBudget)
		defer cancel()
		// mark alive so that it doesn't unload while a child partition is doing a long poll
		pm.MarkAlive()
	}

	for {
		resp := &matchingservice.GetTaskQueueUserDataResponse{}
		userData, userDataChanged, err := pm.GetUserDataManager().GetUserData()
		if errors.Is(err, errTaskQueueClosed) {
			// If we're closing, return a success with no data, as if the request expired. We shouldn't
			// close due to idleness (because of the MarkAlive above), so we're probably closing due to a
			// change of ownership. The caller will retry and be redirected to the new owner.
			return resp, nil
		} else if err != nil {
			return nil, err
		}
		if req.WaitNewData && userData.GetVersion() == version {
			// long-poll: wait for data to change/appear
			select {
			case <-ctx.Done():
				return resp, nil
			case <-userDataChanged:
				continue
			}
		}
		if userData != nil {
			if userData.Version > version {
				resp.UserData = userData
			} else if userData.Version < version {
				// This is highly unlikely but may happen due to an edge case in during ownership transfer.
				// We rely on client retries in this case to let the system eventually self-heal.
				return nil, serviceerror.NewInvalidArgument(
					"requested task queue user data for version greater than known version")
			}
		}
		return resp, nil
	}
}

func (e *matchingEngineImpl) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	req *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	taskQueueFamily, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseUserData)
	if err != nil {
		return nil, err
	}
	updateOptions := UserDataUpdateOptions{
		// Avoid setting a limit to allow the replication event to always be applied
		TaskQueueLimitPerBuildId: 0,
	}
	err = pm.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(current *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		mergedUserData := common.CloneProto(current)
		_, buildIdsRemoved := GetBuildIdDeltas(current.GetVersioningData(), req.GetUserData().GetVersioningData())
		var buildIdsToRevive []string
		for _, buildId := range buildIdsRemoved {
			// We accept that the user data is locked for updates while running these visibility queries.
			// Nothing else is _supposed_ to update it on follower (standby) clusters.
			exists, err := worker_versioning.WorkflowsExistForBuildId(ctx, e.visibilityManager, ns, req.TaskQueue, buildId)
			if err != nil {
				return nil, false, err
			}
			if exists {
				buildIdsToRevive = append(buildIdsToRevive, buildId)
			}
		}
		mergedData := MergeVersioningData(current.GetVersioningData(), req.GetUserData().GetVersioningData())

		for _, buildId := range buildIdsToRevive {
			setIdx, buildIdIdx := worker_versioning.FindBuildId(mergedData, buildId)
			if setIdx == -1 {
				continue
			}
			set := mergedData.VersionSets[setIdx]
			set.BuildIds[buildIdIdx] = e.reviveBuildId(ns, req.GetTaskQueue(), set.GetBuildIds()[buildIdIdx])
			mergedUserData.Clock = hlc.Max(mergedUserData.Clock, set.BuildIds[buildIdIdx].StateUpdateTimestamp)

			setDefault := set.BuildIds[len(set.BuildIds)-1]
			if setDefault.State == persistencespb.STATE_DELETED {
				// We merged an update which removed (at least) two build ids: the default for set x and another one for set
				// x. We discovered we're still using the other one, so we revive it. now we also have to revive the default
				// for set x, or it will be left with the wrong default.
				set.BuildIds[len(set.BuildIds)-1] = e.reviveBuildId(ns, req.GetTaskQueue(), setDefault)
				mergedUserData.Clock = hlc.Max(mergedUserData.Clock, setDefault.StateUpdateTimestamp)
			}
		}

		// No need to keep the tombstones around after replication.
		mergedUserData.VersioningData = ClearTombstones(mergedData)
		return mergedUserData, len(buildIdsToRevive) > 0, nil
	})
	return &matchingservice.ApplyTaskQueueUserDataReplicationEventResponse{}, err
}

func (e *matchingEngineImpl) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	req *matchingservice.GetBuildIdTaskQueueMappingRequest,
) (*matchingservice.GetBuildIdTaskQueueMappingResponse, error) {
	taskQueues, err := e.taskManager.GetTaskQueuesByBuildId(ctx, &persistence.GetTaskQueuesByBuildIdRequest{
		NamespaceID: req.NamespaceId,
		BuildID:     req.BuildId,
	})
	if err != nil {
		return nil, err
	}
	return &matchingservice.GetBuildIdTaskQueueMappingResponse{TaskQueues: taskQueues}, nil
}

func (e *matchingEngineImpl) ForceUnloadTaskQueue(
	ctx context.Context,
	req *matchingservice.ForceUnloadTaskQueueRequest,
) (*matchingservice.ForceUnloadTaskQueueResponse, error) {
	p, err := tqid.NormalPartitionFromRpcName(req.GetTaskQueue(), req.GetNamespaceId(), req.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	wasLoaded := e.unloadTaskQueuePartitionByKey(p, nil, unloadCauseForce)
	return &matchingservice.ForceUnloadTaskQueueResponse{WasLoaded: wasLoaded}, nil
}

func (e *matchingEngineImpl) UpdateTaskQueueUserData(ctx context.Context, request *matchingservice.UpdateTaskQueueUserDataRequest) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
	locks := e.getNamespaceUpdateLocks(request.GetNamespaceId())
	locks.updateLock.Lock()
	defer locks.updateLock.Unlock()

	err := e.taskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID:     request.GetNamespaceId(),
		TaskQueue:       request.GetTaskQueue(),
		UserData:        request.GetUserData(),
		BuildIdsAdded:   request.BuildIdsAdded,
		BuildIdsRemoved: request.BuildIdsRemoved,
	})
	return &matchingservice.UpdateTaskQueueUserDataResponse{}, err
}

func (e *matchingEngineImpl) ReplicateTaskQueueUserData(ctx context.Context, request *matchingservice.ReplicateTaskQueueUserDataRequest) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {
	if e.namespaceReplicationQueue == nil {
		return &matchingservice.ReplicateTaskQueueUserDataResponse{}, nil
	}

	locks := e.getNamespaceUpdateLocks(request.GetNamespaceId())
	locks.replicationLock.Lock()
	defer locks.replicationLock.Unlock()

	err := e.namespaceReplicationQueue.Publish(ctx, &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
		Attributes: &replicationspb.ReplicationTask_TaskQueueUserDataAttributes{
			TaskQueueUserDataAttributes: &replicationspb.TaskQueueUserDataAttributes{
				NamespaceId:   request.GetNamespaceId(),
				TaskQueueName: request.GetTaskQueue(),
				UserData:      request.GetUserData(),
			},
		},
	})
	return &matchingservice.ReplicateTaskQueueUserDataResponse{}, err

}

// nexusResult is container for a response or error.
// Only one field may be set at a time.
type nexusResult struct {
	successfulWorkerResponse *matchingservice.RespondNexusTaskCompletedRequest
	failedWorkerResponse     *matchingservice.RespondNexusTaskFailedRequest
	internalError            error
}

func (e *matchingEngineImpl) DispatchNexusTask(ctx context.Context, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
	partition, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseNexusTask)
	if err != nil {
		return nil, err
	}

	taskID := uuid.New()
	resp, err := pm.DispatchNexusTask(ctx, taskID, request)

	// if we get a response or error it means that the Nexus task was handled by forwarding to another matching host
	// this remote host's result can be returned directly
	if resp != nil || err != nil {
		return resp, err
	}

	// if we get here it means that dispatch of query task has occurred locally
	// must wait on result channel to get query result
	resultCh := make(chan *nexusResult, 1)
	e.nexusResults.Set(taskID, resultCh)
	defer e.nexusResults.Delete(taskID)

	select {
	case result := <-resultCh:
		if result.internalError != nil {
			return nil, result.internalError
		}
		if result.failedWorkerResponse != nil {
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
				HandlerError: result.failedWorkerResponse.GetRequest().GetError(),
			}}, nil
		}

		return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: result.successfulWorkerResponse.GetRequest().GetResponse(),
		}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *matchingEngineImpl) PollNexusTaskQueue(
	ctx context.Context,
	req *matchingservice.PollNexusTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.Request
	taskQueueName := request.TaskQueue.GetName()
	e.logger.Debug("Received PollNexusTaskQueue for taskQueue", tag.Name(taskQueueName))
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_NEXUS)
		if err != nil {
			return nil, err
		}
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			forwardedFrom:             req.GetForwardedSource(),
		}
		task, _, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return &matchingservice.PollNexusTaskQueueResponse{}, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollNexusTaskQueueResponse(), nil
		}

		task.finish(err)
		if err != nil {
			continue pollLoop
		}

		taskToken := &tokenspb.NexusTask{
			NamespaceId: string(namespaceID),
			TaskQueue:   taskQueueName,
			TaskId:      task.nexus.taskID,
		}
		serializedToken, _ := e.tokenSerializer.SerializeNexusTaskToken(taskToken)

		nexusReq := task.nexus.request.GetRequest()
		nexusReq.Header[nexus.HeaderRequestTimeout] = time.Until(task.nexus.deadline).String()

		return &matchingservice.PollNexusTaskQueueResponse{
			Response: &workflowservice.PollNexusTaskQueueResponse{
				TaskToken: serializedToken,
				Request:   nexusReq,
			},
		}, nil
	}
}

func (e *matchingEngineImpl) RespondNexusTaskCompleted(ctx context.Context, request *matchingservice.RespondNexusTaskCompletedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskCompletedResponse, error) {
	resultCh, ok := e.nexusResults.Pop(request.GetTaskId())
	if !ok {
		opMetrics.Counter(metrics.RespondNexusTaskFailedPerTaskQueueCounter.Name()).Record(1)
		return nil, serviceerror.NewNotFound("Nexus task not found or already expired")
	}
	resultCh <- &nexusResult{
		successfulWorkerResponse: request,
		internalError:            nil,
	}
	return &matchingservice.RespondNexusTaskCompletedResponse{}, nil
}

func (e *matchingEngineImpl) RespondNexusTaskFailed(ctx context.Context, request *matchingservice.RespondNexusTaskFailedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskFailedResponse, error) {
	resultCh, ok := e.nexusResults.Pop(request.GetTaskId())
	if !ok {
		opMetrics.Counter(metrics.RespondNexusTaskFailedPerTaskQueueCounter.Name()).Record(1)
		return nil, serviceerror.NewNotFound("Nexus task not found or already expired")
	}
	resultCh <- &nexusResult{
		failedWorkerResponse: request,
		internalError:        nil,
	}
	return &matchingservice.RespondNexusTaskFailedResponse{}, nil
}

func (e *matchingEngineImpl) CreateNexusEndpoint(ctx context.Context, request *matchingservice.CreateNexusEndpointRequest) (*matchingservice.CreateNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.CreateNexusEndpoint(ctx, &internalCreateNexusEndpointRequest{
		spec:       request.GetSpec(),
		clusterID:  e.clusterMeta.GetClusterID(),
		timeSource: e.timeSource,
	})
	if err != nil {
		e.logger.Error("Failed to create Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetSpec().GetName()))
	} else {
		e.logger.Info("Created Nexus endpoint", tag.Endpoint(request.GetSpec().GetName()))
	}
	return res, err
}

func (e *matchingEngineImpl) UpdateNexusEndpoint(ctx context.Context, request *matchingservice.UpdateNexusEndpointRequest) (*matchingservice.UpdateNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.UpdateNexusEndpoint(ctx, &internalUpdateNexusEndpointRequest{
		endpointID: request.GetId(),
		version:    request.GetVersion(),
		spec:       request.GetSpec(),
		clusterID:  e.clusterMeta.GetClusterID(),
		timeSource: e.timeSource,
	})
	if err != nil {
		e.logger.Error("Failed to update Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetSpec().GetName()))
	} else {
		e.logger.Info("Updated Nexus endpoint", tag.Endpoint(request.GetSpec().GetName()))
	}
	return res, err
}

func (e *matchingEngineImpl) DeleteNexusEndpoint(ctx context.Context, request *matchingservice.DeleteNexusEndpointRequest) (*matchingservice.DeleteNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.DeleteNexusEndpoint(ctx, request)
	if err != nil {
		e.logger.Error("Failed to delete Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetId()))
	} else {
		e.logger.Info("Deleted Nexus endpoint", tag.Endpoint(request.GetId()))
	}
	return res, err
}

func (e *matchingEngineImpl) ListNexusEndpoints(ctx context.Context, request *matchingservice.ListNexusEndpointsRequest) (*matchingservice.ListNexusEndpointsResponse, error) {
	lastKnownVersion := request.LastKnownTableVersion
	// Read API, verify table ownership via membership.
	isOwner, ownershipLostCh, err := e.checkNexusEndpointsOwnership()
	if err != nil {
		e.logger.Error("Failed to check Nexus endpoints ownerhip", tag.Error(err))
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("cannot verify ownership of Nexus endpoints table: %v", err))
	}
	if !isOwner {
		e.logger.Error("Matching node doesn't think it's the Nexus endpoints table owner", tag.Error(err))
		return nil, serviceerror.NewFailedPrecondition("matching node doesn't think it's the Nexus endpoints table owner")
	}

	if request.Wait {
		if request.NextPageToken != nil {
			return nil, serviceerror.NewInvalidArgument("request Wait=true and NextPageToken!=nil on ListNexusEndpoints request. waiting is only allowed on first page")
		}

		// if waiting, send request with unknown table version so we get the newest view of the table
		request.LastKnownTableVersion = 0

		var cancel context.CancelFunc
		ctx, cancel = newChildContext(ctx, e.config.ListNexusEndpointsLongPollTimeout(), returnEmptyTaskTimeBudget)
		defer cancel()
	}

	for {
		resp, tableVersionChanged, err := e.nexusEndpointClient.ListNexusEndpoints(ctx, request)
		if err != nil {
			return resp, err
		}

		if request.Wait && lastKnownVersion == resp.TableVersion {
			// long-poll: wait for data to change/appear
			select {
			case <-ownershipLostCh:
				return nil, serviceerror.NewFailedPrecondition("Nexus endpoints table ownership lost")
			case <-ctx.Done():
				return resp, nil
			case <-tableVersionChanged:
				continue
			}
		}

		return resp, err
	}
}

func (e *matchingEngineImpl) checkNexusEndpointsOwnership() (bool, <-chan struct{}, error) {
	// Get the channel before checking the condition to prevent the channel from being closed while we're running this
	// check.
	ch := e.nexusEndpointsOwnershipLostCh
	self := e.hostInfoProvider.HostInfo().Identity()
	owner, err := e.serviceResolver.Lookup(nexusEndpointsTablePartitionRoutingKey)
	if err != nil {
		return false, nil, fmt.Errorf("cannot resolve Nexus endpoints partition owner: %w", err)
	}
	return owner.Identity() == self, ch, nil
}

func (e *matchingEngineImpl) notifyIfNexusEndpointsOwnershipLost() {
	// We don't care about the channel returned here. This method is ensured to only be called from the single
	// watchMembership method and is the only way the channel may be replace.
	isOwner, _, err := e.checkNexusEndpointsOwnership()
	if err != nil {
		e.logger.Error("Failed to check Nexus endpoints ownerhip", tag.Error(err))
		return
	}
	if !isOwner {
		close(e.nexusEndpointsOwnershipLostCh)
		e.nexusEndpointsOwnershipLostCh = make(chan struct{})
	}
}

func (e *matchingEngineImpl) getNamespaceUpdateLocks(namespaceId string) *namespaceUpdateLocks {
	e.namespaceUpdateLockMapLock.Lock()
	defer e.namespaceUpdateLockMapLock.Unlock()
	locks, found := e.namespaceUpdateLockMap[namespaceId]
	if !found {
		locks = &namespaceUpdateLocks{}
		e.namespaceUpdateLockMap[namespaceId] = locks
	}
	return locks
}

func (e *matchingEngineImpl) getHostInfo(partitionKey string) (string, error) {
	host, err := e.serviceResolver.Lookup(partitionKey)
	if err != nil {
		return "", err
	}
	return host.GetAddress(), nil
}

func (e *matchingEngineImpl) getAllPartitionRpcNames(
	ns namespace.Name,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
) ([]string, error) {
	var partitionKeys []string
	namespaceID, err := e.namespaceRegistry.GetNamespaceID(ns)
	if err != nil {
		return partitionKeys, err
	}
	taskQueueFamily, err := tqid.NewTaskQueueFamily(namespaceID.String(), taskQueue.GetName())
	if err != nil {
		return partitionKeys, err
	}

	n := e.config.NumTaskqueueWritePartitions(ns.String(), taskQueueFamily.Name(), taskQueueType)
	for i := 0; i < n; i++ {
		partitionKeys = append(partitionKeys, taskQueueFamily.TaskQueue(taskQueueType).NormalPartition(i).RpcName())
	}
	return partitionKeys, nil
}

func (e *matchingEngineImpl) pollTask(
	ctx context.Context,
	partition tqid.Partition,
	pollMetadata *pollMetadata,
) (*internalTask, bool, error) {
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCausePoll)
	if err != nil {
		return nil, false, err
	}

	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	ctx, cancel := newChildContext(ctx, pm.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	if pollerID, ok := ctx.Value(pollerIDKey).(string); ok && pollerID != "" {
		e.outstandingPollers.Set(pollerID, cancel)
		defer e.outstandingPollers.Delete(pollerID)
	}
	return pm.PollTask(ctx, pollMetadata)
}

// Unloads the given task queue partition. If it has already been unloaded (i.e. it's not present in the loaded
// partitions map), then does nothing.
func (e *matchingEngineImpl) unloadTaskQueuePartition(unloadPM taskQueuePartitionManager, unloadCause unloadCause) {
	e.unloadTaskQueuePartitionByKey(unloadPM.Partition(), unloadPM, unloadCause)
}

// Unloads a task queue partition by id. If unloadPM is given and the loaded partition for queueID does not match
// unloadPM, then nothing is unloaded from matching engine (but unloadPM will be stopped).
// Returns true if it unloaded a partition and false if not.
func (e *matchingEngineImpl) unloadTaskQueuePartitionByKey(
	partition tqid.Partition,
	unloadPM taskQueuePartitionManager,
	unloadCause unloadCause,
) bool {
	key := partition.Key()
	e.partitionsLock.Lock()
	foundTQM, ok := e.partitions[key]
	if !ok || (unloadPM != nil && foundTQM != unloadPM) {
		e.partitionsLock.Unlock()
		if unloadPM != nil {
			unloadPM.Stop(unloadCause)
		}
		return false
	}
	delete(e.partitions, key)
	e.partitionsLock.Unlock()
	foundTQM.Stop(unloadCause)
	return true
}

// Responsible for emitting and updating loaded_physical_task_queue_count metric
func (e *matchingEngineImpl) updatePhysicalTaskQueueGauge(pm *physicalTaskQueueManagerImpl, delta int) {

	// calculating versioned to be one of: “unversioned” or "buildId” or “versionSet”
	versioned := "unversioned"
	if buildID := pm.queue.BuildId(); buildID != "" {
		versioned = "buildId"
	} else if versionSet := pm.queue.VersionSet(); versionSet != "" {
		versioned = "versionSet"
	}

	physicalTaskQueueParameters := taskQueueCounterKey{
		namespaceID:   pm.partitionMgr.Partition().NamespaceId(),
		taskType:      pm.partitionMgr.Partition().TaskType(),
		partitionType: pm.partitionMgr.Partition().Kind(),
		versioned:     versioned,
	}

	e.gaugeMetrics.lock.Lock()
	defer e.gaugeMetrics.lock.Unlock()
	e.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters] += delta
	loadedPhysicalTaskQueueCounter := e.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters]

	pmImpl := pm.partitionMgr

	e.metricsHandler.Gauge(metrics.LoadedPhysicalTaskQueueGauge.Name()).Record(
		float64(loadedPhysicalTaskQueueCounter),
		metrics.NamespaceTag(pmImpl.ns.Name().String()),
		metrics.TaskTypeTag(physicalTaskQueueParameters.taskType.String()),
		metrics.PartitionTypeTag(physicalTaskQueueParameters.partitionType.String()),
		metrics.VersionedTag(versioned),
	)
}

// Responsible for emitting and updating loaded_task_queue_family_count, loaded_task_queue_count and
// loaded_task_queue_partition_count metrics
func (e *matchingEngineImpl) updateTaskQueuePartitionGauge(pm *taskQueuePartitionManagerImpl, delta int) {

	// each metric shall be accessed based on the mentioned parameters
	taskQueueFamilyParameters := taskQueueCounterKey{
		namespaceID: pm.Partition().NamespaceId(),
	}

	taskQueueParameters := taskQueueCounterKey{
		namespaceID: pm.Partition().NamespaceId(),
		taskType:    pm.Partition().TaskType(),
	}

	taskQueuePartitionParameters := taskQueueCounterKey{
		namespaceID:   pm.Partition().NamespaceId(),
		taskType:      pm.Partition().TaskType(),
		partitionType: pm.Partition().Kind(),
	}

	rootPartition := pm.Partition().IsRoot()
	e.gaugeMetrics.lock.Lock()
	defer e.gaugeMetrics.lock.Unlock()

	loadedTaskQueueFamilyCounter, loadedTaskQueueCounter, loadedTaskQueuePartitionCounter :=
		e.gaugeMetrics.loadedTaskQueueFamilyCount[taskQueueFamilyParameters], e.gaugeMetrics.loadedTaskQueueCount[taskQueueParameters],
		e.gaugeMetrics.loadedTaskQueuePartitionCount[taskQueuePartitionParameters]

	loadedTaskQueuePartitionCounter += delta
	e.gaugeMetrics.loadedTaskQueuePartitionCount[taskQueuePartitionParameters] = loadedTaskQueuePartitionCounter
	if rootPartition {
		loadedTaskQueueCounter += delta
		e.gaugeMetrics.loadedTaskQueueCount[taskQueueParameters] = loadedTaskQueueCounter
		if pm.Partition().TaskType() == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			loadedTaskQueueFamilyCounter += delta
			e.gaugeMetrics.loadedTaskQueueFamilyCount[taskQueueFamilyParameters] = loadedTaskQueueFamilyCounter
		}
	}

	e.metricsHandler.Gauge(metrics.LoadedTaskQueueFamilyGauge.Name()).Record(
		float64(loadedTaskQueueFamilyCounter),
		metrics.NamespaceTag(pm.ns.Name().String()),
	)

	metrics.LoadedTaskQueueGauge.With(e.metricsHandler).Record(
		float64(loadedTaskQueueCounter),
		metrics.NamespaceTag(pm.ns.Name().String()),
		metrics.TaskTypeTag(taskQueueParameters.taskType.String()),
	)

	metrics.LoadedTaskQueuePartitionGauge.With(e.metricsHandler).Record(
		float64(loadedTaskQueuePartitionCounter),
		metrics.NamespaceTag(pm.ns.Name().String()),
		metrics.TaskTypeTag(taskQueueParameters.taskType.String()),
		metrics.PartitionTypeTag(taskQueuePartitionParameters.partitionType.String()),
	)
}

// Populate the workflow task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollWorkflowTaskQueueResponse(
	task *internalTask,
	recordStartResp *historyservice.RecordWorkflowTaskStartedResponse,
	metricsHandler metrics.Handler,
) *matchingservice.PollWorkflowTaskQueueResponse {

	var serializedToken []byte
	if task.isQuery() {
		// for a query task
		queryRequest := task.query.request
		queryTaskToken := &tokenspb.QueryTask{
			NamespaceId: queryRequest.GetNamespaceId(),
			TaskQueue:   queryRequest.TaskQueue.Name,
			TaskId:      task.query.taskID,
		}
		serializedToken, _ = e.tokenSerializer.SerializeQueryTaskToken(queryTaskToken)
	} else {
		taskToken := tasktoken.NewWorkflowTaskToken(
			task.event.Data.GetNamespaceId(),
			task.event.Data.GetWorkflowId(),
			task.event.Data.GetRunId(),
			recordStartResp.GetScheduledEventId(),
			recordStartResp.GetStartedEventId(),
			recordStartResp.GetStartedTime(),
			recordStartResp.GetAttempt(),
			recordStartResp.GetClock(),
			recordStartResp.GetVersion(),
		)
		serializedToken, _ = e.tokenSerializer.Serialize(taskToken)
		if task.responseC == nil {
			ct := timestamp.TimeValue(task.event.Data.CreateTime)
			metrics.AsyncMatchLatencyPerTaskQueue.With(metricsHandler).Record(time.Since(ct))
		}
	}

	response := common.CreateMatchingPollWorkflowTaskQueueResponse(
		recordStartResp,
		task.workflowExecution(),
		serializedToken)

	if task.query != nil {
		response.Query = task.query.request.QueryRequest.Query
	}
	if task.backlogCountHint != nil {
		response.BacklogCountHint = task.backlogCountHint()
	}
	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollActivityTaskQueueResponse(
	task *internalTask,
	historyResponse *historyservice.RecordActivityTaskStartedResponse,
	metricsHandler metrics.Handler,
) *matchingservice.PollActivityTaskQueueResponse {

	scheduledEvent := historyResponse.ScheduledEvent
	if scheduledEvent.GetActivityTaskScheduledEventAttributes() == nil {
		panic("GetActivityTaskScheduledEventAttributes is not set")
	}
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()
	if attributes.ActivityId == "" {
		panic("ActivityTaskScheduledEventAttributes.ActivityID is not set")
	}
	if task.responseC == nil {
		ct := timestamp.TimeValue(task.event.Data.CreateTime)
		metrics.AsyncMatchLatencyPerTaskQueue.With(metricsHandler).Record(time.Since(ct))
	}

	taskToken := tasktoken.NewActivityTaskToken(
		task.event.Data.GetNamespaceId(),
		task.event.Data.GetWorkflowId(),
		task.event.Data.GetRunId(),
		task.event.Data.GetScheduledEventId(),
		attributes.GetActivityId(),
		attributes.GetActivityType().GetName(),
		historyResponse.GetAttempt(),
		historyResponse.GetClock(),
		historyResponse.GetVersion(),
	)
	serializedToken, _ := e.tokenSerializer.Serialize(taskToken)

	// This is here to ensure that this field is never nil as expected by the TS SDK.
	// This may happen if ScheduleActivityExecution was recorded in version 1.23.
	scheduleToCloseTimeout := attributes.ScheduleToCloseTimeout
	if scheduleToCloseTimeout == nil {
		scheduleToCloseTimeout = timestamp.DurationPtr(0)
	}

	return &matchingservice.PollActivityTaskQueueResponse{
		ActivityId:                  attributes.ActivityId,
		ActivityType:                attributes.ActivityType,
		Header:                      attributes.Header,
		Input:                       attributes.Input,
		WorkflowExecution:           task.workflowExecution(),
		CurrentAttemptScheduledTime: historyResponse.CurrentAttemptScheduledTime,
		ScheduledTime:               scheduledEvent.EventTime,
		ScheduleToCloseTimeout:      scheduleToCloseTimeout,
		StartedTime:                 historyResponse.StartedTime,
		StartToCloseTimeout:         attributes.StartToCloseTimeout,
		HeartbeatTimeout:            attributes.HeartbeatTimeout,
		TaskToken:                   serializedToken,
		Attempt:                     taskToken.Attempt,
		HeartbeatDetails:            historyResponse.HeartbeatDetails,
		WorkflowType:                historyResponse.WorkflowType,
		WorkflowNamespace:           historyResponse.WorkflowNamespace,
	}
}

func (e *matchingEngineImpl) recordWorkflowTaskStarted(
	ctx context.Context,
	pollReq *workflowservice.PollWorkflowTaskQueueRequest,
	task *internalTask,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	ctx, cancel := newRecordTaskStartedContext(ctx, task)
	defer cancel()

	return e.historyClient.RecordWorkflowTaskStarted(ctx, &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:         task.event.Data.GetNamespaceId(),
		WorkflowExecution:   task.workflowExecution(),
		ScheduledEventId:    task.event.Data.GetScheduledEventId(),
		Clock:               task.event.Data.GetClock(),
		RequestId:           uuid.New(),
		PollRequest:         pollReq,
		BuildIdRedirectInfo: task.redirectInfo,
	})
}

func (e *matchingEngineImpl) recordActivityTaskStarted(
	ctx context.Context,
	pollReq *workflowservice.PollActivityTaskQueueRequest,
	task *internalTask,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	ctx, cancel := newRecordTaskStartedContext(ctx, task)
	defer cancel()

	return e.historyClient.RecordActivityTaskStarted(ctx, &historyservice.RecordActivityTaskStartedRequest{
		NamespaceId:         task.event.Data.GetNamespaceId(),
		WorkflowExecution:   task.workflowExecution(),
		ScheduledEventId:    task.event.Data.GetScheduledEventId(),
		Clock:               task.event.Data.GetClock(),
		RequestId:           uuid.New(),
		PollRequest:         pollReq,
		BuildIdRedirectInfo: task.redirectInfo,
	})
}

// newRecordTaskStartedContext creates a context for recording
// activity or workflow task started. The parentCtx from
// pollActivity/WorkflowTaskQueue endpoint (which is a long poll
// API) has long timeout and unsuitable for recording task started,
// especially if the task is doing sync match and has caller
// (history transfer queue) waiting for response.
func newRecordTaskStartedContext(
	parentCtx context.Context,
	task *internalTask,
) (context.Context, context.CancelFunc) {
	timeout := recordTaskStartedDefaultTimeout
	if task.isSyncMatchTask() {
		timeout = recordTaskStartedSyncMatchTimeout
	}

	return context.WithTimeout(parentCtx, timeout)
}

// Revives a deleted build ID updating its HLC timestamp.
// Returns a new build ID leaving the provided one untouched.
func (e *matchingEngineImpl) reviveBuildId(ns *namespace.Namespace, taskQueue string, buildId *persistencespb.BuildId) *persistencespb.BuildId {
	// Bump the stamp and ensure it's newer than the deletion stamp.
	prevStamp := common.CloneProto(buildId.StateUpdateTimestamp)
	stamp := hlc.Next(prevStamp, e.timeSource)
	stamp.ClusterId = e.clusterMeta.GetClusterID()
	e.logger.Info("Revived build ID while applying replication event",
		tag.WorkflowNamespace(ns.Name().String()),
		tag.WorkflowTaskQueueName(taskQueue),
		tag.BuildId(buildId.Id))
	return &persistencespb.BuildId{
		Id:                     buildId.GetId(),
		State:                  persistencespb.STATE_ACTIVE,
		StateUpdateTimestamp:   stamp,
		BecameDefaultTimestamp: buildId.BecameDefaultTimestamp,
	}
}

// We use a very short timeout for considering a sticky worker available, since tasks can also
// be processed on the normal queue.
func stickyWorkerAvailable(pm taskQueuePartitionManager) bool {
	return pm != nil && pm.HasPollerAfter("", time.Now().Add(-stickyPollerUnavailableWindow))
}

// largerBacklogAge returns the larger BacklogAge
func largerBacklogAge(rootBacklogAge *durationpb.Duration, currentPartitionAge *durationpb.Duration) *durationpb.Duration {
	if rootBacklogAge.AsDuration() > currentPartitionAge.AsDuration() {
		return rootBacklogAge
	}
	return currentPartitionAge
}
