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
	"google.golang.org/protobuf/types/known/durationpb"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common/tqid"
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
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
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
	"go.temporal.io/server/common/worker_versioning"
)

const (
	// If sticky poller is not seem in last 10s, we treat it as sticky worker unavailable
	// This seems aggressive, but the default sticky schedule_to_start timeout is 5s, so 10s seems reasonable.
	stickyPollerUnavailableWindow = 10 * time.Second
	// If a compatible poller hasn't been seen for this time, we fail the CommitBuildId
	// Set to 70s so that it's a little over the max time a poller should be kept waiting.
	versioningPollerSeenWindow                       = 70 * time.Second
	versioningReachabilityDeletedRuleInclusionPeriod = 2 * time.Minute
	recordTaskStartedDefaultTimeout                  = 10 * time.Second
	recordTaskStartedSyncMatchTimeout                = 1 * time.Second
)

type (
	pollerIDCtxKey string
	identityCtxKey string

	// lockableQueryTaskMap maps query TaskID (which is a UUID generated in QueryWorkflow() call) to a channel
	// that QueryWorkflow() will block on. The channel is unblocked either by worker sending response through
	// RespondQueryTaskCompleted() or through an internal service error causing temporal to be unable to dispatch
	// query task to workflow worker.
	lockableQueryTaskMap struct {
		sync.RWMutex
		queryTaskMap map[string]chan *queryResult
	}

	lockablePollMap struct {
		sync.Mutex
		polls map[string]context.CancelFunc
	}

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
		status               int32
		taskManager          persistence.TaskManager
		historyClient        resource.HistoryClient
		matchingRawClient    resource.MatchingRawClient
		tokenSerializer      common.TaskTokenSerializer
		historySerializer    serialization.Serializer
		logger               log.Logger
		throttledLogger      log.ThrottledLogger
		namespaceRegistry    namespace.Registry
		keyResolver          membership.ServiceResolver
		clusterMeta          cluster.Metadata
		timeSource           clock.TimeSource
		visibilityManager    manager.VisibilityManager
		metricsHandler       metrics.Handler
		partitionsLock       sync.RWMutex // locks mutation of partitions
		partitions           map[tqid.PartitionKey]taskQueuePartitionManager
		gaugeMetrics         gaugeMetrics // per-namespace task queue counters
		config               *Config
		lockableQueryTaskMap lockableQueryTaskMap
		// pollMap is needed to keep track of all outstanding pollers for a particular
		// taskqueue.  PollerID generated by frontend is used as the key and CancelFunc is the
		// value.  This is used to cancel the context to unblock any outstanding poller when
		// the frontend detects client connection is closed to prevent tasks being dispatched
		// to zombie pollers.
		pollMap lockablePollMap
		// Only set if global namespaces are enabled on the cluster.
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		// Disables concurrent task queue user data updates and replication requests (due to a cassandra limitation)
		namespaceUpdateLockMap map[string]*namespaceUpdateLocks
		// Serializes access to the per namespace lock map
		namespaceUpdateLockMapLock sync.Mutex
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
	resolver membership.ServiceResolver,
	clusterMeta cluster.Metadata,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityManager manager.VisibilityManager,
) Engine {

	return &matchingEngineImpl{
		status:            common.DaemonStatusInitialized,
		taskManager:       taskManager,
		historyClient:     historyClient,
		matchingRawClient: matchingRawClient,
		tokenSerializer:   common.NewProtoTaskTokenSerializer(),
		historySerializer: serialization.NewSerializer(),
		logger:            log.With(logger, tag.ComponentMatchingEngine),
		throttledLogger:   log.With(throttledLogger, tag.ComponentMatchingEngine),
		namespaceRegistry: namespaceRegistry,
		keyResolver:       resolver,
		clusterMeta:       clusterMeta,
		timeSource:        clock.NewRealTimeSource(), // No need to mock this at the moment
		visibilityManager: visibilityManager,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingEngineScope)),
		partitions:        make(map[tqid.PartitionKey]taskQueuePartitionManager),
		gaugeMetrics: gaugeMetrics{
			loadedTaskQueueFamilyCount:    make(map[taskQueueCounterKey]int),
			loadedTaskQueueCount:          make(map[taskQueueCounterKey]int),
			loadedTaskQueuePartitionCount: make(map[taskQueueCounterKey]int),
			loadedPhysicalTaskQueueCount:  make(map[taskQueueCounterKey]int),
		},
		config:                    config,
		lockableQueryTaskMap:      lockableQueryTaskMap{queryTaskMap: make(map[string]chan *queryResult)},
		pollMap:                   lockablePollMap{polls: make(map[string]context.CancelFunc)},
		namespaceReplicationQueue: namespaceReplicationQueue,
		namespaceUpdateLockMap:    make(map[string]*namespaceUpdateLocks),
	}
}

func (e *matchingEngineImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
}

func (e *matchingEngineImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	for _, l := range e.getTaskQueues(math.MaxInt32) {
		l.Stop()
	}
}

func (e *matchingEngineImpl) getTaskQueues(maxCount int) (lists []taskQueuePartitionManager) {
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
	for _, l := range e.getTaskQueues(1000) {
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
) (taskQueuePartitionManager, error) {
	pm, err := e.getTaskQueuePartitionManagerNoWait(partition, create)
	if err != nil || pm == nil {
		return nil, err
	}
	if err = pm.WaitUntilInitialized(ctx); err != nil {
		e.unloadTaskQueuePartition(pm)
		return nil, err
	}
	return pm, nil
}

// Returns taskQueuePartitionManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This does not block for the task queue to be
// initialized.
func (e *matchingEngineImpl) getTaskQueuePartitionManagerNoWait(
	partition tqid.Partition,
	create bool,
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky)
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
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true)
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
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
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

			var hist *history.History
			var nextPageToken []byte
			if dynamicconfig.AccessHistory(e.config.FrontendAccessHistoryFraction, e.metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingPollWorkflowTaskQueueTag))) {
				hist, nextPageToken, err = e.getHistoryForQueryTask(ctx, namespaceID, task, isStickyEnabled)
			}

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
			// We remove build id from workerVersionCapabilities so History can differentiate between
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
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.WorkflowEventID(task.event.Data.GetScheduledEventId()),
					tag.Error(err),
				)
				task.finish(nil)
			case *serviceerrors.TaskAlreadyStarted:
				e.logger.Debug("Duplicated workflow task", tag.WorkflowTaskQueueName(taskQueueName), tag.TaskID(task.event.GetTaskId()))
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
			// We remove build id from workerVersionCapabilities so History can differentiate between
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky)
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
	e.lockableQueryTaskMap.put(taskID, queryResultCh)
	defer e.lockableQueryTaskMap.delete(taskID)

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
		return nil, ctx.Err()
	}
}

func (e *matchingEngineImpl) RespondQueryTaskCompleted(
	_ context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opMetrics metrics.Handler,
) error {
	if err := e.deliverQueryResult(request.GetTaskId(), &queryResult{workerResponse: request}); err != nil {
		opMetrics.Counter(metrics.RespondQueryTaskFailedPerTaskQueueCounter.Name()).Record(1)
		return err
	}
	return nil
}

func (e *matchingEngineImpl) deliverQueryResult(taskID string, queryResult *queryResult) error {
	queryResultCh, ok := e.lockableQueryTaskMap.get(taskID)
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
	e.pollMap.cancel(request.PollerId)
	return nil
}

func (e *matchingEngineImpl) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (*matchingservice.DescribeTaskQueueResponse, error) {
	req := request.GetDescRequest()
	if req.ApiMode == enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED {
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
					Versions:          req.GetVersions(),
					ReportBacklogInfo: false,
					ReportPollers:     req.GetReportTaskReachability(),
				})
				if err != nil {
					return nil, err
				}
				for _, vii := range partitionResp.VersionsInfoInternal {
					if _, ok := physicalInfoByBuildId[vii.BuildId]; !ok {
						physicalInfoByBuildId[vii.BuildId] = make(map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
					}
					if physInfo, ok := physicalInfoByBuildId[vii.BuildId][taskQueueType]; !ok {
						physicalInfoByBuildId[vii.BuildId][taskQueueType] = vii.PhysicalTaskQueueInfo
					} else {
						var bInfo *taskqueuepb.BacklogInfo
						if i == 0 { // root partition
							bInfo = vii.PhysicalTaskQueueInfo.BacklogInfo
						}
						merged := &taskqueuespb.PhysicalTaskQueueInfo{
							Pollers:     append(physInfo.GetPollers(), vii.PhysicalTaskQueueInfo.GetPollers()...),
							BacklogInfo: bInfo,
						}
						physicalInfoByBuildId[vii.BuildId][taskQueueType] = merged
					}
				}
			}
		}
		// smush internal info into versions info
		versionsInfo := make([]*taskqueuepb.TaskQueueVersionInfo, 0)
		for bid, typeMap := range physicalInfoByBuildId {
			typesInfo := make([]*taskqueuepb.TaskQueueTypeInfo, 0)
			for taskQueueType, physicalInfo := range typeMap {
				typesInfo = append(typesInfo, &taskqueuepb.TaskQueueTypeInfo{
					Type:    taskQueueType,
					Pollers: physicalInfo.Pollers,
				})
			}
			reachability, err := e.getBuildIdTaskReachability(ctx, request.GetNamespaceId(), req.GetNamespace(), req.GetTaskQueue().GetName(), bid)
			if err != nil {
				return nil, err
			}
			versionsInfo = append(versionsInfo, &taskqueuepb.TaskQueueVersionInfo{
				BuildId:          bid,
				TypesInfo:        typesInfo,
				TaskReachability: reachability,
			})
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true)
	if err != nil {
		return nil, err
	}
	return pm.LegacyDescribeTaskQueue(req.GetIncludeTaskQueueStatus()), nil
}

func (e *matchingEngineImpl) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	pm, err := e.getTaskQueuePartitionManager(ctx, tqid.PartitionFromPartitionProto(request.GetTaskQueuePartition(), request.GetNamespaceId()), true)
	if err != nil {
		return nil, err
	}
	buildIds, err := e.getBuildIds(request.GetVersions(), pm)
	if err != nil {
		return nil, err
	}
	return pm.Describe(buildIds, request.GetVersions().GetAllActive(), request.GetReportBacklogInfo(), request.GetReportPollers())
}

func (e *matchingEngineImpl) getBuildIds(
	versions *taskqueuepb.TaskQueueVersionSelection,
	tqMgr taskQueuePartitionManager) (map[string]bool, error) {
	buildIds := make(map[string]bool)
	if versions != nil {
		for _, bid := range versions.GetBuildIds() {
			buildIds[bid] = true
		}
		if versions.GetUnversioned() {
			buildIds[""] = true
		}
	} else {
		defaultBuildId, err := e.getDefaultBuildId(tqMgr)
		if err != nil {
			return nil, err
		}
		buildIds[defaultBuildId] = true
	}
	return buildIds, nil
}

// getDefaultBuildId gets the build id mentioned in the first unconditional Assignment Rule.
// If there is no default Build ID, the result for the unversioned queue will be returned.
func (e *matchingEngineImpl) getDefaultBuildId(tqMgr taskQueuePartitionManager) (string, error) {
	resp, err := e.getWorkerVersioningRules(tqMgr, nil)
	if err != nil {
		return "", err
	}
	for _, ar := range resp.GetResponse().GetAssignmentRules() {
		if isUnconditional(ar.GetRule()) {
			return ar.GetRule().GetTargetBuildId(), nil
		}
	}
	return "", nil
}

func (e *matchingEngineImpl) getBuildIdTaskReachability(
	ctx context.Context,
	nsID,
	nsName,
	taskQueue,
	buildId string,
) (enumspb.BuildIdTaskReachability, error) {
	getResp, err := e.GetWorkerVersioningRules(ctx, &matchingservice.GetWorkerVersioningRulesRequest{
		NamespaceId:                nsID,
		TaskQueue:                  taskQueue,
		DeletedRuleInclusionPeriod: durationpb.New(versioningReachabilityDeletedRuleInclusionPeriod),
	})
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	assignmentRules := getResp.GetResponse().GetAssignmentRules()
	redirectRules := getResp.GetResponse().GetCompatibleRedirectRules()

	// 1. Easy UNREACHABLE case
	if isTimestampedRedirectRuleSource(buildId, redirectRules) {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
	}

	// Gather list of all build ids that could point to buildId -> upstreamBuildIds
	upstreamBuildIds := getUpstreamBuildIds(buildId, redirectRules)
	buildIdsOfInterest := append(upstreamBuildIds, buildId)

	// 2. Cases for REACHABLE
	// 2a. If buildId is assignable to new tasks
	for _, bid := range buildIdsOfInterest {
		if isReachableAssignmentRuleTarget(bid, assignmentRules) {
			return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
		}
	}

	// 2b. If buildId could be reached from the backlog
	if existsBacklog, err := existsBackloggedActivityOrWFAssignedToAny(
		ctx, nsID, nsName, taskQueue, buildIdsOfInterest, e.matchingRawClient); err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	} else if existsBacklog {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// Note: The below cases are not applicable to activity-only task queues, since we don't record those in visibility

	// 2c. If buildId is assignable to tasks from open workflows
	existsOpenWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, e.visibilityManager, nsID, nsName, taskQueue, buildIdsOfInterest, true)
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsOpenWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, nil
	}

	// 3. Cases for CLOSED_WORKFLOWS_ONLY
	existsClosedWFAssignedToBuildId, err := existsWFAssignedToAny(ctx, e.visibilityManager, nsID, nsName, taskQueue, buildIdsOfInterest, false)
	if err != nil {
		return enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, err
	}
	if existsClosedWFAssignedToBuildId {
		return enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, nil
	}

	// 4. Otherwise, UNREACHABLE
	return enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, nil
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
	tqMgr, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true)
	if err != nil {
		return nil, err
	}

	// we don't set updateOptions.TaskQueueLimitPerBuildId, because the Versioning Rule limits will be checked separately
	// we don't set updateOptions.KnownVersion, because we handle external API call ordering with conflictToken
	updateOptions := UserDataUpdateOptions{}
	cT := req.GetConflictToken()
	var getResp *matchingservice.GetWorkerVersioningRulesResponse

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
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleRedirectRule:
			versioningData, err = ReplaceCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetReplaceCompatibleRedirectRule(),
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
		getResp, err = GetWorkerVersioningRules(versioningData, updatedClock, nil)
		if err != nil {
			return nil, false, err
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

	return &matchingservice.UpdateWorkerVersioningRulesResponse{Response: &workflowservice.UpdateWorkerVersioningRulesResponse{
		AssignmentRules:         getResp.GetResponse().GetAssignmentRules(),
		CompatibleRedirectRules: getResp.GetResponse().GetCompatibleRedirectRules(),
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
	tqMgr, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true)
	if err != nil {
		return nil, err
	}

	return e.getWorkerVersioningRules(tqMgr, request.GetDeletedRuleInclusionPeriod())
}

func (e *matchingEngineImpl) getWorkerVersioningRules(tqMgr taskQueuePartitionManager, deletedRuleInclusionPeriod *durationpb.Duration) (*matchingservice.GetWorkerVersioningRulesResponse, error) {
	data, _, err := tqMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	if data == nil {
		data = &persistencespb.VersionedTaskQueueUserData{Data: &persistencespb.TaskQueueUserData{}}
	} else {
		data = common.CloneProto(data)
	}
	clk := data.GetData().GetClock()
	if clk == nil {
		clk = hlc.Zero(e.clusterMeta.GetClusterID())
	}
	return GetWorkerVersioningRules(data.GetData().GetVersioningData(), clk, deletedRuleInclusionPeriod)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueue.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true)
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
		if err != nil {
			return nil, err
		}
		if req.WaitNewData && userData.GetVersion() == version {
			// long-poll: wait for data to change/appear
			select {
			case <-ctx.Done():
				resp.TaskQueueHasUserData = userData != nil
				return resp, nil
			case <-userDataChanged:
				continue
			}
		}
		if userData != nil {
			resp.TaskQueueHasUserData = true
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
	pm, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, p, true)
	if err != nil {
		return nil, err
	}
	if pm == nil {
		return &matchingservice.ForceUnloadTaskQueueResponse{WasLoaded: false}, nil
	}
	e.unloadTaskQueuePartition(pm)
	return &matchingservice.ForceUnloadTaskQueueResponse{WasLoaded: true}, nil
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
	host, err := e.keyResolver.Lookup(partitionKey)
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
	pm, err := e.getTaskQueuePartitionManager(ctx, partition, true)
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
		e.pollMap.add(pollerID, cancel)
		defer e.pollMap.remove(pollerID)
	}
	return pm.PollTask(ctx, pollMetadata)
}

func (e *matchingEngineImpl) unloadTaskQueuePartition(unloadPM taskQueuePartitionManager) {
	key := unloadPM.Partition().Key()
	e.partitionsLock.Lock()
	foundTQM, ok := e.partitions[key]
	if !ok || foundTQM != unloadPM {
		e.partitionsLock.Unlock()
		unloadPM.Stop()
		return
	}
	delete(e.partitions, key)
	e.partitionsLock.Unlock()
	foundTQM.Stop()
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

	e.metricsHandler.Gauge(metrics.LoadedTaskQueueGauge.Name()).Record(
		float64(loadedTaskQueueCounter),
		metrics.NamespaceTag(pm.ns.Name().String()),
		metrics.TaskTypeTag(taskQueueParameters.taskType.String()),
	)

	e.metricsHandler.Gauge(metrics.LoadedTaskQueuePartitionGauge.Name()).Record(
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
			metricsHandler.Timer(metrics.AsyncMatchLatencyPerTaskQueue.Name()).Record(time.Since(ct))
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
		metricsHandler.Timer(metrics.AsyncMatchLatencyPerTaskQueue.Name()).Record(time.Since(ct))
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

	return &matchingservice.PollActivityTaskQueueResponse{
		ActivityId:                  attributes.ActivityId,
		ActivityType:                attributes.ActivityType,
		Header:                      attributes.Header,
		Input:                       attributes.Input,
		WorkflowExecution:           task.workflowExecution(),
		CurrentAttemptScheduledTime: historyResponse.CurrentAttemptScheduledTime,
		ScheduledTime:               scheduledEvent.EventTime,
		ScheduleToCloseTimeout:      attributes.ScheduleToCloseTimeout,
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
		NamespaceId:       task.event.Data.GetNamespaceId(),
		WorkflowExecution: task.workflowExecution(),
		ScheduledEventId:  task.event.Data.GetScheduledEventId(),
		Clock:             task.event.Data.GetClock(),
		RequestId:         uuid.New(),
		PollRequest:       pollReq,
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
		NamespaceId:       task.event.Data.GetNamespaceId(),
		WorkflowExecution: task.workflowExecution(),
		ScheduledEventId:  task.event.Data.GetScheduledEventId(),
		Clock:             task.event.Data.GetClock(),
		RequestId:         uuid.New(),
		PollRequest:       pollReq,
	})
}

func (m *lockableQueryTaskMap) put(key string, value chan *queryResult) {
	m.Lock()
	defer m.Unlock()
	m.queryTaskMap[key] = value
}

func (m *lockableQueryTaskMap) get(key string) (chan *queryResult, bool) {
	m.RLock()
	defer m.RUnlock()
	result, ok := m.queryTaskMap[key]
	return result, ok
}

func (m *lockableQueryTaskMap) delete(key string) {
	m.Lock()
	defer m.Unlock()
	delete(m.queryTaskMap, key)
}

func (m *lockablePollMap) add(cancelId string, cancel context.CancelFunc) {
	m.Lock()
	defer m.Unlock()
	m.polls[cancelId] = cancel
}

func (m *lockablePollMap) remove(cancelId string) {
	m.Lock()
	defer m.Unlock()
	delete(m.polls, cancelId)
}

func (m *lockablePollMap) cancel(cancelId string) {
	m.Lock()
	defer m.Unlock()
	if cancel, ok := m.polls[cancelId]; ok {
		cancel()
		delete(m.polls, cancelId)
	}
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

// Revives a deleted build id updating its HLC timestamp.
// Returns a new build id leaving the provided one untouched.
func (e *matchingEngineImpl) reviveBuildId(ns *namespace.Namespace, taskQueue string, buildId *persistencespb.BuildId) *persistencespb.BuildId {
	// Bump the stamp and ensure it's newer than the deletion stamp.
	prevStamp := common.CloneProto(buildId.StateUpdateTimestamp)
	stamp := hlc.Next(prevStamp, e.timeSource)
	stamp.ClusterId = e.clusterMeta.GetClusterID()
	e.logger.Info("Revived build id while applying replication event",
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
