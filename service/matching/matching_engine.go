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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
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

	recordTaskStartedDefaultTimeout   = 10 * time.Second
	recordTaskStartedSyncMatchTimeout = 1 * time.Second
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
		namespaceID namespace.ID
		taskType    enumspb.TaskQueueType
		kind        enumspb.TaskQueueKind
		versioned   bool
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
		taskQueuesLock       sync.RWMutex // locks mutation of taskQueues
		taskQueues           map[taskQueueID]taskQueueManager
		taskQueueCountLock   sync.Mutex
		taskQueueCount       map[taskQueueCounterKey]int // per-namespace task queue counter
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
		status:                    common.DaemonStatusInitialized,
		taskManager:               taskManager,
		historyClient:             historyClient,
		matchingRawClient:         matchingRawClient,
		tokenSerializer:           common.NewProtoTaskTokenSerializer(),
		historySerializer:         serialization.NewSerializer(),
		logger:                    log.With(logger, tag.ComponentMatchingEngine),
		throttledLogger:           log.With(throttledLogger, tag.ComponentMatchingEngine),
		namespaceRegistry:         namespaceRegistry,
		keyResolver:               resolver,
		clusterMeta:               clusterMeta,
		timeSource:                clock.NewRealTimeSource(), // No need to mock this at the moment
		visibilityManager:         visibilityManager,
		metricsHandler:            metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingEngineScope)),
		taskQueues:                make(map[taskQueueID]taskQueueManager),
		taskQueueCount:            make(map[taskQueueCounterKey]int),
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

func (e *matchingEngineImpl) getTaskQueues(maxCount int) (lists []taskQueueManager) {
	e.taskQueuesLock.RLock()
	defer e.taskQueuesLock.RUnlock()
	lists = make([]taskQueueManager, 0, len(e.taskQueues))
	count := 0
	for _, tlMgr := range e.taskQueues {
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

// Returns taskQueueManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This blocks (up to the context deadline) for the
// task queue to be initialized.
//
// Note that stickyInfo is not used as part of the task queue identity. That means that if
// getTaskQueueManager is called twice with the same taskQueue but different stickyInfo, the
// properties of the taskQueueManager will depend on which call came first. In general, we can
// rely on kind being the same for all calls now, but normalName was a later addition to the
// protocol and is not always set consistently. normalName is only required when using
// versioning, and SDKs that support versioning will always set it. The current server version
// will also set it when adding tasks from history. So that particular inconsistency is okay.
func (e *matchingEngineImpl) getTaskQueueManager(
	ctx context.Context,
	taskQueue *taskQueueID,
	stickyInfo stickyInfo,
	create bool,
) (taskQueueManager, error) {
	tqm, err := e.getTaskQueueManagerNoWait(taskQueue, stickyInfo, create)
	if err != nil || tqm == nil {
		return nil, err
	}
	if err = tqm.WaitUntilInitialized(ctx); err != nil {
		return nil, err
	}
	return tqm, nil
}

// Returns taskQueueManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This does not block for the task queue to be
// initialized.
func (e *matchingEngineImpl) getTaskQueueManagerNoWait(
	taskQueue *taskQueueID,
	stickyInfo stickyInfo,
	create bool,
) (taskQueueManager, error) {
	e.taskQueuesLock.RLock()
	tqm, ok := e.taskQueues[*taskQueue]
	e.taskQueuesLock.RUnlock()
	if !ok {
		if !create {
			return nil, nil
		}

		// If it gets here, write lock and check again in case a task queue is created between the two locks
		e.taskQueuesLock.Lock()
		tqm, ok = e.taskQueues[*taskQueue]
		if !ok {
			var err error
			tqm, err = newTaskQueueManager(e, taskQueue, stickyInfo, e.config)
			if err != nil {
				e.taskQueuesLock.Unlock()
				return nil, err
			}
			e.taskQueues[*taskQueue] = tqm
		}
		e.taskQueuesLock.Unlock()

		if !ok {
			tqm.Start()
			e.updateTaskQueueGauge(tqm, 1)
		}
	}
	return tqm, nil
}

// For use in tests
func (e *matchingEngineImpl) updateTaskQueue(taskQueue *taskQueueID, mgr taskQueueManager) {
	e.taskQueuesLock.Lock()
	defer e.taskQueuesLock.Unlock()
	e.taskQueues[*taskQueue] = mgr
}

// AddWorkflowTask either delivers task directly to waiting poller or saves it into task queue persistence.
func (e *matchingEngineImpl) AddWorkflowTask(
	ctx context.Context,
	addRequest *matchingservice.AddWorkflowTaskRequest,
) (bool, error) {
	namespaceID := namespace.ID(addRequest.GetNamespaceId())
	taskQueueName := addRequest.TaskQueue.GetName()
	stickyInfo := stickyInfoFromTaskQueue(addRequest.TaskQueue)

	origTaskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return false, err
	}

	sticky := stickyInfo.kind == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	baseTqm, err := e.getTaskQueueManager(ctx, origTaskQueue, stickyInfo, !sticky)
	if err != nil {
		return false, err
	} else if sticky && !stickyWorkerAvailable(baseTqm) {
		return false, serviceerrors.NewStickyWorkerUnavailable()
	}

	// We don't need the userDataChanged channel here because:
	// - if we sync match or sticky worker unavailable, we're done
	// - if we spool to db, we'll re-resolve when it comes out of the db
	tqm, _, err := baseTqm.RedirectToVersionedQueueForAdd(ctx, addRequest.VersionDirective)
	if err != nil {
		return false, err
	}

	// This needs to move to history see - https://go.temporal.io/server/issues/181
	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := addRequest.GetScheduleToStartTimeout().AsDuration()
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      namespaceID.String(),
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		ExpiryTime:       expirationTime,
		CreateTime:       timestamppb.New(now),
		VersionDirective: addRequest.VersionDirective,
	}

	return tqm.AddTask(ctx, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
		baseTqm:       baseTqm,
	})
}

// AddActivityTask either delivers task directly to waiting poller or save it into task queue persistence.
func (e *matchingEngineImpl) AddActivityTask(
	ctx context.Context,
	addRequest *matchingservice.AddActivityTaskRequest,
) (bool, error) {
	namespaceID := namespace.ID(addRequest.GetNamespaceId())
	taskQueueName := addRequest.TaskQueue.GetName()
	stickyInfo := stickyInfoFromTaskQueue(addRequest.TaskQueue)

	origTaskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return false, err
	}

	baseTqm, err := e.getTaskQueueManager(ctx, origTaskQueue, stickyInfo, true)
	if err != nil {
		return false, err
	}
	// We don't need the userDataChanged channel here because:
	// - if we sync match, we're done
	// - if we spool to db, we'll re-resolve when it comes out of the db
	tqm, _, err := baseTqm.RedirectToVersionedQueueForAdd(ctx, addRequest.VersionDirective)
	if err != nil {
		return false, err
	}

	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := timestamp.DurationValue(addRequest.GetScheduleToStartTimeout())
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      namespaceID.String(),
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		CreateTime:       timestamppb.New(now),
		ExpiryTime:       expirationTime,
		VersionDirective: addRequest.VersionDirective,
	}

	return tqm.AddTask(ctx, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
		baseTqm:       baseTqm,
	})
}

func (e *matchingEngineImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	origTaskQueue *taskQueueID,
	stickyInfo stickyInfo,
) error {
	taskInfo := task.event.GetData()
	// This task came from taskReader so task.event is always set here.
	directive := taskInfo.GetVersionDirective()
	// If this came from a versioned queue, ignore the version and re-resolve, in case we're
	// going to the default and the default changed.
	unversionedOrigTaskQueue := newTaskQueueIDWithVersionSet(origTaskQueue, "")
	// Redirect and re-resolve if we're blocked in matcher and user data changes.
	for {
		// If normal queue: always load the base tqm to get versioning data.
		// If sticky queue: sticky is not versioned, so if we got here (by taskReader calling this),
		// the queue is already loaded.
		// So we can always use true here.
		baseTqm, err := e.getTaskQueueManager(ctx, unversionedOrigTaskQueue, stickyInfo, true)
		if err != nil {
			return err
		}
		tqm, userDataChanged, err := baseTqm.RedirectToVersionedQueueForAdd(ctx, directive)
		if err != nil {
			return err
		}
		err = tqm.DispatchSpooledTask(ctx, task, userDataChanged)
		if err != errInterrupted { // nolint:goerr113
			return err
		}
	}
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
	stickyInfo := stickyInfoFromTaskQueue(request.TaskQueue)
	e.logger.Debug("Received PollWorkflowTaskQueue for taskQueue", tag.WorkflowTaskQueueName(taskQueueName))
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
		taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return nil, err
		}
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			forwardedFrom:             req.GetForwardedSource(),
		}
		task, err := e.pollTask(pollerCtx, taskQueue, stickyInfo, pollMetadata)
		if err != nil {
			if err == errNoTasks {
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

		resp, err := e.recordWorkflowTaskStarted(ctx, request, task)
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
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()
	stickyInfo := stickyInfoFromTaskQueue(request.TaskQueue)
	e.logger.Debug("Received PollActivityTaskQueue for taskQueue", tag.Name(taskQueueName))
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}

		taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
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
		task, err := e.pollTask(pollerCtx, taskQueue, stickyInfo, pollMetadata)
		if err != nil {
			if err == errNoTasks {
				return emptyPollActivityTaskQueueResponse, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollActivityTaskQueueResponse(), nil
		}

		resp, err := e.recordActivityTaskStarted(ctx, request, task)
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
	namespaceID := namespace.ID(queryRequest.GetNamespaceId())
	taskQueueName := queryRequest.TaskQueue.GetName()
	stickyInfo := stickyInfoFromTaskQueue(queryRequest.TaskQueue)

	origTaskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	sticky := stickyInfo.kind == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	baseTqm, err := e.getTaskQueueManager(ctx, origTaskQueue, stickyInfo, !sticky)
	if err != nil {
		return nil, err
	} else if sticky && !stickyWorkerAvailable(baseTqm) {
		return nil, serviceerrors.NewStickyWorkerUnavailable()
	}

	// We don't need the userDataChanged channel here because we either do this sync (local or remote)
	// or fail with a relatively short timeout.
	tqm, _, err := baseTqm.RedirectToVersionedQueueForAdd(ctx, queryRequest.VersionDirective)
	if err != nil {
		return nil, err
	}

	taskID := uuid.New()
	resp, err := tqm.DispatchQueryTask(ctx, taskID, queryRequest)

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
	namespaceID := namespace.ID(request.GetNamespaceId())
	taskQueueType := request.DescRequest.GetTaskQueueType()
	taskQueueName := request.DescRequest.TaskQueue.GetName()
	stickyInfo := stickyInfoFromTaskQueue(request.DescRequest.TaskQueue)
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, taskQueueType)
	if err != nil {
		return nil, err
	}
	tlMgr, err := e.getTaskQueueManager(ctx, taskQueue, stickyInfo, true)
	if err != nil {
		return nil, err
	}

	return tlMgr.DescribeTaskQueue(request.DescRequest.GetIncludeTaskQueueStatus()), nil
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
	partitions, err := e.getAllPartitions(
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

func (e *matchingEngineImpl) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	req *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	taskQueueName := req.GetTaskQueue()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(ctx, taskQueue, normalStickyInfo, true)
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

	err = tqMgr.UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
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
		err = tqMgr.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
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
	namespaceID := namespace.ID(req.GetNamespaceId())
	taskQueueName := req.GetRequest().GetTaskQueue()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(ctx, taskQueue, normalStickyInfo, true)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			return &matchingservice.GetWorkerBuildIdCompatibilityResponse{}, nil
		}
		return nil, err
	}
	userData, _, err := tqMgr.GetUserData()
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
	namespaceID := namespace.ID(req.GetNamespaceId())
	taskQueue, err := newTaskQueueID(namespaceID, req.GetTaskQueue(), req.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(ctx, taskQueue, normalStickyInfo, true)
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
		tqMgr.MarkAlive()
	}

	for {
		resp := &matchingservice.GetTaskQueueUserDataResponse{}
		userData, userDataChanged, err := tqMgr.GetUserData()
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
	taskQueueName := req.GetTaskQueue()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(ctx, taskQueue, normalStickyInfo, true)
	if err != nil {
		return nil, err
	}
	updateOptions := UserDataUpdateOptions{
		// Avoid setting a limit to allow the replication event to always be applied
		TaskQueueLimitPerBuildId: 0,
	}
	err = tqMgr.UpdateUserData(ctx, updateOptions, func(current *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
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
	namespaceID := namespace.ID(req.GetNamespaceId())
	taskQueue, err := newTaskQueueID(namespaceID, req.TaskQueue, req.TaskQueueType)
	if err != nil {
		return nil, err
	}
	tqm, err := e.getTaskQueueManager(ctx, taskQueue, normalStickyInfo, false)
	if err != nil {
		return nil, err
	}
	if tqm == nil {
		return &matchingservice.ForceUnloadTaskQueueResponse{WasLoaded: false}, nil
	}
	e.unloadTaskQueue(tqm)
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

func (e *matchingEngineImpl) getAllPartitions(
	ns namespace.Name,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
) ([]string, error) {
	var partitionKeys []string
	namespaceID, err := e.namespaceRegistry.GetNamespaceID(ns)
	if err != nil {
		return partitionKeys, err
	}
	taskQueueID, err := newTaskQueueID(namespaceID, taskQueue.GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return partitionKeys, err
	}

	n := e.config.NumTaskqueueWritePartitions(ns.String(), taskQueueID.BaseNameString(), taskQueueType)
	for i := 0; i < n; i++ {
		partitionKeys = append(partitionKeys, taskQueueID.WithPartition(i).FullName())
	}

	return partitionKeys, nil
}

func (e *matchingEngineImpl) pollTask(
	ctx context.Context,
	origTaskQueue *taskQueueID,
	stickyInfo stickyInfo,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	baseTqm, err := e.getTaskQueueManager(ctx, origTaskQueue, stickyInfo, true)
	if err != nil {
		return nil, err
	}

	tqm, err := baseTqm.RedirectToVersionedQueueForPoll(ctx, pollMetadata.workerVersionCapabilities)
	if err != nil {
		return nil, err
	}

	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	ctx, cancel := newChildContext(ctx, baseTqm.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	if pollerID, ok := ctx.Value(pollerIDKey).(string); ok && pollerID != "" {
		e.pollMap.add(pollerID, cancel)
		defer e.pollMap.remove(pollerID)
	}

	if identity, ok := ctx.Value(identityKey).(string); ok && identity != "" {
		baseTqm.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
		// update timestamp when long poll ends
		defer baseTqm.UpdatePollerInfo(pollerIdentity(identity), pollMetadata)
	}

	return tqm.PollTask(ctx, pollMetadata)
}

func (e *matchingEngineImpl) unloadTaskQueue(unloadTQM taskQueueManager) {
	queueID := unloadTQM.QueueID()
	e.taskQueuesLock.Lock()
	foundTQM, ok := e.taskQueues[*queueID]
	if !ok || foundTQM != unloadTQM {
		e.taskQueuesLock.Unlock()
		return
	}
	delete(e.taskQueues, *queueID)
	e.taskQueuesLock.Unlock()
	// This may call unloadTaskQueue again but that's okay, the next call will not find it.
	foundTQM.Stop()
	e.updateTaskQueueGauge(foundTQM, -1)
}

func (e *matchingEngineImpl) updateTaskQueueGauge(tqm taskQueueManager, delta int) {
	id := tqm.QueueID()
	countKey := taskQueueCounterKey{
		namespaceID: id.namespaceID,
		taskType:    id.taskType,
		kind:        tqm.TaskQueueKind(),
		versioned:   id.VersionSet() != "",
	}

	e.taskQueueCountLock.Lock()
	e.taskQueueCount[countKey] += delta
	newCount := e.taskQueueCount[countKey]
	e.taskQueueCountLock.Unlock()

	nsEntry, err := e.namespaceRegistry.GetNamespaceByID(countKey.namespaceID)
	ns := namespace.Name("unknown")
	if err == nil {
		ns = nsEntry.Name()
	}

	e.metricsHandler.Gauge(metrics.LoadedTaskQueueGauge.Name()).Record(
		float64(newCount),
		metrics.NamespaceTag(ns.String()),
		metrics.TaskTypeTag(countKey.taskType.String()),
		metrics.QueueTypeTag(countKey.kind.String()),
		metrics.VersionedTag(countKey.versioned),
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
func stickyWorkerAvailable(tqm taskQueueManager) bool {
	return tqm != nil && tqm.HasPollerAfter(time.Now().Add(-stickyPollerUnavailableWindow))
}
