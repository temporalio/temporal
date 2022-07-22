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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

// If sticky poller is not seem in last 10s, we treat it as sticky worker unavailable
// This seems aggressive, but the default sticky schedule_to_start timeout is 5s, so 10s seems reasonable.
const stickyPollerUnavailableWindow = 10 * time.Second

// Implements matching.Engine
// TODO: Switch implementation from lock/channel based to a partitioned agent
// to simplify code and reduce possibility of synchronization errors.
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

	taskQueueCounterKey struct {
		namespaceID namespace.ID
		taskType    enumspb.TaskQueueType
		queueType   enumspb.TaskQueueKind
	}

	matchingEngineImpl struct {
		status               int32
		taskManager          persistence.TaskManager
		historyService       historyservice.HistoryServiceClient
		matchingClient       matchingservice.MatchingServiceClient
		tokenSerializer      common.TaskTokenSerializer
		logger               log.Logger
		metricsClient        metrics.Client
		taskQueuesLock       sync.RWMutex                     // locks mutation of taskQueues
		taskQueues           map[taskQueueID]taskQueueManager // Convert to LRU cache
		taskQueueCount       map[taskQueueCounterKey]int      // per-namespace task queue counter
		config               *Config
		lockableQueryTaskMap lockableQueryTaskMap
		namespaceRegistry    namespace.Registry
		keyResolver          membership.ServiceResolver
		clusterMeta          cluster.Metadata
	}
)

var (
	// EmptyPollWorkflowTaskQueueResponse is the response when there are no workflow tasks to hand out
	emptyPollWorkflowTaskQueueResponse = &matchingservice.PollWorkflowTaskQueueResponse{}
	// EmptyPollActivityTaskQueueResponse is the response when there are no activity tasks to hand out
	emptyPollActivityTaskQueueResponse = &matchingservice.PollActivityTaskQueueResponse{}
	persistenceOperationRetryPolicy    = common.CreatePersistenceRetryPolicy()
	historyServiceOperationRetryPolicy = common.CreateHistoryServiceRetryPolicy()

	// ErrNoTasks is exported temporarily for integration test
	ErrNoTasks    = errors.New("no tasks")
	errPumpClosed = errors.New("task queue pump closed its channel")

	pollerIDKey pollerIDCtxKey = "pollerID"
	identityKey identityCtxKey = "identity"
)

var _ Engine = (*matchingEngineImpl)(nil) // Asserts that interface is indeed implemented

// NewEngine creates an instance of matching engine
func NewEngine(taskManager persistence.TaskManager,
	historyService historyservice.HistoryServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
	config *Config,
	logger log.Logger,
	metricsClient metrics.Client,
	namespaceRegistry namespace.Registry,
	resolver membership.ServiceResolver,
	clusterMeta cluster.Metadata,
) Engine {

	return &matchingEngineImpl{
		status:               common.DaemonStatusInitialized,
		taskManager:          taskManager,
		historyService:       historyService,
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		taskQueues:           make(map[taskQueueID]taskQueueManager),
		taskQueueCount:       make(map[taskQueueCounterKey]int),
		logger:               log.With(logger, tag.ComponentMatchingEngine),
		metricsClient:        metricsClient,
		matchingClient:       matchingClient,
		config:               config,
		lockableQueryTaskMap: lockableQueryTaskMap{queryTaskMap: make(map[string]chan *queryResult)},
		namespaceRegistry:    namespaceRegistry,
		keyResolver:          resolver,
		clusterMeta:          clusterMeta,
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
func (e *matchingEngineImpl) getTaskQueueManager(ctx context.Context, taskQueue *taskQueueID, taskQueueKind enumspb.TaskQueueKind, create bool) (taskQueueManager, error) {
	e.taskQueuesLock.RLock()
	tqm, ok := e.taskQueues[*taskQueue]
	e.taskQueuesLock.RUnlock()

	if !ok {
		if !create {
			return nil, nil
		}

		// If it gets here, write lock and check again in case a task queue is created between the two locks
		e.taskQueuesLock.Lock()
		if tqm, ok = e.taskQueues[*taskQueue]; !ok {
			var err error
			tqm, err = newTaskQueueManager(e, taskQueue, taskQueueKind, e.config, e.clusterMeta)
			if err != nil {
				e.taskQueuesLock.Unlock()
				return nil, err
			}
			tqm.Start()
			e.taskQueues[*taskQueue] = tqm
			countKey := taskQueueCounterKey{namespaceID: taskQueue.namespaceID, taskType: taskQueue.taskType, queueType: taskQueueKind}
			e.taskQueueCount[countKey]++
			taskQueueCount := e.taskQueueCount[countKey]
			e.updateTaskQueueGauge(countKey, taskQueueCount)
		}
		e.taskQueuesLock.Unlock()
	}

	if err := tqm.WaitUntilInitialized(ctx); err != nil {
		return nil, err
	}

	return tqm, nil
}

// For use in tests
func (e *matchingEngineImpl) updateTaskQueue(taskQueue *taskQueueID, mgr taskQueueManager) {
	e.taskQueuesLock.Lock()
	defer e.taskQueuesLock.Unlock()
	e.taskQueues[*taskQueue] = mgr
}

// AddWorkflowTask either delivers task directly to waiting poller or save it into task queue persistence.
func (e *matchingEngineImpl) AddWorkflowTask(
	hCtx *handlerContext,
	addRequest *matchingservice.AddWorkflowTaskRequest,
) (bool, error) {
	namespaceID := namespace.ID(addRequest.GetNamespaceId())
	taskQueueName := addRequest.TaskQueue.GetName()
	taskQueueKind := addRequest.TaskQueue.GetKind()

	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return false, err
	}

	sticky := taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	tqm, err := e.getTaskQueueManager(hCtx, taskQueue, taskQueueKind, !sticky)
	if err != nil {
		return false, err
	} else if sticky && (tqm == nil || !tqm.HasPollerAfter(time.Now().Add(-stickyPollerUnavailableWindow))) {
		return false, serviceerrors.NewStickyWorkerUnavailable()
	}

	// This needs to move to history see - https://go.temporal.io/server/issues/181
	var expirationTime *time.Time
	now := timestamp.TimePtr(time.Now().UTC())
	expirationDuration := timestamp.DurationValue(addRequest.GetScheduleToStartTimeout())
	if expirationDuration == 0 {
		// noop
	} else {
		expirationTime = timestamp.TimePtr(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      namespaceID.String(),
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		ExpiryTime:       expirationTime,
		CreateTime:       now,
	}

	return tqm.AddTask(hCtx.Context, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
	})
}

// AddActivityTask either delivers task directly to waiting poller or save it into task queue persistence.
func (e *matchingEngineImpl) AddActivityTask(
	hCtx *handlerContext,
	addRequest *matchingservice.AddActivityTaskRequest,
) (bool, error) {
	namespaceID := namespace.ID(addRequest.GetNamespaceId())
	runID := addRequest.Execution.GetRunId()
	taskQueueName := addRequest.TaskQueue.GetName()
	taskQueueKind := addRequest.TaskQueue.GetKind()

	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return false, err
	}

	tlMgr, err := e.getTaskQueueManager(hCtx, taskQueue, taskQueueKind, true)
	if err != nil {
		return false, err
	}

	var expirationTime *time.Time
	now := timestamp.TimePtr(time.Now().UTC())
	expirationDuration := timestamp.DurationValue(addRequest.GetScheduleToStartTimeout())
	if expirationDuration == 0 {
		// noop
	} else {
		expirationTime = timestamp.TimePtr(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      namespaceID.String(),
		RunId:            runID,
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		CreateTime:       now,
		ExpiryTime:       expirationTime,
	}

	return tlMgr.AddTask(hCtx.Context, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		source:        addRequest.GetSource(),
		forwardedFrom: addRequest.GetForwardedSource(),
	})
}

// PollWorkflowTaskQueue tries to get the workflow task using exponential backoff.
func (e *matchingEngineImpl) PollWorkflowTaskQueue(
	hCtx *handlerContext,
	req *matchingservice.PollWorkflowTaskQueueRequest,
) (*matchingservice.PollWorkflowTaskQueueResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()
	e.logger.Debug("Received PollWorkflowTaskQueue for taskQueue", tag.WorkflowTaskQueueName(taskQueueName))
pollLoop:
	for {
		err := common.IsValidContext(hCtx.Context)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(hCtx.Context, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return nil, err
		}
		taskQueueKind := request.TaskQueue.GetKind()
		task, err := e.getTask(pollerCtx, taskQueue, nil, taskQueueKind)
		if err != nil {
			// TODO: Is empty poll the best reply for errPumpClosed?
			if err == ErrNoTasks || err == errPumpClosed {
				return emptyPollWorkflowTaskQueueResponse, nil
			}
			return nil, err
		}

		e.emitForwardedSourceStats(hCtx.scope, task.isForwarded(), req.GetForwardedSource())

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollWorkflowTaskQueueResponse(), nil
		}

		if task.isQuery() {
			task.finish(nil) // this only means query task sync match succeed.

			// for query task, we don't need to update history to record workflow task started. but we need to know
			// the NextEventID so front end knows what are the history events to load for this workflow task.
			mutableStateResp, err := e.historyService.GetMutableState(hCtx.Context, &historyservice.GetMutableStateRequest{
				NamespaceId: req.GetNamespaceId(),
				Execution:   task.workflowExecution(),
			})
			if err != nil {
				// will notify query client that the query task failed
				_ = e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err})
				return emptyPollWorkflowTaskQueueResponse, nil
			}

			isStickyEnabled := false
			if len(mutableStateResp.StickyTaskQueue.GetName()) != 0 {
				isStickyEnabled = true
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
			}
			return e.createPollWorkflowTaskQueueResponse(task, resp, hCtx.scope), nil
		}

		resp, err := e.recordWorkflowTaskStarted(hCtx.Context, request, task)
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
			}

			continue pollLoop
		}
		task.finish(nil)
		return e.createPollWorkflowTaskQueueResponse(task, resp, hCtx.scope), nil
	}
}

// PollActivityTaskQueue takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollActivityTaskQueue(
	hCtx *handlerContext,
	req *matchingservice.PollActivityTaskQueueRequest,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()
	e.logger.Debug("Received PollActivityTaskQueue for taskQueue", tag.Name(taskQueueName))
pollLoop:
	for {
		err := common.IsValidContext(hCtx.Context)
		if err != nil {
			return nil, err
		}

		taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		if err != nil {
			return nil, err
		}

		var maxDispatch *float64
		if request.TaskQueueMetadata != nil && request.TaskQueueMetadata.MaxTasksPerSecond != nil {
			maxDispatch = &request.TaskQueueMetadata.MaxTasksPerSecond.Value
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(hCtx.Context, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		taskQueueKind := request.TaskQueue.GetKind()
		task, err := e.getTask(pollerCtx, taskQueue, maxDispatch, taskQueueKind)
		if err != nil {
			// TODO: Is empty poll the best reply for errPumpClosed?
			if err == ErrNoTasks || err == errPumpClosed {
				return emptyPollActivityTaskQueueResponse, nil
			}
			return nil, err
		}

		e.emitForwardedSourceStats(hCtx.scope, task.isForwarded(), req.GetForwardedSource())

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollActivityTaskQueueResponse(), nil
		}

		resp, err := e.recordActivityTaskStarted(hCtx.Context, request, task)
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
			}

			continue pollLoop
		}
		task.finish(nil)
		return e.createPollActivityTaskQueueResponse(task, resp, hCtx.scope), nil
	}
}

type queryResult struct {
	workerResponse *matchingservice.RespondQueryTaskCompletedRequest
	internalError  error
}

// QueryWorkflow creates a WorkflowTask with query data, send it through sync match channel, wait for that WorkflowTask
// to be processed by worker, and then return the query result.
func (e *matchingEngineImpl) QueryWorkflow(
	hCtx *handlerContext,
	queryRequest *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	namespaceID := namespace.ID(queryRequest.GetNamespaceId())
	taskQueueName := queryRequest.TaskQueue.GetName()
	taskQueueKind := queryRequest.TaskQueue.GetKind()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	sticky := taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	tqm, err := e.getTaskQueueManager(hCtx, taskQueue, taskQueueKind, !sticky)
	if err != nil {
		return nil, err
	} else if sticky && (tqm == nil || !tqm.HasPollerAfter(time.Now().Add(-stickyPollerUnavailableWindow))) {
		return nil, serviceerrors.NewStickyWorkerUnavailable()
	}

	taskID := uuid.New()
	resp, err := tqm.DispatchQueryTask(hCtx.Context, taskID, queryRequest)

	// if get response or error it means that query task was handled by forwarding to another matching host
	// this remote host's result can be returned directly
	if resp != nil || err != nil {
		return resp, err
	}

	// if get here it means that dispatch of query task has occurred locally
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
	case <-hCtx.Done():
		return nil, hCtx.Err()
	}
}

func (e *matchingEngineImpl) RespondQueryTaskCompleted(
	hCtx *handlerContext,
	request *matchingservice.RespondQueryTaskCompletedRequest,
) error {
	if err := e.deliverQueryResult(request.GetTaskId(), &queryResult{workerResponse: request}); err != nil {
		hCtx.scope.IncCounter(metrics.RespondQueryTaskFailedPerTaskQueueCounter)
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
	hCtx *handlerContext,
	request *matchingservice.CancelOutstandingPollRequest,
) error {
	namespaceID := namespace.ID(request.GetNamespaceId())
	taskQueueType := request.GetTaskQueueType()
	taskQueueName := request.TaskQueue.GetName()
	pollerID := request.GetPollerId()

	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, taskQueueType)
	if err != nil {
		return err
	}
	taskQueueKind := request.TaskQueue.GetKind()
	tlMgr, err := e.getTaskQueueManager(hCtx, taskQueue, taskQueueKind, true)
	if err != nil {
		return err
	}

	tlMgr.CancelPoller(pollerID)
	return nil
}

func (e *matchingEngineImpl) DescribeTaskQueue(
	hCtx *handlerContext,
	request *matchingservice.DescribeTaskQueueRequest,
) (*matchingservice.DescribeTaskQueueResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	taskQueueType := request.DescRequest.GetTaskQueueType()
	taskQueueName := request.DescRequest.TaskQueue.GetName()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, taskQueueType)
	if err != nil {
		return nil, err
	}
	taskQueueKind := request.DescRequest.TaskQueue.GetKind()
	tlMgr, err := e.getTaskQueueManager(hCtx, taskQueue, taskQueueKind, true)
	if err != nil {
		return nil, err
	}

	return tlMgr.DescribeTaskQueue(request.DescRequest.GetIncludeTaskQueueStatus()), nil
}

func (e *matchingEngineImpl) ListTaskQueuePartitions(
	hCtx *handlerContext,
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
		*request.TaskQueue,
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

func (e *matchingEngineImpl) UpdateWorkerBuildIdOrdering(
	hCtx *handlerContext,
	req *matchingservice.UpdateWorkerBuildIdOrderingRequest,
) (*matchingservice.UpdateWorkerBuildIdOrderingResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	taskQueueName := req.GetRequest().GetTaskQueue()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(hCtx, taskQueue, enumspb.TASK_QUEUE_KIND_NORMAL, true)
	if err != nil {
		return nil, err
	}
	err = tqMgr.MutateVersioningData(hCtx.Context, func(data *persistencespb.VersioningData) error {
		return UpdateVersionsGraph(data, req.GetRequest(), e.config.MaxVersionGraphSize())
	})
	if err != nil {
		return nil, err
	}
	return &matchingservice.UpdateWorkerBuildIdOrderingResponse{}, nil
}

func (e *matchingEngineImpl) GetWorkerBuildIdOrdering(
	hCtx *handlerContext,
	req *matchingservice.GetWorkerBuildIdOrderingRequest,
) (*matchingservice.GetWorkerBuildIdOrderingResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	taskQueueName := req.GetRequest().GetTaskQueue()
	taskQueue, err := newTaskQueueID(namespaceID, taskQueueName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tqMgr, err := e.getTaskQueueManager(hCtx, taskQueue, enumspb.TASK_QUEUE_KIND_NORMAL, true)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			return &matchingservice.GetWorkerBuildIdOrderingResponse{}, nil
		}
		return nil, err
	}
	verDat, err := tqMgr.GetVersioningData(hCtx.Context)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			return &matchingservice.GetWorkerBuildIdOrderingResponse{}, nil
		}
		return nil, err
	}
	return &matchingservice.GetWorkerBuildIdOrderingResponse{
		Response: ToBuildIdOrderingResponse(verDat, int(req.GetRequest().GetMaxDepth())),
	}, nil
}

func (e *matchingEngineImpl) getHostInfo(partitionKey string) (string, error) {
	host, err := e.keyResolver.Lookup(partitionKey)
	if err != nil {
		return "", err
	}
	return host.GetAddress(), nil
}

func (e *matchingEngineImpl) getAllPartitions(
	namespace namespace.Name,
	taskQueue taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
) ([]string, error) {
	var partitionKeys []string
	namespaceID, err := e.namespaceRegistry.GetNamespaceID(namespace)
	if err != nil {
		return partitionKeys, err
	}
	taskQueueID, err := newTaskQueueID(namespaceID, taskQueue.GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return partitionKeys, err
	}
	rootPartition := taskQueueID.GetRoot()

	partitionKeys = append(partitionKeys, rootPartition)

	nWritePartitions := e.config.NumTaskqueueWritePartitions
	n := nWritePartitions(namespace.String(), rootPartition, taskQueueType)
	if n <= 0 {
		return partitionKeys, nil
	}

	for i := 1; i < n; i++ {
		partitionKeys = append(partitionKeys, fmt.Sprintf("%v%v/%v", taskQueuePartitionPrefix, rootPartition, i))
	}

	return partitionKeys, nil
}

// Loads a task from persistence and wraps it in a task context
func (e *matchingEngineImpl) getTask(
	ctx context.Context,
	taskQueue *taskQueueID,
	maxDispatchPerSecond *float64,
	taskQueueKind enumspb.TaskQueueKind,
) (*internalTask, error) {
	tlMgr, err := e.getTaskQueueManager(ctx, taskQueue, taskQueueKind, true)
	if err != nil {
		return nil, err
	}
	return tlMgr.GetTask(ctx, maxDispatchPerSecond)
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
	countKey := taskQueueCounterKey{namespaceID: queueID.namespaceID, taskType: queueID.taskType, queueType: foundTQM.TaskQueueKind()}
	e.taskQueueCount[countKey]--
	taskQueueCount := e.taskQueueCount[countKey]
	e.taskQueuesLock.Unlock()

	e.updateTaskQueueGauge(countKey, taskQueueCount)
	foundTQM.Stop()
}

func (e *matchingEngineImpl) updateTaskQueueGauge(countKey taskQueueCounterKey, taskQueueCount int) {
	nsEntry, err := e.namespaceRegistry.GetNamespaceByID(countKey.namespaceID)
	namespace := namespace.Name("unknown")
	if err == nil {
		namespace = nsEntry.Name()
	}

	e.metricsClient.Scope(
		metrics.MatchingEngineScope,
		metrics.NamespaceTag(namespace.String()),
		metrics.TaskTypeTag(countKey.taskType.String()),
		metrics.QueueTypeTag(countKey.queueType.String()),
	).UpdateGauge(metrics.LoadedTaskQueueGauge, float64(taskQueueCount))
}

// Populate the workflow task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollWorkflowTaskQueueResponse(
	task *internalTask,
	historyResponse *historyservice.RecordWorkflowTaskStartedResponse,
	scope metrics.Scope,
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
		taskToken := &tokenspb.Task{
			NamespaceId:      task.event.Data.GetNamespaceId(),
			WorkflowId:       task.event.Data.GetWorkflowId(),
			RunId:            task.event.Data.GetRunId(),
			ScheduledEventId: historyResponse.GetScheduledEventId(),
			Attempt:          historyResponse.GetAttempt(),
			Clock:            historyResponse.GetClock(),
		}
		serializedToken, _ = e.tokenSerializer.Serialize(taskToken)
		if task.responseC == nil {
			ct := timestamp.TimeValue(task.event.Data.CreateTime)
			scope.RecordTimer(metrics.AsyncMatchLatencyPerTaskQueue, time.Since(ct))
		}
	}

	response := common.CreateMatchingPollWorkflowTaskQueueResponse(
		historyResponse,
		task.workflowExecution(),
		serializedToken)

	if task.query != nil {
		response.Query = task.query.request.QueryRequest.Query
	}
	response.BacklogCountHint = task.backlogCountHint
	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollActivityTaskQueueResponse(
	task *internalTask,
	historyResponse *historyservice.RecordActivityTaskStartedResponse,
	scope metrics.Scope,
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
		scope.RecordTimer(metrics.AsyncMatchLatencyPerTaskQueue, time.Since(ct))
	}

	taskToken := &tokenspb.Task{
		NamespaceId:      task.event.Data.GetNamespaceId(),
		WorkflowId:       task.event.Data.GetWorkflowId(),
		RunId:            task.event.Data.GetRunId(),
		ScheduledEventId: task.event.Data.GetScheduledEventId(),
		Attempt:          historyResponse.GetAttempt(),
		ActivityId:       attributes.GetActivityId(),
		ActivityType:     attributes.GetActivityType().GetName(),
		Clock:            historyResponse.GetClock(),
	}

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
	request := &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:       task.event.Data.GetNamespaceId(),
		WorkflowExecution: task.workflowExecution(),
		ScheduledEventId:  task.event.Data.GetScheduledEventId(),
		Clock:             task.event.Data.GetClock(),
		TaskId:            task.event.GetTaskId(),
		RequestId:         uuid.New(),
		PollRequest:       pollReq,
	}
	var resp *historyservice.RecordWorkflowTaskStartedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = e.historyService.RecordWorkflowTaskStarted(ctx, request)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, historyServiceOperationRetryPolicy, func(err error) bool {
		switch err.(type) {
		case *serviceerror.NotFound, *serviceerror.NamespaceNotFound, *serviceerrors.TaskAlreadyStarted:
			return false
		}
		return true
	})
	return resp, err
}

func (e *matchingEngineImpl) recordActivityTaskStarted(
	ctx context.Context,
	pollReq *workflowservice.PollActivityTaskQueueRequest,
	task *internalTask,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	request := &historyservice.RecordActivityTaskStartedRequest{
		NamespaceId:       task.event.Data.GetNamespaceId(),
		WorkflowExecution: task.workflowExecution(),
		ScheduledEventId:  task.event.Data.GetScheduledEventId(),
		Clock:             task.event.Data.GetClock(),
		TaskId:            task.event.GetTaskId(),
		RequestId:         uuid.New(),
		PollRequest:       pollReq,
	}
	var resp *historyservice.RecordActivityTaskStartedResponse
	op := func(ctx context.Context) error {
		var err error
		resp, err = e.historyService.RecordActivityTaskStarted(ctx, request)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, historyServiceOperationRetryPolicy, func(err error) bool {
		switch err.(type) {
		case *serviceerror.NotFound, *serviceerror.NamespaceNotFound, *serviceerrors.TaskAlreadyStarted:
			return false
		}
		return true
	})
	return resp, err
}

func (e *matchingEngineImpl) emitForwardedSourceStats(
	scope metrics.Scope,
	isTaskForwarded bool,
	pollForwardedSource string,
) {
	isPollForwarded := len(pollForwardedSource) > 0
	switch {
	case isTaskForwarded && isPollForwarded:
		scope.IncCounter(metrics.RemoteToRemoteMatchPerTaskQueueCounter)
	case isTaskForwarded:
		scope.IncCounter(metrics.RemoteToLocalMatchPerTaskQueueCounter)
	case isPollForwarded:
		scope.IncCounter(metrics.LocalToRemoteMatchPerTaskQueueCounter)
	default:
		scope.IncCounter(metrics.LocalToLocalMatchPerTaskQueueCounter)
	}
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
