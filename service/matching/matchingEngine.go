// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/temporalio/temporal/common/membership"

	"github.com/pborman/uuid"

	h "github.com/temporalio/temporal/.gen/go/history"
	m "github.com/temporalio/temporal/.gen/go/matching"
	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/client"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
)

// Implements matching.Engine
// TODO: Switch implementation from lock/channel based to a partitioned agent
// to simplify code and reduce possibility of synchronization errors.
type (
	pollerIDCtxKey string
	identityCtxKey string

	// lockableQueryTaskMap maps query TaskID (which is a UUID generated in QueryWorkflow() call) to a channel
	// that QueryWorkflow() will block on. The channel is unblocked either by worker sending response through
	// RespondQueryTaskCompleted() or through an internal service error causing cadence to be unable to dispatch
	// query task to workflow worker.
	lockableQueryTaskMap struct {
		sync.Mutex
		queryTaskMap map[string]chan *queryResult
	}

	matchingEngineImpl struct {
		taskManager          persistence.TaskManager
		historyService       history.Client
		matchingClient       matching.Client
		tokenSerializer      common.TaskTokenSerializer
		logger               log.Logger
		metricsClient        metrics.Client
		taskListsLock        sync.RWMutex                   // locks mutation of taskLists
		taskLists            map[taskListID]taskListManager // Convert to LRU cache
		config               *Config
		lockableQueryTaskMap lockableQueryTaskMap
		domainCache          cache.DomainCache
		versionChecker       client.VersionChecker
		keyResolver          membership.ServiceResolver
	}
)

var (
	// EmptyPollForDecisionTaskResponse is the response when there are no decision tasks to hand out
	emptyPollForDecisionTaskResponse = &m.PollForDecisionTaskResponse{}
	// EmptyPollForActivityTaskResponse is the response when there are no activity tasks to hand out
	emptyPollForActivityTaskResponse   = &workflow.PollForActivityTaskResponse{}
	persistenceOperationRetryPolicy    = common.CreatePersistanceRetryPolicy()
	historyServiceOperationRetryPolicy = common.CreateHistoryServiceRetryPolicy()

	// ErrNoTasks is exported temporarily for integration test
	ErrNoTasks    = errors.New("No tasks")
	errPumpClosed = errors.New("Task list pump closed its channel")

	pollerIDKey pollerIDCtxKey = "pollerID"
	identityKey identityCtxKey = "identity"
)

var _ Engine = (*matchingEngineImpl)(nil) // Asserts that interface is indeed implemented

// NewEngine creates an instance of matching engine
func NewEngine(taskManager persistence.TaskManager,
	historyService history.Client,
	matchingClient matching.Client,
	config *Config,
	logger log.Logger,
	metricsClient metrics.Client,
	domainCache cache.DomainCache,
	resolver membership.ServiceResolver,
) Engine {

	return &matchingEngineImpl{
		taskManager:          taskManager,
		historyService:       historyService,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		taskLists:            make(map[taskListID]taskListManager),
		logger:               logger.WithTags(tag.ComponentMatchingEngine),
		metricsClient:        metricsClient,
		matchingClient:       matchingClient,
		config:               config,
		lockableQueryTaskMap: lockableQueryTaskMap{queryTaskMap: make(map[string]chan *queryResult)},
		domainCache:          domainCache,
		versionChecker:       client.NewVersionChecker(),
		keyResolver:          resolver,
	}
}

func (e *matchingEngineImpl) Start() {
	// As task lists are initialized lazily nothing is done on startup at this point.
}

func (e *matchingEngineImpl) Stop() {
	// Executes Stop() on each task list outside of lock
	for _, l := range e.getTaskLists(math.MaxInt32) {
		l.Stop()
	}
}

func (e *matchingEngineImpl) getTaskLists(maxCount int) (lists []taskListManager) {
	e.taskListsLock.RLock()
	defer e.taskListsLock.RUnlock()
	lists = make([]taskListManager, 0, len(e.taskLists))
	count := 0
	for _, tlMgr := range e.taskLists {
		lists = append(lists, tlMgr)
		count++
		if count >= maxCount {
			break
		}
	}
	return
}

func (e *matchingEngineImpl) String() string {
	// Executes taskList.String() on each task list outside of lock
	buf := new(bytes.Buffer)
	for _, l := range e.getTaskLists(1000) {
		fmt.Fprintf(buf, "\n%s", l.String())
	}
	return buf.String()
}

// Returns taskListManager for a task list. If not already cached gets new range from DB and
// if successful creates one.
func (e *matchingEngineImpl) getTaskListManager(taskList *taskListID,
	taskListKind *workflow.TaskListKind) (taskListManager, error) {
	// The first check is an optimization so almost all requests will have a task list manager
	// and return avoiding the write lock
	e.taskListsLock.RLock()
	if result, ok := e.taskLists[*taskList]; ok {
		e.taskListsLock.RUnlock()
		return result, nil
	}
	e.taskListsLock.RUnlock()
	// If it gets here, write lock and check again in case a task list is created between the two locks
	e.taskListsLock.Lock()
	if result, ok := e.taskLists[*taskList]; ok {
		e.taskListsLock.Unlock()
		return result, nil
	}
	e.logger.Info("", tag.LifeCycleStarting, tag.WorkflowTaskListName(taskList.name), tag.WorkflowTaskListType(taskList.taskType))
	mgr, err := newTaskListManager(e, taskList, taskListKind, e.config)
	if err != nil {
		e.taskListsLock.Unlock()
		e.logger.Info("", tag.LifeCycleStartFailed, tag.WorkflowTaskListName(taskList.name), tag.WorkflowTaskListType(taskList.taskType), tag.Error(err))
		return nil, err
	}
	e.taskLists[*taskList] = mgr
	e.taskListsLock.Unlock()
	err = mgr.Start()
	if err != nil {
		e.logger.Info("", tag.LifeCycleStartFailed, tag.WorkflowTaskListName(taskList.name), tag.WorkflowTaskListType(taskList.taskType), tag.Error(err))
		return nil, err
	}
	e.logger.Info("", tag.LifeCycleStarted, tag.WorkflowTaskListName(taskList.name), tag.WorkflowTaskListType(taskList.taskType))
	return mgr, nil
}

// For use in tests
func (e *matchingEngineImpl) updateTaskList(taskList *taskListID, mgr taskListManager) {
	e.taskListsLock.Lock()
	defer e.taskListsLock.Unlock()
	e.taskLists[*taskList] = mgr
}

func (e *matchingEngineImpl) removeTaskListManager(id *taskListID) {
	e.taskListsLock.Lock()
	defer e.taskListsLock.Unlock()
	delete(e.taskLists, *id)
}

// AddDecisionTask either delivers task directly to waiting poller or save it into task list persistence.
func (e *matchingEngineImpl) AddDecisionTask(ctx context.Context, addRequest *m.AddDecisionTaskRequest) (bool, error) {
	domainID := addRequest.GetDomainUUID()
	taskListName := addRequest.TaskList.GetName()
	taskListKind := common.TaskListKindPtr(addRequest.TaskList.GetKind())

	e.logger.Debug(
		fmt.Sprintf("Received AddDecisionTask for taskList=%v, WorkflowID=%v, RunID=%v, ScheduleToStartTimeout=%v",
			addRequest.TaskList.GetName(),
			addRequest.Execution.GetWorkflowId(),
			addRequest.Execution.GetRunId(),
			addRequest.GetScheduleToStartTimeoutSeconds()))

	taskList, err := newTaskListID(domainID, taskListName, persistence.TaskListTypeDecision)
	if err != nil {
		return false, err
	}

	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return false, err
	}

	taskInfo := &persistence.TaskInfo{
		DomainID:               domainID,
		RunID:                  addRequest.Execution.GetRunId(),
		WorkflowID:             addRequest.Execution.GetWorkflowId(),
		ScheduleID:             addRequest.GetScheduleId(),
		ScheduleToStartTimeout: addRequest.GetScheduleToStartTimeoutSeconds(),
		CreatedTime:            time.Now(),
	}
	return tlMgr.AddTask(ctx, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		forwardedFrom: addRequest.GetForwardedFrom(),
	})
}

// AddActivityTask either delivers task directly to waiting poller or save it into task list persistence.
func (e *matchingEngineImpl) AddActivityTask(ctx context.Context, addRequest *m.AddActivityTaskRequest) (bool, error) {
	domainID := addRequest.GetDomainUUID()
	sourceDomainID := addRequest.GetSourceDomainUUID()
	taskListName := addRequest.TaskList.GetName()
	taskListKind := common.TaskListKindPtr(addRequest.TaskList.GetKind())

	e.logger.Debug(
		fmt.Sprintf("Received AddActivityTask for taskList=%v WorkflowID=%v, RunID=%v",
			taskListName,
			addRequest.Execution.WorkflowId,
			addRequest.Execution.RunId))

	taskList, err := newTaskListID(domainID, taskListName, persistence.TaskListTypeActivity)
	if err != nil {
		return false, err
	}

	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return false, err
	}

	taskInfo := &persistence.TaskInfo{
		DomainID:               sourceDomainID,
		RunID:                  addRequest.Execution.GetRunId(),
		WorkflowID:             addRequest.Execution.GetWorkflowId(),
		ScheduleID:             addRequest.GetScheduleId(),
		ScheduleToStartTimeout: addRequest.GetScheduleToStartTimeoutSeconds(),
		CreatedTime:            time.Now(),
	}
	return tlMgr.AddTask(ctx, addTaskParams{
		execution:     addRequest.Execution,
		taskInfo:      taskInfo,
		forwardedFrom: addRequest.GetForwardedFrom(),
	})
}

// PollForDecisionTask tries to get the decision task using exponential backoff.
func (e *matchingEngineImpl) PollForDecisionTask(ctx context.Context, req *m.PollForDecisionTaskRequest) (
	*m.PollForDecisionTaskResponse, error) {
	domainID := req.GetDomainUUID()
	pollerID := req.GetPollerID()
	request := req.PollRequest
	taskListName := request.TaskList.GetName()
	e.logger.Debug("Received PollForDecisionTask for taskList", tag.WorkflowTaskListName(taskListName))
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so tasklistMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		taskList, err := newTaskListID(domainID, taskListName, persistence.TaskListTypeDecision)
		if err != nil {
			return nil, err
		}
		taskListKind := common.TaskListKindPtr(request.TaskList.GetKind())
		task, err := e.getTask(pollerCtx, taskList, nil, taskListKind)
		if err != nil {
			// TODO: Is empty poll the best reply for errPumpClosed?
			if err == ErrNoTasks || err == errPumpClosed {
				return emptyPollForDecisionTaskResponse, nil
			}
			return nil, err
		}

		e.emitForwardedFromStats(metrics.MatchingPollForDecisionTaskScope, task.isForwarded(), req.GetForwardedFrom())

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollForDecisionResponse(), nil
		}

		if task.isQuery() {
			task.finish(nil) // this only means query task sync match succeed.

			// for query task, we don't need to update history to record decision task started. but we need to know
			// the NextEventID so front end knows what are the history events to load for this decision task.
			mutableStateResp, err := e.historyService.GetMutableState(ctx, &h.GetMutableStateRequest{
				DomainUUID: req.DomainUUID,
				Execution:  task.workflowExecution(),
			})
			if err != nil {
				// will notify query client that the query task failed
				e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err}) //nolint:errcheck
				return emptyPollForDecisionTaskResponse, nil
			}

			isStickyEnabled := false
			supportsSticky := client.NewVersionChecker().SupportsStickyQuery(mutableStateResp.GetClientImpl(), mutableStateResp.GetClientFeatureVersion()) == nil
			if len(mutableStateResp.StickyTaskList.GetName()) != 0 && supportsSticky {
				isStickyEnabled = true
			}
			resp := &h.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId:    mutableStateResp.PreviousStartedEventId,
				NextEventId:               mutableStateResp.NextEventId,
				WorkflowType:              mutableStateResp.WorkflowType,
				StickyExecutionEnabled:    common.BoolPtr(isStickyEnabled),
				WorkflowExecutionTaskList: mutableStateResp.TaskList,
				BranchToken:               mutableStateResp.CurrentBranchToken,
			}
			return e.createPollForDecisionTaskResponse(task, resp), nil
		}

		resp, err := e.recordDecisionTaskStarted(ctx, request, task)
		if err != nil {
			switch err.(type) {
			case *workflow.EntityNotExistsError, *h.EventAlreadyStartedError:
				e.logger.Debug("Duplicated decision task",
					tag.Name(taskListName), tag.TaskID(task.event.TaskID))
				task.finish(nil)
			default:
				task.finish(err)
			}

			continue pollLoop
		}
		task.finish(nil)
		return e.createPollForDecisionTaskResponse(task, resp), nil
	}
}

// pollForActivityTaskOperation takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollForActivityTask(ctx context.Context, req *m.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	domainID := req.GetDomainUUID()
	pollerID := req.GetPollerID()
	request := req.PollRequest
	taskListName := request.TaskList.GetName()
	e.logger.Debug("Received PollForActivityTask for taskList", tag.Name(taskListName))
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}

		taskList, err := newTaskListID(domainID, taskListName, persistence.TaskListTypeActivity)
		if err != nil {
			return nil, err
		}

		var maxDispatch *float64
		if request.TaskListMetadata != nil {
			maxDispatch = request.TaskListMetadata.MaxTasksPerSecond
		}
		// Add frontend generated pollerID to context so tasklistMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		taskListKind := common.TaskListKindPtr(request.TaskList.GetKind())
		task, err := e.getTask(pollerCtx, taskList, maxDispatch, taskListKind)
		if err != nil {
			// TODO: Is empty poll the best reply for errPumpClosed?
			if err == ErrNoTasks || err == errPumpClosed {
				return emptyPollForActivityTaskResponse, nil
			}
			return nil, err
		}

		e.emitForwardedFromStats(metrics.MatchingPollForActivityTaskScope, task.isForwarded(), req.GetForwardedFrom())

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollForActivityResponse(), nil
		}

		resp, err := e.recordActivityTaskStarted(ctx, request, task)
		if err != nil {
			switch err.(type) {
			case *workflow.EntityNotExistsError, *h.EventAlreadyStartedError:
				e.logger.Debug("Duplicated activity task", tag.Name(taskListName), tag.TaskID(task.event.TaskID))
				task.finish(nil)
			default:
				task.finish(err)
			}

			continue pollLoop
		}
		task.finish(nil)
		return e.createPollForActivityTaskResponse(task, resp), nil
	}
}

type queryResult struct {
	workerResponse *m.RespondQueryTaskCompletedRequest
	internalError  error
}

// QueryWorkflow creates a DecisionTask with query data, send it through sync match channel, wait for that DecisionTask
// to be processed by worker, and then return the query result.
func (e *matchingEngineImpl) QueryWorkflow(ctx context.Context, queryRequest *m.QueryWorkflowRequest) (*workflow.QueryWorkflowResponse, error) {
	domainID := queryRequest.GetDomainUUID()
	taskListName := queryRequest.TaskList.GetName()
	taskListKind := common.TaskListKindPtr(queryRequest.TaskList.GetKind())
	taskList, err := newTaskListID(domainID, taskListName, persistence.TaskListTypeDecision)
	if err != nil {
		return nil, err
	}

	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return nil, err
	}
	taskID := uuid.New()
	resp, err := tlMgr.DispatchQueryTask(ctx, taskID, queryRequest)

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
		// if query was intended as consistent query check to see if worker supports consistent query
		if queryRequest.GetQueryRequest().GetQueryConsistencyLevel() == workflow.QueryConsistencyLevelStrong {
			if err := e.versionChecker.SupportsConsistentQuery(
				workerResponse.GetCompletedRequest().GetWorkerVersionInfo().GetImpl(),
				workerResponse.GetCompletedRequest().GetWorkerVersionInfo().GetFeatureVersion()); err != nil {
				// TODO: this error is swallowed and client gets "deadline exceeded" instead.
				return nil, err
			}
		}

		switch workerResponse.GetCompletedRequest().GetCompletedType() {
		case workflow.QueryTaskCompletedTypeCompleted:
			return &workflow.QueryWorkflowResponse{QueryResult: workerResponse.GetCompletedRequest().GetQueryResult()}, nil
		case workflow.QueryTaskCompletedTypeFailed:
			return nil, &workflow.QueryFailedError{Message: workerResponse.GetCompletedRequest().GetErrorMessage()}
		default:
			return nil, &workflow.InternalServiceError{Message: "unknown query completed type"}
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *matchingEngineImpl) RespondQueryTaskCompleted(ctx context.Context, request *m.RespondQueryTaskCompletedRequest) error {
	if err := e.deliverQueryResult(request.GetTaskID(), &queryResult{workerResponse: request}); err != nil {
		e.metricsClient.IncCounter(metrics.MatchingRespondQueryTaskCompletedScope, metrics.RespondQueryTaskFailedCounter)
		return err
	}
	return nil
}

func (e *matchingEngineImpl) deliverQueryResult(taskID string, queryResult *queryResult) error {
	queryResultCh, ok := e.lockableQueryTaskMap.get(taskID)
	if !ok {
		return &workflow.InternalServiceError{Message: "query task not found, or already expired"}
	}
	queryResultCh <- queryResult
	return nil
}

func (e *matchingEngineImpl) CancelOutstandingPoll(ctx context.Context, request *m.CancelOutstandingPollRequest) error {
	domainID := request.GetDomainUUID()
	taskListType := int(request.GetTaskListType())
	taskListName := request.TaskList.GetName()
	pollerID := request.GetPollerID()

	taskList, err := newTaskListID(domainID, taskListName, taskListType)
	if err != nil {
		return err
	}
	taskListKind := common.TaskListKindPtr(request.TaskList.GetKind())
	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return err
	}

	tlMgr.CancelPoller(pollerID)
	return nil
}

func (e *matchingEngineImpl) DescribeTaskList(ctx context.Context, request *m.DescribeTaskListRequest) (*workflow.DescribeTaskListResponse, error) {
	domainID := request.GetDomainUUID()
	taskListType := persistence.TaskListTypeDecision
	if request.DescRequest.GetTaskListType() == workflow.TaskListTypeActivity {
		taskListType = persistence.TaskListTypeActivity
	}
	taskListName := request.DescRequest.TaskList.GetName()
	taskList, err := newTaskListID(domainID, taskListName, taskListType)
	if err != nil {
		return nil, err
	}
	taskListKind := common.TaskListKindPtr(request.DescRequest.TaskList.GetKind())
	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return nil, err
	}

	return tlMgr.DescribeTaskList(request.DescRequest.GetIncludeTaskListStatus()), nil
}

func (e *matchingEngineImpl) ListTaskListPartitions(ctx context.Context, request *m.ListTaskListPartitionsRequest) (*workflow.ListTaskListPartitionsResponse, error) {
	activityTaskListInfo, err := e.listTaskListPartitions(request, persistence.TaskListTypeActivity)
	if err != nil {
		return nil, err
	}
	decisionTaskListInfo, err := e.listTaskListPartitions(request, persistence.TaskListTypeDecision)
	if err != nil {
		return nil, err
	}
	resp := workflow.ListTaskListPartitionsResponse{
		ActivityTaskListPartitions: activityTaskListInfo,
		DecisionTaskListPartitions: decisionTaskListInfo,
	}
	return &resp, nil
}

func (e *matchingEngineImpl) listTaskListPartitions(request *m.ListTaskListPartitionsRequest, taskListType int) ([]*workflow.TaskListPartitionMetadata, error) {
	partitions, err := e.getAllPartitions(
		request.GetDomain(),
		*request.TaskList,
		taskListType,
	)
	if err != nil {
		return nil, err
	}
	partitionHostInfo := make([]*workflow.TaskListPartitionMetadata, len(partitions))

	if err != nil {
		return nil, err
	}
	for _, partition := range partitions {
		if host, err := e.getHostInfo(partition); err != nil {
			partitionHostInfo = append(partitionHostInfo,
				&workflow.TaskListPartitionMetadata{
					Key:           common.StringPtr(partition),
					OwnerHostName: common.StringPtr(host),
				})
		}
	}
	return partitionHostInfo, nil
}

func (e *matchingEngineImpl) getHostInfo(partitionKey string) (string, error) {
	host, err := e.keyResolver.Lookup(partitionKey)
	if err != nil {
		return "", err
	}
	return host.GetAddress(), nil
}

func (e *matchingEngineImpl) getAllPartitions(
	domain string,
	taskList workflow.TaskList,
	taskListType int,
) ([]string, error) {
	var partitionKeys []string
	domainID, err := e.domainCache.GetDomainID(domain)
	if err != nil {
		return partitionKeys, err
	}
	taskListID, err := newTaskListID(domainID, taskList.GetName(), persistence.TaskListTypeDecision)
	rootPartition := taskListID.GetRoot()

	partitionKeys = append(partitionKeys, rootPartition)

	nWritePartitions := e.config.GetTasksBatchSize
	n := nWritePartitions(domain, rootPartition, taskListType)
	if n <= 0 {
		return partitionKeys, nil
	}

	for i := 1; i < n; i++ {
		partitionKeys = append(partitionKeys, fmt.Sprintf("%v%v/%v", taskListPartitionPrefix, rootPartition, i))
	}

	return partitionKeys, nil
}

// Loads a task from persistence and wraps it in a task context
func (e *matchingEngineImpl) getTask(
	ctx context.Context, taskList *taskListID, maxDispatchPerSecond *float64, taskListKind *workflow.TaskListKind,
) (*internalTask, error) {
	tlMgr, err := e.getTaskListManager(taskList, taskListKind)
	if err != nil {
		return nil, err
	}
	return tlMgr.GetTask(ctx, maxDispatchPerSecond)
}

func (e *matchingEngineImpl) unloadTaskList(id *taskListID) {
	e.taskListsLock.Lock()
	tlMgr, ok := e.taskLists[*id]
	if ok {
		delete(e.taskLists, *id)
	}
	e.taskListsLock.Unlock()
	if ok {
		tlMgr.Stop()
	}
}

// Populate the decision task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollForDecisionTaskResponse(
	task *internalTask,
	historyResponse *h.RecordDecisionTaskStartedResponse,
) *m.PollForDecisionTaskResponse {

	var token []byte
	if task.isQuery() {
		// for a query task
		queryRequest := task.query.request
		taskToken := &common.QueryTaskToken{
			DomainID: *queryRequest.DomainUUID,
			TaskList: *queryRequest.TaskList.Name,
			TaskID:   task.query.taskID,
		}
		token, _ = e.tokenSerializer.SerializeQueryTaskToken(taskToken)
	} else {
		taskoken := &common.TaskToken{
			DomainID:        task.event.DomainID,
			WorkflowID:      task.event.WorkflowID,
			RunID:           task.event.RunID,
			ScheduleID:      historyResponse.GetScheduledEventId(),
			ScheduleAttempt: historyResponse.GetAttempt(),
		}
		token, _ = e.tokenSerializer.Serialize(taskoken)
		if task.responseC == nil {
			scope := e.metricsClient.Scope(metrics.MatchingPollForDecisionTaskScope)
			scope.Tagged(metrics.DomainTag(task.domainName)).RecordTimer(metrics.AsyncMatchLatency, time.Since(task.event.CreatedTime))
		}
	}

	response := common.CreateMatchingPollForDecisionTaskResponse(historyResponse, task.workflowExecution(), token)
	if task.query != nil {
		response.Query = task.query.request.QueryRequest.Query
	}
	response.BacklogCountHint = common.Int64Ptr(task.backlogCountHint)
	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollForActivityTaskResponse(
	task *internalTask,
	historyResponse *h.RecordActivityTaskStartedResponse,
) *workflow.PollForActivityTaskResponse {

	scheduledEvent := historyResponse.ScheduledEvent
	if scheduledEvent.ActivityTaskScheduledEventAttributes == nil {
		panic("GetActivityTaskScheduledEventAttributes is not set")
	}
	attributes := scheduledEvent.ActivityTaskScheduledEventAttributes
	if attributes.ActivityId == nil {
		panic("ActivityTaskScheduledEventAttributes.ActivityID is not set")
	}
	if task.responseC == nil {
		scope := e.metricsClient.Scope(metrics.MatchingPollForActivityTaskScope)
		scope.Tagged(metrics.DomainTag(task.domainName)).RecordTimer(metrics.AsyncMatchLatency, time.Since(task.event.CreatedTime))
	}

	response := &workflow.PollForActivityTaskResponse{}
	response.ActivityId = attributes.ActivityId
	response.ActivityType = attributes.ActivityType
	response.Header = attributes.Header
	response.Input = attributes.Input
	response.WorkflowExecution = task.workflowExecution()
	response.ScheduledTimestampOfThisAttempt = historyResponse.ScheduledTimestampOfThisAttempt
	response.ScheduledTimestamp = common.Int64Ptr(*scheduledEvent.Timestamp)
	response.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(*attributes.ScheduleToCloseTimeoutSeconds)
	response.StartedTimestamp = historyResponse.StartedTimestamp
	response.StartToCloseTimeoutSeconds = common.Int32Ptr(*attributes.StartToCloseTimeoutSeconds)
	response.HeartbeatTimeoutSeconds = common.Int32Ptr(*attributes.HeartbeatTimeoutSeconds)

	token := &common.TaskToken{
		DomainID:        task.event.DomainID,
		WorkflowID:      task.event.WorkflowID,
		RunID:           task.event.RunID,
		ScheduleID:      task.event.ScheduleID,
		ScheduleAttempt: historyResponse.GetAttempt(),
	}

	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	response.Attempt = common.Int32Ptr(int32(token.ScheduleAttempt))
	response.HeartbeatDetails = historyResponse.HeartbeatDetails
	response.WorkflowType = historyResponse.WorkflowType
	response.WorkflowDomain = historyResponse.WorkflowDomain
	return response
}

func (e *matchingEngineImpl) recordDecisionTaskStarted(
	ctx context.Context,
	pollReq *workflow.PollForDecisionTaskRequest,
	task *internalTask,
) (*h.RecordDecisionTaskStartedResponse, error) {
	request := &h.RecordDecisionTaskStartedRequest{
		DomainUUID:        &task.event.DomainID,
		WorkflowExecution: task.workflowExecution(),
		ScheduleId:        &task.event.ScheduleID,
		TaskId:            &task.event.TaskID,
		RequestId:         common.StringPtr(uuid.New()),
		PollRequest:       pollReq,
	}
	var resp *h.RecordDecisionTaskStartedResponse
	op := func() error {
		var err error
		resp, err = e.historyService.RecordDecisionTaskStarted(ctx, request)
		return err
	}
	err := backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		switch err.(type) {
		case *workflow.EntityNotExistsError, *h.EventAlreadyStartedError:
			return false
		}
		return true
	})
	return resp, err
}

func (e *matchingEngineImpl) recordActivityTaskStarted(
	ctx context.Context,
	pollReq *workflow.PollForActivityTaskRequest,
	task *internalTask,
) (*h.RecordActivityTaskStartedResponse, error) {
	request := &h.RecordActivityTaskStartedRequest{
		DomainUUID:        &task.event.DomainID,
		WorkflowExecution: task.workflowExecution(),
		ScheduleId:        &task.event.ScheduleID,
		TaskId:            &task.event.TaskID,
		RequestId:         common.StringPtr(uuid.New()),
		PollRequest:       pollReq,
	}
	var resp *h.RecordActivityTaskStartedResponse
	op := func() error {
		var err error
		resp, err = e.historyService.RecordActivityTaskStarted(ctx, request)
		return err
	}
	err := backoff.Retry(op, historyServiceOperationRetryPolicy, func(err error) bool {
		switch err.(type) {
		case *workflow.EntityNotExistsError, *h.EventAlreadyStartedError:
			return false
		}
		return true
	})
	return resp, err
}

func (e *matchingEngineImpl) emitForwardedFromStats(scope int, isTaskForwarded bool, pollForwardedFrom string) {
	isPollForwarded := len(pollForwardedFrom) > 0
	switch {
	case isTaskForwarded && isPollForwarded:
		e.metricsClient.IncCounter(scope, metrics.RemoteToRemoteMatchCounter)
	case isTaskForwarded:
		e.metricsClient.IncCounter(scope, metrics.RemoteToLocalMatchCounter)
	case isPollForwarded:
		e.metricsClient.IncCounter(scope, metrics.LocalToRemoteMatchCounter)
	default:
		e.metricsClient.IncCounter(scope, metrics.LocalToLocalMatchCounter)
	}
}

func (m *lockableQueryTaskMap) put(key string, value chan *queryResult) {
	m.Lock()
	defer m.Unlock()
	m.queryTaskMap[key] = value
}

func (m *lockableQueryTaskMap) get(key string) (chan *queryResult, bool) {
	m.Lock()
	defer m.Unlock()
	result, ok := m.queryTaskMap[key]
	return result, ok
}

func (m *lockableQueryTaskMap) delete(key string) {
	m.Lock()
	defer m.Unlock()
	delete(m.queryTaskMap, key)
}
