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

package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encoding/json"

	"go.uber.org/yarpc"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	signalInputSizeLimit                     = 256 * 1024
	conditionalRetryCount                    = 5
	activityCancelationMsgActivityIDUnknown  = "ACTIVITY_ID_UNKNOWN"
	activityCancelationMsgActivityNotStarted = "ACTIVITY_ID_NOT_STARTED"
	timerCancelationMsgTimerIDUnknown        = "TIMER_ID_UNKNOWN"
)

type (
	historyEngineImpl struct {
		currentClusterName   string
		shard                ShardContext
		historyMgr           persistence.HistoryManager
		executionManager     persistence.ExecutionManager
		txProcessor          transferQueueProcessor
		timerProcessor       timerQueueProcessor
		replicator           *historyReplicator
		replicatorProcessor  queueProcessor
		historyEventNotifier historyEventNotifier
		tokenSerializer      common.TaskTokenSerializer
		hSerializerFactory   persistence.HistorySerializerFactory
		historyCache         *historyCache
		metricsClient        metrics.Client
		logger               bark.Logger
	}

	// shardContextWrapper wraps ShardContext to notify transferQueueProcessor on new tasks.
	// TODO: use to notify timerQueueProcessor as well.
	shardContextWrapper struct {
		currentClusterName string
		ShardContext
		txProcessor          transferQueueProcessor
		replcatorProcessor   queueProcessor
		historyEventNotifier historyEventNotifier
	}
)

var _ Engine = (*historyEngineImpl)(nil)

var (
	// ErrTaskRetry is the error indicating that the timer / transfer task should be retried.
	ErrTaskRetry = errors.New("passive task should retry due to condition in mutable state is not met")
	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("Duplicate task, completing it")
	// ErrConflict is exported temporarily for integration test
	ErrConflict = errors.New("Conditional update failed")
	// ErrMaxAttemptsExceeded is exported temporarily for integration test
	ErrMaxAttemptsExceeded = errors.New("Maximum attempts exceeded to update history")
	// ErrStaleState is the error returned during state update indicating that cached mutable state could be stale
	ErrStaleState = errors.New("Cache mutable state could potentially be stale")
	// ErrActivityTaskNotFound is the error to indicate activity task could be duplicate and activity already completed
	ErrActivityTaskNotFound = &workflow.EntityNotExistsError{Message: "Activity task not found."}
	// ErrWorkflowCompleted is the error to indicate workflow execution already completed
	ErrWorkflowCompleted = &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
	// ErrWorkflowParent is the error to parent execution is given and mismatch
	ErrWorkflowParent = &workflow.EntityNotExistsError{Message: "Workflow parent does not match."}
	// ErrDeserializingToken is the error to indicate task token is invalid
	ErrDeserializingToken = &workflow.BadRequestError{Message: "Error deserializing task token."}
	// ErrSignalOverSize is the error to indicate signal input size is > 256K
	ErrSignalOverSize = &workflow.BadRequestError{Message: "Signal input size is over 256K."}
	// ErrCancellationAlreadyRequested is the error indicating cancellation for target workflow is already requested
	ErrCancellationAlreadyRequested = &workflow.CancellationAlreadyRequestedError{Message: "Cancellation already requested for this workflow execution."}
	// ErrBufferedEventsLimitExceeded is the error indicating limit reached for maximum number of buffered events
	ErrBufferedEventsLimitExceeded = &workflow.LimitExceededError{Message: "Exceeded workflow execution limit for buffered events"}
	// FailedWorkflowCloseState is a set of failed workflow close states, used for start workflow policy
	// for start workflow execution API
	FailedWorkflowCloseState = map[int]bool{
		persistence.WorkflowCloseStatusFailed:     true,
		persistence.WorkflowCloseStatusCanceled:   true,
		persistence.WorkflowCloseStatusTerminated: true,
		persistence.WorkflowCloseStatusTimedOut:   true,
	}
)

// NewEngineWithShardContext creates an instance of history engine
func NewEngineWithShardContext(shard ShardContext, visibilityMgr persistence.VisibilityManager,
	matching matching.Client, historyClient hc.Client, historyEventNotifier historyEventNotifier, publisher messaging.Producer) Engine {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	shardWrapper := &shardContextWrapper{
		currentClusterName:   currentClusterName,
		ShardContext:         shard,
		historyEventNotifier: historyEventNotifier,
	}
	shard = shardWrapper
	logger := shard.GetLogger()
	executionManager := shard.GetExecutionManager()
	historyManager := shard.GetHistoryManager()
	historyCache := newHistoryCache(shard, logger)
	historySerializerFactory := persistence.NewHistorySerializerFactory()
	historyEngImpl := &historyEngineImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		historyMgr:         historyManager,
		executionManager:   executionManager,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: historySerializerFactory,
		historyCache:       historyCache,
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueHistoryEngineComponent,
		}),
		metricsClient:        shard.GetMetricsClient(),
		historyEventNotifier: historyEventNotifier,
	}
	txProcessor := newTransferQueueProcessor(shard, historyEngImpl, visibilityMgr, matching, historyClient, logger)
	historyEngImpl.timerProcessor = newTimerQueueProcessor(shard, historyEngImpl, matching, logger)
	historyEngImpl.txProcessor = txProcessor
	shardWrapper.txProcessor = txProcessor

	// Only start the replicator processor if valid publisher is passed in
	if publisher != nil {
		replicatorProcessor := newReplicatorQueueProcessor(shard, publisher, executionManager, historyManager,
			historySerializerFactory, logger)
		historyEngImpl.replicatorProcessor = replicatorProcessor
		shardWrapper.replcatorProcessor = replicatorProcessor
		historyEngImpl.replicator = newHistoryReplicator(shard, historyEngImpl, historyCache, shard.GetDomainCache(), historyManager,
			logger)
	}

	return historyEngImpl
}

// Start will spin up all the components needed to start serving this shard.
// Make sure all the components are loaded lazily so start can return immediately.  This is important because
// ShardController calls start sequentially for all the shards for a given host during startup.
func (e *historyEngineImpl) Start() {
	logging.LogHistoryEngineStartingEvent(e.logger)
	defer logging.LogHistoryEngineStartedEvent(e.logger)

	e.registerDomainFailoverCallback()

	e.txProcessor.Start()
	e.timerProcessor.Start()
	if e.replicatorProcessor != nil {
		e.replicatorProcessor.Start()
	}
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	logging.LogHistoryEngineShuttingDownEvent(e.logger)
	defer logging.LogHistoryEngineShutdownEvent(e.logger)

	e.txProcessor.Stop()
	e.timerProcessor.Stop()
	if e.replicatorProcessor != nil {
		e.replicatorProcessor.Stop()
	}

	// unset the failover callback
	e.shard.GetDomainCache().UnregisterDomainChangeCallback(e.shard.GetShardID())
}

func (e *historyEngineImpl) registerDomainFailoverCallback() {

	failoverPredicate := func(nextDomain *cache.DomainCacheEntry, action func()) {
		domainFailoverNotificationVersion := nextDomain.GetFailoverNotificationVersion()
		shardNotificationVersion := e.shard.GetDomainNotificationVersion()
		domainActiveCluster := nextDomain.GetReplicationConfig().ActiveClusterName

		if nextDomain.IsGlobalDomain() &&
			domainFailoverNotificationVersion >= shardNotificationVersion &&
			domainActiveCluster == e.currentClusterName {
			action()
		}
	}

	// first set the failover callback
	e.shard.GetDomainCache().RegisterDomainChangeCallback(
		e.shard.GetShardID(),
		e.shard.GetDomainCache().GetDomainNotificationVersion(),
		// before the domain change, this will be invoked when (most of time) domain cache is locked
		func(prevDomain *cache.DomainCacheEntry, nextDomain *cache.DomainCacheEntry) {
			e.logger.Infof("Domain Change Event: Shard: %v, Domain: %v, ID: %v, Failover Notification Version: %v, Active Cluster: %v, Shard Domain Notification Version: %v\n",
				e.shard.GetShardID(), nextDomain.GetInfo().Name, nextDomain.GetInfo().ID,
				nextDomain.GetFailoverNotificationVersion(), nextDomain.GetReplicationConfig().ActiveClusterName, e.shard.GetDomainNotificationVersion())

			failoverPredicate(nextDomain, func() {
				e.logger.Infof("Domain Failover Start: Shard: %v, Domain: %v, ID: %v\n",
					e.shard.GetShardID(), nextDomain.GetInfo().Name, nextDomain.GetInfo().ID)

				domainID := nextDomain.GetInfo().ID
				e.txProcessor.FailoverDomain(domainID)
				e.timerProcessor.FailoverDomain(domainID)
			})
		},
		// after the domain change, this will be invoked when domain cache is NOT locked
		func(prevDomain *cache.DomainCacheEntry, nextDomain *cache.DomainCacheEntry) {
			failoverPredicate(nextDomain, func() {
				e.logger.Infof("Domain Failover Notify Active: Shard: %v, Domain: %v, ID: %v\n",
					e.shard.GetShardID(), nextDomain.GetInfo().Name, nextDomain.GetInfo().ID)

				now := e.shard.GetTimeSource().Now()
				// the fake tasks will not be actually used, we just need to make sure
				// its length > 0 and has correct timestamp, to trigger a db scan
				fakeDecisionTask := []persistence.Task{&persistence.DecisionTask{}}
				fakeDecisionTimeoutTask := []persistence.Task{&persistence.DecisionTimeoutTask{VisibilityTimestamp: now}}
				e.txProcessor.NotifyNewTask(e.currentClusterName, fakeDecisionTask)
				e.timerProcessor.NotifyNewTimers(e.currentClusterName, now, fakeDecisionTimeoutTask)
			})
			e.shard.UpdateDomainNotificationVersion(nextDomain.GetNotificationVersion() + 1)
		},
	)
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(startRequest *h.StartWorkflowExecutionRequest) (
	*workflow.StartWorkflowExecutionResponse, error) {
	domainEntry, err := e.getActiveDomainEntry(startRequest.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	request := startRequest.StartRequest
	err = validateStartWorkflowExecutionRequest(request)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}

	var parentExecution *workflow.WorkflowExecution
	initiatedID := common.EmptyEventID
	parentDomainID := ""
	parentInfo := startRequest.ParentExecutionInfo
	if parentInfo != nil {
		parentDomainID = *parentInfo.DomainUUID
		parentExecution = parentInfo.Execution
		initiatedID = *parentInfo.InitiatedId
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()

	// Generate first decision task event.
	taskList := request.TaskList.GetName()
	// TODO when the workflow is going to be replicated, use the
	var msBuilder mutableState
	if clusterMetadata.IsGlobalDomainEnabled() && domainEntry.IsGlobalDomain() {
		// all workflows within a global domain should have replication state, no matter whether it will be replicated to multiple
		// target clusters or not
		msBuilder = newMutableStateBuilderWithReplicationState(
			clusterMetadata.GetCurrentClusterName(),
			e.shard.GetConfig(),
			e.logger,
			domainEntry.GetFailoverVersion(),
		)
	} else {
		msBuilder = newMutableStateBuilder(
			clusterMetadata.GetCurrentClusterName(),
			e.shard.GetConfig(),
			e.logger,
		)
	}
	startedEvent := msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if startedEvent == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	var transferTasks []persistence.Task
	decisionVersion := common.EmptyVersion
	decisionScheduleID := common.EmptyEventID
	decisionStartID := common.EmptyEventID
	decisionTimeout := int32(0)
	if parentInfo == nil {
		// DecisionTask is only created when it is not a Child Workflow Execution
		di := msBuilder.AddDecisionTaskScheduledEvent()
		if di == nil {
			return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
		}

		transferTasks = []persistence.Task{&persistence.DecisionTask{
			DomainID: domainID, TaskList: taskList, ScheduleID: di.ScheduleID,
		}}
		decisionVersion = di.Version
		decisionScheduleID = di.ScheduleID
		decisionStartID = di.StartedID
		decisionTimeout = di.DecisionTimeout
	}

	duration := time.Duration(*request.ExecutionStartToCloseTimeoutSeconds) * time.Second
	timerTasks := []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(duration),
	}}
	// Serialize the history
	serializedHistory, serializedError := msBuilder.GetHistoryBuilder().Serialize()
	if serializedError != nil {
		logging.LogHistorySerializationErrorEvent(e.logger, serializedError, fmt.Sprintf(
			"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v",
			execution.GetWorkflowId(), execution.GetRunId()))
		return nil, serializedError
	}

	err = e.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:  domainID,
		Execution: execution,
		// It is ok to use 0 for TransactionID because RunID is unique so there are
		// no potential duplicates to override.
		TransactionID:     0,
		FirstEventID:      startedEvent.GetEventId(),
		EventBatchVersion: startedEvent.GetVersion(),
		Events:            serializedHistory,
	})
	if err != nil {
		return nil, err
	}
	msBuilder.GetExecutionInfo().LastFirstEventID = startedEvent.GetEventId()

	var replicationState *persistence.ReplicationState
	var replicationTasks []persistence.Task
	if msBuilder.GetReplicationState() != nil {
		msBuilder.UpdateReplicationStateLastEventID(
			clusterMetadata.GetCurrentClusterName(),
			msBuilder.GetCurrentVersion(),
			msBuilder.GetNextEventID()-1,
		)
		replicationState = msBuilder.GetReplicationState()
		// this is a hack, only create replication task if have # target cluster > 1, for more see #868
		if domainEntry.CanReplicateEvent() {
			replicationTask := &persistence.HistoryReplicationTask{
				FirstEventID:        common.FirstEventID,
				NextEventID:         msBuilder.GetNextEventID(),
				Version:             msBuilder.GetCurrentVersion(),
				LastReplicationInfo: nil,
			}
			replicationTasks = append(replicationTasks, replicationTask)
		}
	}
	setTaskInfo(msBuilder.GetCurrentVersion(), time.Now(), transferTasks, timerTasks)

	createWorkflow := func(isBrandNew bool, prevRunID string) (string, error) {
		createRequest := &persistence.CreateWorkflowExecutionRequest{
			RequestID:                   common.StringDefault(request.RequestId),
			DomainID:                    domainID,
			Execution:                   execution,
			ParentDomainID:              parentDomainID,
			ParentExecution:             parentExecution,
			InitiatedID:                 initiatedID,
			TaskList:                    *request.TaskList.Name,
			WorkflowTypeName:            *request.WorkflowType.Name,
			WorkflowTimeout:             *request.ExecutionStartToCloseTimeoutSeconds,
			DecisionTimeoutValue:        *request.TaskStartToCloseTimeoutSeconds,
			ExecutionContext:            nil,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			ReplicationTasks:            replicationTasks,
			DecisionVersion:             decisionVersion,
			DecisionScheduleID:          decisionScheduleID,
			DecisionStartedID:           decisionStartID,
			DecisionStartToCloseTimeout: decisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               !isBrandNew,
			PreviousRunID:               prevRunID,
			ReplicationState:            replicationState,
			HasRetryPolicy:              request.RetryPolicy != nil,
		}
		if createRequest.HasRetryPolicy {
			createRequest.InitialInterval = request.RetryPolicy.GetInitialIntervalInSeconds()
			createRequest.BackoffCoefficient = request.RetryPolicy.GetBackoffCoefficient()
			createRequest.MaximumInterval = request.RetryPolicy.GetMaximumIntervalInSeconds()
			expireTimeNano := startedEvent.WorkflowExecutionStartedEventAttributes.GetExpirationTimestamp()
			if expireTimeNano > 0 {
				createRequest.ExpirationTime = time.Unix(0, expireTimeNano)
			}
			createRequest.MaximumAttempts = request.RetryPolicy.GetMaximumAttempts()
			createRequest.NonRetriableErrors = request.RetryPolicy.NonRetriableErrorReasons
		}
		_, err = e.shard.CreateWorkflowExecution(createRequest)

		if err != nil {
			switch t := err.(type) {
			case *persistence.WorkflowExecutionAlreadyStartedError:
				if t.StartRequestID == common.StringDefault(request.RequestId) {
					e.deleteEvents(domainID, execution)
					return t.RunID, nil
				}
			case *persistence.ShardOwnershipLostError:
				e.deleteEvents(domainID, execution)
			}
			return "", err
		}
		return execution.GetRunId(), nil
	}

	workflowExistsErrHandler := func(err *persistence.WorkflowExecutionAlreadyStartedError) error {
		// set the prev run ID for database conditional update
		prevStartRequestID := err.StartRequestID
		prevRunID := err.RunID
		prevState := err.State
		prevCloseState := err.CloseStatus

		errFn := func(errMsg string, createRequestID string, workflowID string, runID string) error {
			msg := fmt.Sprintf(errMsg, workflowID, runID)
			return &workflow.WorkflowExecutionAlreadyStartedError{
				Message:        common.StringPtr(msg),
				StartRequestId: common.StringPtr(fmt.Sprintf("%v", createRequestID)),
				RunId:          common.StringPtr(fmt.Sprintf("%v", runID)),
			}
		}

		// here we know there is some information about the prev workflow, i.e. either running right now
		// or has history check if the this workflow is finished
		if prevState != persistence.WorkflowStateCompleted {
			e.deleteEvents(domainID, execution)
			msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
			return errFn(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
		}
		switch startRequest.StartRequest.GetWorkflowIdReusePolicy() {
		case workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly:
			if _, ok := FailedWorkflowCloseState[prevCloseState]; !ok {
				e.deleteEvents(domainID, execution)
				msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow ID reuse policy: allow duplicate workflow ID if last run failed."
				return errFn(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
			}
		case workflow.WorkflowIdReusePolicyAllowDuplicate:
			// as long as workflow not running, so this case has no check
		case workflow.WorkflowIdReusePolicyRejectDuplicate:
			e.deleteEvents(domainID, execution)
			msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow ID reuse policy: reject duplicate workflow ID."
			return errFn(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
		default:
			e.deleteEvents(domainID, execution)
			return &workflow.InternalServiceError{Message: "Failed to process start workflow reuse policy."}
		}

		return nil
	}

	// try to create the workflow execution
	isBrandNew := true
	resultRunID := ""
	resultRunID, err = createWorkflow(isBrandNew, "")
	// if err still non nil, see if retry
	if errExist, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
		if err = workflowExistsErrHandler(errExist); err == nil {
			isBrandNew = false
			resultRunID, err = createWorkflow(isBrandNew, errExist.RunID)
		}
	}

	if err == nil {
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)

		return &workflow.StartWorkflowExecutionResponse{
			RunId: common.StringPtr(resultRunID),
		}, nil
	}
	return nil, err
}

// GetMutableState retrieves the mutable state of the workflow execution
func (e *historyEngineImpl) GetMutableState(ctx context.Context,
	request *h.GetMutableStateRequest) (*h.GetMutableStateResponse, error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	response, err := e.getMutableState(ctx, domainID, execution)
	if err != nil {
		return nil, err
	}
	// set the run id in case query the current running workflow
	execution.RunId = response.Execution.RunId

	// expectedNextEventID is 0 when caller want to get the current next event ID without blocking
	expectedNextEventID := common.FirstEventID
	if request.ExpectedNextEventId != nil {
		expectedNextEventID = request.GetExpectedNextEventId()
	}

	// if caller decide to long poll on workflow execution
	// and the event ID we are looking for is smaller than current next event ID
	if expectedNextEventID >= response.GetNextEventId() && response.GetIsWorkflowRunning() {
		subscriberID, channel, err := e.historyEventNotifier.WatchHistoryEvent(newWorkflowIdentifier(domainID, &execution))
		if err != nil {
			return nil, err
		}
		defer e.historyEventNotifier.UnwatchHistoryEvent(newWorkflowIdentifier(domainID, &execution), subscriberID)

		// check again in case the next event ID is updated
		response, err = e.getMutableState(ctx, domainID, execution)
		if err != nil {
			return nil, err
		}

		if expectedNextEventID < response.GetNextEventId() || !response.GetIsWorkflowRunning() {
			return response, nil
		}

		domainCache, err := e.shard.GetDomainCache().GetDomainByID(domainID)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(e.shard.GetConfig().LongPollExpirationInterval(domainCache.GetInfo().Name))
		defer timer.Stop()
		for {
			select {
			case event := <-channel:
				response.LastFirstEventId = common.Int64Ptr(event.lastFirstEventID)
				response.NextEventId = common.Int64Ptr(event.nextEventID)
				response.IsWorkflowRunning = common.BoolPtr(event.isWorkflowRunning)
				if expectedNextEventID < response.GetNextEventId() || !response.GetIsWorkflowRunning() {
					return response, nil
				}
			case <-timer.C:
				return response, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return response, nil
}

func (e *historyEngineImpl) getMutableState(ctx context.Context,
	domainID string, execution workflow.WorkflowExecution) (retResp *h.GetMutableStateResponse, retError error) {

	context, release, retError := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if retError != nil {
		return
	}
	defer func() { release(retError) }()

	msBuilder, retError := context.loadWorkflowExecution()
	if retError != nil {
		return
	}

	executionInfo := msBuilder.GetExecutionInfo()
	execution.RunId = context.workflowExecution.RunId
	retResp = &h.GetMutableStateResponse{
		Execution:                            &execution,
		WorkflowType:                         &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
		LastFirstEventId:                     common.Int64Ptr(msBuilder.GetLastFirstEventID()),
		NextEventId:                          common.Int64Ptr(msBuilder.GetNextEventID()),
		TaskList:                             &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
		StickyTaskList:                       &workflow.TaskList{Name: common.StringPtr(executionInfo.StickyTaskList)},
		ClientLibraryVersion:                 common.StringPtr(executionInfo.ClientLibraryVersion),
		ClientFeatureVersion:                 common.StringPtr(executionInfo.ClientFeatureVersion),
		ClientImpl:                           common.StringPtr(executionInfo.ClientImpl),
		IsWorkflowRunning:                    common.BoolPtr(msBuilder.IsWorkflowExecutionRunning()),
		StickyTaskListScheduleToStartTimeout: common.Int32Ptr(executionInfo.StickyScheduleToStartTimeout),
	}

	return
}

func (e *historyEngineImpl) DescribeMutableState(ctx context.Context,
	request *h.DescribeMutableStateRequest) (retResp *h.DescribeMutableStateResponse, retError error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	cacheCtx, dbCtx, release, cacheHit, err := e.historyCache.getAndCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()
	retResp = &h.DescribeMutableStateResponse{}

	if cacheHit && cacheCtx.msBuilder != nil {
		msb := cacheCtx.msBuilder
		retResp.MutableStateInCache, retError = e.toMutableStateJSON(msb)
	}

	msb, retError := dbCtx.loadWorkflowExecution()
	retResp.MutableStateInDatabase, retError = e.toMutableStateJSON(msb)

	return
}

func (e *historyEngineImpl) toMutableStateJSON(msb mutableState) (*string, error) {
	ms := msb.CopyToPersistence()

	jsonBytes, err := json.Marshal(ms)
	if err != nil {
		return nil, err
	}
	return common.StringPtr(string(jsonBytes)), nil
}

// ResetStickyTaskList reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskList
// 2. StickyScheduleToStartTimeout
// 3. ClientLibraryVersion
// 4. ClientFeatureVersion
// 5. ClientImpl
func (e *historyEngineImpl) ResetStickyTaskList(ctx context.Context, resetRequest *h.ResetStickyTaskListRequest) (*h.ResetStickyTaskListResponse, error) {
	domainID, err := validateDomainUUID(resetRequest.DomainUUID)
	if err != nil {
		return nil, err
	}

	err = e.updateWorkflowExecution(ctx, domainID, *resetRequest.Execution, false, false,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}
			msBuilder.ClearStickyness()
			return nil, nil
		},
	)

	if err != nil {
		return nil, err
	}
	return &h.ResetStickyTaskListResponse{}, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (e *historyEngineImpl) DescribeWorkflowExecution(ctx context.Context,
	request *h.DescribeWorkflowExecutionRequest) (retResp *workflow.DescribeWorkflowExecutionResponse, retError error) {
	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := *request.Request.Execution

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

	msBuilder, err1 := context.loadWorkflowExecution()
	if err1 != nil {
		return nil, err1
	}
	executionInfo := msBuilder.GetExecutionInfo()

	result := &workflow.DescribeWorkflowExecutionResponse{
		ExecutionConfiguration: &workflow.WorkflowExecutionConfiguration{
			TaskList: &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionInfo.WorkflowTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(executionInfo.DecisionTimeoutValue),
			ChildPolicy:                         common.ChildPolicyPtr(workflow.ChildPolicyTerminate),
		},
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Execution:     request.Request.Execution,
			Type:          &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
			StartTime:     common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
			HistoryLength: common.Int64Ptr(msBuilder.GetNextEventID() - common.FirstEventID),
		},
	}
	if executionInfo.State == persistence.WorkflowStateCompleted {
		// for closed workflow
		closeStatus := getWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		result.WorkflowExecutionInfo.CloseStatus = &closeStatus
		result.WorkflowExecutionInfo.CloseTime = common.Int64Ptr(msBuilder.GetLastUpdatedTimestamp())
	}

	if len(msBuilder.GetPendingActivityInfos()) > 0 {
		for _, pi := range msBuilder.GetPendingActivityInfos() {
			ai := &workflow.PendingActivityInfo{
				ActivityID: common.StringPtr(pi.ActivityID),
			}
			state := workflow.PendingActivityStateScheduled
			if pi.CancelRequested {
				state = workflow.PendingActivityStateCancelRequested
			} else if pi.StartedID != common.EmptyEventID {
				state = workflow.PendingActivityStateStarted
			}
			ai.State = &state
			lastHeartbeatUnixNano := pi.LastHeartBeatUpdatedTime.UnixNano()
			if lastHeartbeatUnixNano > 0 {
				ai.LastHeartbeatTimestamp = common.Int64Ptr(lastHeartbeatUnixNano)
				ai.HeartbeatDetails = pi.Details
			}
			if scheduledEvent, ok := msBuilder.GetHistoryEvent(pi.ScheduledEvent); ok {
				ai.ActivityType = scheduledEvent.ActivityTaskScheduledEventAttributes.ActivityType
			}
			result.PendingActivities = append(result.PendingActivities, ai)
		}
	}

	return result, nil
}

func (e *historyEngineImpl) RecordDecisionTaskStarted(ctx context.Context,
	request *h.RecordDecisionTaskStartedRequest) (retResp *h.RecordDecisionTaskStartedResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(request.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, *request.WorkflowExecution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

	scheduleID := request.GetScheduleId()
	requestID := request.GetRequestId()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err0 := context.loadWorkflowExecution()
		if err0 != nil {
			return nil, err0
		}
		tBuilder := e.getTimerBuilder(&context.workflowExecution)

		di, isRunning := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			e.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning {
			// Looks like DecisionTask already completed as a result of another call.
			// It is OK to drop the task at this point.
			logging.LogDuplicateTaskEvent(context.logger, persistence.TransferTaskTypeDecisionTask, common.Int64Default(request.TaskId), requestID,
				scheduleID, common.EmptyEventID, isRunning)

			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		if di.StartedID != common.EmptyEventID {
			// If decision is started as part of the current request scope then return a positive response
			if di.RequestID == requestID {
				return e.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.PollRequest.GetIdentity()), nil
			}

			// Looks like DecisionTask already started as a result of another call.
			// It is OK to drop the task at this point.
			logging.LogDuplicateTaskEvent(context.logger, persistence.TaskListTypeDecision, common.Int64Default(request.TaskId), requestID,
				scheduleID, di.StartedID, isRunning)

			return nil, &h.EventAlreadyStartedError{Message: "Decision task already started."}
		}

		_, di = msBuilder.AddDecisionTaskStartedEvent(scheduleID, requestID, request.PollRequest)
		if di == nil {
			// Unable to add DecisionTaskStarted event to history
			return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskStarted event to history."}
		}

		// Start a timer for the decision task.
		timeOutTask := tBuilder.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, di.DecisionTimeout)
		timerTasks := []persistence.Task{timeOutTask}
		defer e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)

		// Generate a transaction ID for appending events to history
		transactionID, err2 := e.shard.GetNextTransferTaskID()
		if err2 != nil {
			return nil, err2
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err3 := context.updateWorkflowExecution(nil, timerTasks, transactionID); err3 != nil {
			if err3 == ErrConflict {
				e.metricsClient.IncCounter(metrics.HistoryRecordDecisionTaskStartedScope,
					metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}
			return nil, err3
		}

		return e.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.PollRequest.GetIdentity()), nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (e *historyEngineImpl) RecordActivityTaskStarted(ctx context.Context,
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {

	domainEntry, err := e.getActiveDomainEntry(request.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	response := &h.RecordActivityTaskStartedResponse{}
	err = e.updateWorkflowExecution(ctx, domainID, execution, false, false,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := request.GetScheduleId()
			requestID := request.GetRequestId()
			ai, isRunning := msBuilder.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskStartedScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like ActivityTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				logging.LogDuplicateTaskEvent(e.logger, persistence.TransferTaskTypeActivityTask,
					common.Int64Default(request.TaskId), requestID, scheduleID, common.EmptyEventID, isRunning)

				return nil, ErrActivityTaskNotFound
			}

			scheduledEvent, exists := msBuilder.GetActivityScheduledEvent(scheduleID)
			if !exists {
				return nil, &workflow.InternalServiceError{Message: "Corrupted workflow execution state."}
			}
			response.ScheduledEvent = scheduledEvent
			response.ScheduledTimestampOfThisAttempt = common.Int64Ptr(ai.ScheduledTime.UnixNano())

			if ai.StartedID != common.EmptyEventID {
				// If activity is started as part of the current request scope then return a positive response
				if ai.RequestID == requestID {
					response.StartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
					response.Attempt = common.Int64Ptr(int64(ai.Attempt))
					return nil, nil
				}

				// Looks like ActivityTask already started as a result of another call.
				// It is OK to drop the task at this point.
				logging.LogDuplicateTaskEvent(e.logger, persistence.TransferTaskTypeActivityTask,
					common.Int64Default(request.TaskId), requestID, scheduleID, ai.StartedID, isRunning)

				return nil, &h.EventAlreadyStartedError{Message: "Activity task already started."}
			}

			msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, requestID, request.PollRequest.GetIdentity())

			response.StartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
			response.Attempt = common.Int64Ptr(int64(ai.Attempt))
			response.HeartbeatDetails = ai.Details

			// Start a timer for the activity task.
			timerTasks := []persistence.Task{}
			if tt := tBuilder.GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
				timerTasks = append(timerTasks, tt)
			}

			return timerTasks, nil
		})

	if err != nil {
		return nil, err
	}

	return response, err
}

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(ctx context.Context, req *h.RespondDecisionTaskCompletedRequest) (response *h.RespondDecisionTaskCompletedResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.CompleteRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	call := yarpc.CallFromContext(ctx)
	clientLibVersion := call.Header(common.LibraryVersionHeaderName)
	clientFeatureVersion := call.Header(common.FeatureVersionHeaderName)
	clientImpl := call.Header(common.ClientImplHeaderName)

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, workflowExecution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}
		executionInfo := msBuilder.GetExecutionInfo()
		tBuilder := e.getTimerBuilder(&context.workflowExecution)

		scheduleID := token.ScheduleID
		di, isRunning := msBuilder.GetPendingDecision(scheduleID)

		// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
		// some extreme cassandra failure cases.
		if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
			e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.StaleMutableStateCounter)
			// Reload workflow execution history
			context.clear()
			continue Update_History_Loop
		}

		if !msBuilder.IsWorkflowExecutionRunning() || !isRunning || di.Attempt != token.ScheduleAttempt ||
			di.StartedID == common.EmptyEventID {
			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		startedID := di.StartedID
		completedEvent := msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request)
		if completedEvent == nil {
			return nil, &workflow.InternalServiceError{Message: "Unable to add DecisionTaskCompleted event to history."}
		}

		failDecision := false
		var failCause workflow.DecisionTaskFailedCause
		var err error
		completedID := *completedEvent.EventId
		hasUnhandledEvents := msBuilder.HasBufferedEvents()
		isComplete := false
		transferTasks := []persistence.Task{}
		timerTasks := []persistence.Task{}
		var continueAsNewBuilder mutableState
		var continueAsNewTimerTasks []persistence.Task
		hasDecisionScheduleActivityTask := false

		if request.StickyAttributes == nil || request.StickyAttributes.WorkerTaskList == nil {
			e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyDisabledCounter)
			executionInfo.StickyTaskList = ""
			executionInfo.StickyScheduleToStartTimeout = 0
		} else {
			e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.CompleteDecisionWithStickyEnabledCounter)
			executionInfo.StickyTaskList = request.StickyAttributes.WorkerTaskList.GetName()
			executionInfo.StickyScheduleToStartTimeout = request.StickyAttributes.GetScheduleToStartTimeoutSeconds()
		}
		executionInfo.ClientLibraryVersion = clientLibVersion
		executionInfo.ClientFeatureVersion = clientFeatureVersion
		executionInfo.ClientImpl = clientImpl

	Process_Decision_Loop:
		for _, d := range request.Decisions {
			switch *d.DecisionType {
			case workflow.DecisionTypeScheduleActivityTask:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeScheduleActivityCounter)
				targetDomainID := domainID
				attributes := d.ScheduleActivityTaskDecisionAttributes
				// First check if we need to use a different target domain to schedule activity
				if attributes.Domain != nil {
					// TODO: Error handling for ActivitySchedule failed when domain lookup fails
					domainEntry, err := e.shard.GetDomainCache().GetDomain(*attributes.Domain)
					if err != nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to schedule activity across domain."}
					}
					targetDomainID = domainEntry.GetInfo().ID
				}

				if err = validateActivityScheduleAttributes(attributes, executionInfo.WorkflowTimeout); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadScheduleActivityAttributes
					break Process_Decision_Loop
				}

				scheduleEvent, _ := msBuilder.AddActivityTaskScheduledEvent(completedID, attributes)
				transferTasks = append(transferTasks, &persistence.ActivityTask{
					DomainID:   targetDomainID,
					TaskList:   *attributes.TaskList.Name,
					ScheduleID: *scheduleEvent.EventId,
				})
				hasDecisionScheduleActivityTask = true

			case workflow.DecisionTypeCompleteWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCompleteWorkflowCounter)
				if hasUnhandledEvents {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
					break Process_Decision_Loop
				}

				// If the decision has more than one completion event than just pick the first one
				if isComplete {
					e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.MultipleCompletionDecisionsCounter)
					logging.LogMultipleCompletionDecisionsEvent(e.logger, *d.DecisionType)
					continue Process_Decision_Loop
				}
				attributes := d.CompleteWorkflowExecutionDecisionAttributes
				if err = validateCompleteWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCompleteWorkflowExecutionAttributes
					break Process_Decision_Loop
				}
				if e := msBuilder.AddCompletedWorkflowEvent(completedID, attributes); e == nil {
					return nil, &workflow.InternalServiceError{Message: "Unable to add complete workflow event."}
				}
				isComplete = true
			case workflow.DecisionTypeFailWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeFailWorkflowCounter)
				if hasUnhandledEvents {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
					break Process_Decision_Loop
				}

				// If the decision has more than one completion event than just pick the first one
				if isComplete {
					e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.MultipleCompletionDecisionsCounter)
					logging.LogMultipleCompletionDecisionsEvent(e.logger, *d.DecisionType)
					continue Process_Decision_Loop
				}

				failedAttributes := d.FailWorkflowExecutionDecisionAttributes
				if err = validateFailWorkflowExecutionAttributes(failedAttributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadFailWorkflowExecutionAttributes
					break Process_Decision_Loop
				}

				retryBackoffInterval := msBuilder.GetRetryBackoffDuration(failedAttributes.GetReason())
				if retryBackoffInterval == common.NoRetryBackoff {
					// no retry
					if evt := msBuilder.AddFailWorkflowEvent(completedID, failedAttributes); evt == nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
					}
				} else {
					// retry with backoff
					startEvent, err := getWorkflowStartedEvent(e.historyMgr, e.logger, domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId())
					if err != nil {
						return nil, err
					}

					startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
					continueAsnewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
						WorkflowType: startAttributes.WorkflowType,
						TaskList:     startAttributes.TaskList,
						RetryPolicy:  startAttributes.RetryPolicy,
						Input:        startAttributes.Input,
						ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
						TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
						BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(retryBackoffInterval.Seconds())),
					}
					if _, continueAsNewBuilder, err = msBuilder.AddContinueAsNewEvent(completedID, domainEntry, startAttributes.GetParentWorkflowDomain(), continueAsnewAttributes); err != nil {
						return nil, err
					}
				}
				isComplete = true

			case workflow.DecisionTypeCancelWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelWorkflowCounter)
				// If new events came while we are processing the decision, we would fail this and give a chance to client
				// to process the new event.
				if hasUnhandledEvents {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
					break Process_Decision_Loop
				}

				// If the decision has more than one completion event than just pick the first one
				if isComplete {
					e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.MultipleCompletionDecisionsCounter)
					logging.LogMultipleCompletionDecisionsEvent(e.logger, *d.DecisionType)
					continue Process_Decision_Loop
				}
				attributes := d.CancelWorkflowExecutionDecisionAttributes
				if err = validateCancelWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCancelWorkflowExecutionAttributes
					break Process_Decision_Loop
				}
				msBuilder.AddWorkflowExecutionCanceledEvent(completedID, attributes)
				isComplete = true

			case workflow.DecisionTypeStartTimer:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeStartTimerCounter)
				attributes := d.StartTimerDecisionAttributes
				if err = validateTimerScheduleAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadStartTimerAttributes
					break Process_Decision_Loop
				}
				_, ti := msBuilder.AddTimerStartedEvent(completedID, attributes)
				if ti == nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseStartTimerDuplicateID
					break Process_Decision_Loop
				}
				tBuilder.AddUserTimer(ti, context.msBuilder)

			case workflow.DecisionTypeRequestCancelActivityTask:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelActivityCounter)
				attributes := d.RequestCancelActivityTaskDecisionAttributes
				if err = validateActivityCancelAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRequestCancelActivityAttributes
					break Process_Decision_Loop
				}
				activityID := *attributes.ActivityId
				actCancelReqEvent, ai, isRunning := msBuilder.AddActivityTaskCancelRequestedEvent(completedID, activityID,
					common.StringDefault(request.Identity))
				if !isRunning {
					msBuilder.AddRequestCancelActivityTaskFailedEvent(completedID, activityID,
						activityCancelationMsgActivityIDUnknown)
					continue Process_Decision_Loop
				}

				if ai.StartedID == common.EmptyEventID {
					// We haven't started the activity yet, we can cancel the activity right away.
					msBuilder.AddActivityTaskCanceledEvent(ai.ScheduleID, ai.StartedID, *actCancelReqEvent.EventId,
						[]byte(activityCancelationMsgActivityNotStarted), common.StringDefault(request.Identity))
				}

			case workflow.DecisionTypeCancelTimer:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelTimerCounter)
				attributes := d.CancelTimerDecisionAttributes
				if err = validateTimerCancelAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCancelTimerAttributes
					break Process_Decision_Loop
				}
				if msBuilder.AddTimerCanceledEvent(completedID, attributes, common.StringDefault(request.Identity)) == nil {
					msBuilder.AddCancelTimerFailedEvent(completedID, attributes, common.StringDefault(request.Identity))
				} else {
					// timer deletion is success. we need to rebuild the timer builder
					// since timer builder has a local cached version of timers
					tBuilder = e.getTimerBuilder(&context.workflowExecution)
					tBuilder.loadUserTimers(msBuilder)
				}

			case workflow.DecisionTypeRecordMarker:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeRecordMarkerCounter)
				attributes := d.RecordMarkerDecisionAttributes
				if err = validateRecordMarkerAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRecordMarkerAttributes
					break Process_Decision_Loop
				}
				msBuilder.AddRecordMarkerEvent(completedID, attributes)

			case workflow.DecisionTypeRequestCancelExternalWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelExternalWorkflowCounter)
				attributes := d.RequestCancelExternalWorkflowExecutionDecisionAttributes
				if err = validateCancelExternalWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRequestCancelExternalWorkflowExecutionAttributes
					break Process_Decision_Loop
				}

				foreignDomainID := ""
				if attributes.GetDomain() == "" {
					foreignDomainID = executionInfo.DomainID
				} else {
					foreignDomainEntry, err := e.shard.GetDomainCache().GetDomain(attributes.GetDomain())
					if err != nil {
						return nil, &workflow.InternalServiceError{
							Message: fmt.Sprintf("Unable to cancel workflow across domain: %v.", attributes.GetDomain())}
					}
					foreignDomainID = foreignDomainEntry.GetInfo().ID
				}

				cancelRequestID := uuid.New()
				wfCancelReqEvent, _ := msBuilder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(completedID,
					cancelRequestID, attributes)
				if wfCancelReqEvent == nil {
					return nil, &workflow.InternalServiceError{Message: "Unable to add external cancel workflow request."}
				}

				transferTasks = append(transferTasks, &persistence.CancelExecutionTask{
					TargetDomainID:          foreignDomainID,
					TargetWorkflowID:        attributes.GetWorkflowId(),
					TargetRunID:             attributes.GetRunId(),
					TargetChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
					InitiatedID:             wfCancelReqEvent.GetEventId(),
				})

			case workflow.DecisionTypeSignalExternalWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeSignalExternalWorkflowCounter)

				attributes := d.SignalExternalWorkflowExecutionDecisionAttributes
				if err = validateSignalExternalWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadSignalWorkflowExecutionAttributes
					break Process_Decision_Loop
				}
				if err = validateSignalInput(attributes.Input, e.metricsClient,
					metrics.HistoryRespondDecisionTaskCompletedScope); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadSignalInputSize
					break Process_Decision_Loop
				}

				foreignDomainID := ""
				if attributes.GetDomain() == "" {
					foreignDomainID = executionInfo.DomainID
				} else {
					foreignDomainEntry, err := e.shard.GetDomainCache().GetDomain(attributes.GetDomain())
					if err != nil {
						return nil, &workflow.InternalServiceError{
							Message: fmt.Sprintf("Unable to signal workflow across domain: %v.", attributes.GetDomain())}
					}
					foreignDomainID = foreignDomainEntry.GetInfo().ID
				}

				signalRequestID := uuid.New() // for deduplicate
				wfSignalReqEvent, _ := msBuilder.AddSignalExternalWorkflowExecutionInitiatedEvent(completedID,
					signalRequestID, attributes)
				if wfSignalReqEvent == nil {
					return nil, &workflow.InternalServiceError{Message: "Unable to add external signal workflow request."}
				}

				transferTasks = append(transferTasks, &persistence.SignalExecutionTask{
					TargetDomainID:          foreignDomainID,
					TargetWorkflowID:        attributes.Execution.GetWorkflowId(),
					TargetRunID:             attributes.Execution.GetRunId(),
					TargetChildWorkflowOnly: attributes.GetChildWorkflowOnly(),
					InitiatedID:             wfSignalReqEvent.GetEventId(),
				})

			case workflow.DecisionTypeContinueAsNewWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeContinueAsNewCounter)
				if hasUnhandledEvents {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseUnhandledDecision
					break Process_Decision_Loop
				}

				// If the decision has more than one completion event than just pick the first one
				if isComplete {
					e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
						metrics.MultipleCompletionDecisionsCounter)
					logging.LogMultipleCompletionDecisionsEvent(e.logger, *d.DecisionType)
					continue Process_Decision_Loop
				}
				attributes := d.ContinueAsNewWorkflowExecutionDecisionAttributes
				if err = validateContinueAsNewWorkflowExecutionAttributes(executionInfo, attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadContinueAsNewAttributes
					break Process_Decision_Loop
				}

				// Extract parentDomainName so it can be passed down to next run of workflow execution
				var parentDomainName string
				if msBuilder.HasParentExecution() {
					parentDomainID := executionInfo.ParentDomainID
					parentDomainEntry, err := e.shard.GetDomainCache().GetDomainByID(parentDomainID)
					if err != nil {
						return nil, err
					}
					parentDomainName = parentDomainEntry.GetInfo().Name
				}

				_, newStateBuilder, err := msBuilder.AddContinueAsNewEvent(completedID, domainEntry, parentDomainName, attributes)
				if err != nil {
					return nil, err
				}

				isComplete = true
				continueAsNewBuilder = newStateBuilder

			case workflow.DecisionTypeStartChildWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeChildWorkflowCounter)
				targetDomainID := domainID
				attributes := d.StartChildWorkflowExecutionDecisionAttributes
				if err = validateStartChildExecutionAttributes(executionInfo, attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadStartChildExecutionAttributes
					break Process_Decision_Loop
				}

				// First check if we need to use a different target domain to schedule child execution
				if attributes.Domain != nil {
					// TODO: Error handling for DecisionType_StartChildWorkflowExecution failed when domain lookup fails
					domainEntry, err := e.shard.GetDomainCache().GetDomain(*attributes.Domain)
					if err != nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to schedule child execution across domain."}
					}
					targetDomainID = domainEntry.GetInfo().ID
				}

				requestID := uuid.New()
				initiatedEvent, _ := msBuilder.AddStartChildWorkflowExecutionInitiatedEvent(completedID, requestID, attributes)
				transferTasks = append(transferTasks, &persistence.StartChildExecutionTask{
					TargetDomainID:   targetDomainID,
					TargetWorkflowID: *attributes.WorkflowId,
					InitiatedID:      *initiatedEvent.EventId,
				})

			default:
				return nil, &workflow.BadRequestError{Message: fmt.Sprintf("Unknown decision type: %v", *d.DecisionType)}
			}
		}

		if err != nil {
			return nil, err
		}

		if failDecision {
			e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope, metrics.FailedDecisionsCounter)
			logging.LogDecisionFailedEvent(e.logger, domainID, token.WorkflowID, token.RunID, failCause)
			var err1 error
			msBuilder, err1 = e.failDecision(context, scheduleID, startedID, failCause, request)
			if err1 != nil {
				return nil, err1
			}
			isComplete = false
			hasUnhandledEvents = true
			continueAsNewBuilder = nil
		}

		if tt := tBuilder.GetUserTimerTaskIfNeeded(msBuilder); tt != nil {
			timerTasks = append(timerTasks, tt)
		}
		if hasDecisionScheduleActivityTask {
			if tt := tBuilder.GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
				timerTasks = append(timerTasks, tt)
			}
		}

		// Schedule another decision task if new events came in during this decision or if request forced to
		createNewDecisionTask := hasUnhandledEvents || request.GetForceCreateNewDecisionTask()

		var newDecisionTaskScheduledID int64
		if createNewDecisionTask {
			di := msBuilder.AddDecisionTaskScheduledEvent()
			newDecisionTaskScheduledID = di.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if !request.GetReturnNewDecisionTask() {
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(&context.workflowExecution)
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						executionInfo.StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			} else {
				// start the new decision task if request asked to do so
				// TODO: replace the poll request
				msBuilder.AddDecisionTaskStartedEvent(di.ScheduleID, "request-from-RespondDecisionTaskCompleted", &workflow.PollForDecisionTaskRequest{
					TaskList: &workflow.TaskList{Name: common.StringPtr(di.TaskList)},
					Identity: request.Identity,
				})
				timeOutTask := tBuilder.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, di.DecisionTimeout)
				timerTasks = append(timerTasks, timeOutTask)
			}
		}

		if isComplete {
			tranT, timerT, err := e.getDeleteWorkflowTasks(domainID, tBuilder)
			if err != nil {
				return nil, err
			}
			transferTasks = append(transferTasks, tranT)
			timerTasks = append(timerTasks, timerT)
		}

		// Generate a transaction ID for appending events to history
		transactionID, err3 := e.shard.GetNextTransferTaskID()
		if err3 != nil {
			return nil, err3
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		var updateErr error
		if continueAsNewBuilder != nil {
			continueAsNewTimerTasks = msBuilder.GetContinueAsNew().TimerTasks
			updateErr = context.continueAsNewWorkflowExecution(request.ExecutionContext, continueAsNewBuilder,
				transferTasks, timerTasks, transactionID)
		} else {
			updateErr = context.updateWorkflowExecutionWithContext(request.ExecutionContext, transferTasks, timerTasks,
				transactionID)
		}

		if updateErr != nil {
			if updateErr == ErrConflict {
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.ConcurrencyUpdateFailureCounter)
				continue Update_History_Loop
			}

			return nil, updateErr
		}

		// add continueAsNewTimerTask
		timerTasks = append(timerTasks, continueAsNewTimerTasks...)
		// Inform timer about the new ones.
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)

		response = &h.RespondDecisionTaskCompletedResponse{}
		if request.GetReturnNewDecisionTask() && createNewDecisionTask {
			di, _ := msBuilder.GetPendingDecision(newDecisionTaskScheduledID)
			response.StartedResponse = e.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.GetIdentity())
			// sticky is always enabled when worker request for new decision task from RespondDecisionTaskCompleted
			response.StartedResponse.StickyExecutionEnabled = common.BoolPtr(true)
		}

		return response, nil
	}

	return nil, ErrMaxAttemptsExceeded
}

func (e *historyEngineImpl) RespondDecisionTaskFailed(ctx context.Context, req *h.RespondDecisionTaskFailedRequest) error {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.FailedRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return e.updateWorkflowExecution(ctx, domainID, workflowExecution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			di, isRunning := msBuilder.GetPendingDecision(scheduleID)
			if !isRunning || di.Attempt != token.ScheduleAttempt || di.StartedID == common.EmptyEventID {
				return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
			}

			msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID, request.GetCause(), request.Details,
				request.GetIdentity())

			return nil, nil
		})
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(ctx context.Context, req *h.RespondActivityTaskCompletedRequest) error {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.CompleteRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return e.updateWorkflowExecution(ctx, domainID, workflowExecution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, msBuilder)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := msBuilder.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCompletedScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleAttempt != 0 && int64(ai.Attempt) != token.ScheduleAttempt) {
				return nil, ErrActivityTaskNotFound
			}

			if msBuilder.AddActivityTaskCompletedEvent(scheduleID, ai.StartedID, request) == nil {
				// Unable to add ActivityTaskCompleted event to history
				return nil, &workflow.InternalServiceError{Message: "Unable to add ActivityTaskCompleted event to history."}
			}

			return nil, nil
		})
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(ctx context.Context, req *h.RespondActivityTaskFailedRequest) error {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.FailedRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return e.updateWorkflowExecutionWithAction(ctx, domainID, workflowExecution,
		func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, msBuilder)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := msBuilder.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskFailedScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return nil, ErrActivityTaskNotFound
			}

			postActions := &updateWorkflowAction{}
			retryTask := msBuilder.CreateRetryTimer(ai, req.FailedRequest.GetReason())
			if retryTask != nil {
				// need retry
				postActions.timerTasks = append(postActions.timerTasks, retryTask)
			} else {
				if msBuilder.AddActivityTaskFailedEvent(scheduleID, ai.StartedID, request) == nil {
					// Unable to add ActivityTaskFailed event to history
					return nil, &workflow.InternalServiceError{Message: "Unable to add ActivityTaskFailed event to history."}
				}
				postActions.createDecision = true
			}

			return postActions, nil
		})
}

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(ctx context.Context, req *h.RespondActivityTaskCanceledRequest) error {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.CancelRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	return e.updateWorkflowExecution(ctx, domainID, workflowExecution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, msBuilder)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := msBuilder.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCanceledScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return nil, ErrActivityTaskNotFound
			}

			if msBuilder.AddActivityTaskCanceledEvent(scheduleID, ai.StartedID, ai.CancelRequestID, request.Details,
				common.StringDefault(request.Identity)) == nil {
				// Unable to add ActivityTaskCanceled event to history
				return nil, &workflow.InternalServiceError{Message: "Unable to add ActivityTaskCanceled event to history."}
			}

			return nil, nil
		})

}

// RecordActivityTaskHeartbeat records an hearbeat for a task.
// This method can be used for two purposes.
// - For reporting liveness of the activity.
// - For reporting progress of the activity, this can be done even if the liveness is not configured.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(ctx context.Context,
	req *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.HeartbeatRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	var cancelRequested bool
	err = e.updateWorkflowExecution(ctx, domainID, workflowExecution, false, false,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				e.logger.Errorf("Heartbeat failed ")
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call RecordActivityHeartbeatByID, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, msBuilder)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := msBuilder.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= msBuilder.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				e.logger.Debugf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, Exist: %v", scheduleID, ai,
					isRunning)
				return nil, ErrActivityTaskNotFound
			}

			cancelRequested = ai.CancelRequested

			e.logger.Debugf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, CancelRequested: %v",
				scheduleID, ai, cancelRequested)

			// Save progress and last HB reported time.
			msBuilder.UpdateActivityProgress(ai, request)

			return nil, nil
		})

	if err != nil {
		return &workflow.RecordActivityTaskHeartbeatResponse{}, err
	}

	return &workflow.RecordActivityTaskHeartbeatResponse{CancelRequested: common.BoolPtr(cancelRequested)}, nil
}

// RequestCancelWorkflowExecution records request cancellation event for workflow execution
func (e *historyEngineImpl) RequestCancelWorkflowExecution(ctx context.Context,
	req *h.RequestCancelWorkflowExecutionRequest) error {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := req.CancelRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()
	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			executionInfo := msBuilder.GetExecutionInfo()
			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowID
				parentRunID := executionInfo.ParentRunID
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, ErrWorkflowParent
				}
			}

			isCancelRequested, cancelRequestID := msBuilder.IsCancelRequested()
			if isCancelRequested {
				cancelRequest := req.CancelRequest
				if cancelRequest.RequestId != nil {
					requestID := *cancelRequest.RequestId
					if requestID != "" && cancelRequestID == requestID {
						return nil, nil
					}
				}
				// if we consider workflow cancellation idempotent, then this error is redundant
				// this error maybe useful if this API is invoked by external, not decision from transfer queue
				return nil, ErrCancellationAlreadyRequested
			}

			if msBuilder.AddWorkflowExecutionCancelRequestedEvent("", req) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to cancel workflow execution."}
			}

			return nil, nil
		})
}

func (e *historyEngineImpl) SignalWorkflowExecution(ctx context.Context, signalRequest *h.SignalWorkflowExecutionRequest) error {

	domainEntry, err := e.getActiveDomainEntry(signalRequest.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := signalRequest.SignalRequest
	parentExecution := signalRequest.ExternalWorkflowExecution
	childWorkflowOnly := signalRequest.GetChildWorkflowOnly()
	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	if err := validateSignalInput(request.GetInput(), e.metricsClient,
		metrics.HistorySignalWorkflowExecutionScope); err != nil {
		return err
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			executionInfo := msBuilder.GetExecutionInfo()
			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowID
				parentRunID := executionInfo.ParentRunID
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, ErrWorkflowParent
				}
			}

			// deduplicate by request id for signal decision
			if requestID := request.GetRequestId(); requestID != "" {
				if msBuilder.IsSignalRequested(requestID) {
					return nil, nil
				}
				msBuilder.AddSignalRequested(requestID)
			}

			if msBuilder.AddWorkflowExecutionSignaled(request) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			return nil, nil
		})
}

func (e *historyEngineImpl) SignalWithStartWorkflowExecution(ctx context.Context, signalWithStartRequest *h.SignalWithStartWorkflowExecutionRequest) (
	retResp *workflow.StartWorkflowExecutionResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(signalWithStartRequest.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	sRequest := signalWithStartRequest.SignalWithStartRequest
	execution := workflow.WorkflowExecution{
		WorkflowId: sRequest.WorkflowId,
	}

	if err := validateSignalInput(sRequest.GetSignalInput(), e.metricsClient,
		metrics.HistorySignalWithStartWorkflowExecutionScope); err != nil {
		return nil, err
	}

	isBrandNew := false
	prevRunID := ""
	attempt := 0

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)

	if err0 == nil {
		defer func() { release(retError) }()
	Just_Signal_Loop:
		for ; attempt < conditionalRetryCount; attempt++ {
			msBuilder, err1 := context.loadWorkflowExecution()
			if err1 != nil {
				if _, ok := err1.(*workflow.EntityNotExistsError); ok {
					isBrandNew = true
					break
				}
				return nil, err1
			}
			if !msBuilder.IsWorkflowExecutionRunning() {
				prevRunID = context.workflowExecution.GetRunId()
				break
			}
			executionInfo := msBuilder.GetExecutionInfo()

			if msBuilder.AddWorkflowExecutionSignaled(getSignalRequest(sRequest)) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			var transferTasks []persistence.Task
			var timerTasks []persistence.Task
			// Create a transfer task to schedule a decision task
			if !msBuilder.HasPendingDecisionTask() {
				di := msBuilder.AddDecisionTaskScheduledEvent()
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(&context.workflowExecution)
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						executionInfo.StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			}
			// Generate a transaction ID for appending events to history
			transactionID, err2 := e.shard.GetNextTransferTaskID()
			if err2 != nil {
				return nil, err2
			}

			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
			// the history and try the operation again.
			if err := context.updateWorkflowExecution(transferTasks, timerTasks, transactionID); err != nil {
				if err == ErrConflict {
					continue Just_Signal_Loop
				}
				return nil, err
			}
			e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
			return &workflow.StartWorkflowExecutionResponse{RunId: context.workflowExecution.RunId}, nil
		} // end for Just_Signal_Loop
		if attempt == conditionalRetryCount {
			return nil, ErrMaxAttemptsExceeded
		}
	} else {
		if _, ok := err0.(*workflow.EntityNotExistsError); ok {
			isBrandNew = true
		} else {
			return nil, err0
		}
	}

	// Start workflow and signal
	startRequest := getStartRequest(domainID, sRequest)
	request := startRequest.StartRequest
	err = validateStartWorkflowExecutionRequest(request)
	if err != nil {
		return nil, err
	}

	execution = workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()

	// Generate first decision task event.
	taskList := request.TaskList.GetName()
	// TODO when the workflow is going to be replicated, use the
	var msBuilder mutableState
	if clusterMetadata.IsGlobalDomainEnabled() && domainEntry.IsGlobalDomain() {
		// all workflows within a global domain should have replication state, no matter whether it will be replicated to multiple
		// target clusters or not
		msBuilder = newMutableStateBuilderWithReplicationState(
			clusterMetadata.GetCurrentClusterName(),
			e.shard.GetConfig(),
			e.logger,
			domainEntry.GetFailoverVersion(),
		)
	} else {
		msBuilder = newMutableStateBuilder(
			clusterMetadata.GetCurrentClusterName(),
			e.shard.GetConfig(),
			e.logger,
		)
	}
	startedEvent := msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if startedEvent == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	if msBuilder.AddWorkflowExecutionSignaled(getSignalRequest(sRequest)) == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution signaled event."}
	}

	var transferTasks []persistence.Task
	di := msBuilder.AddDecisionTaskScheduledEvent()
	if di == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
	}

	transferTasks = []persistence.Task{&persistence.DecisionTask{
		DomainID: domainID, TaskList: taskList, ScheduleID: di.ScheduleID,
	}}
	decisionVersion := di.Version
	decisionScheduleID := di.ScheduleID
	decisionStartID := di.StartedID
	decisionTimeout := di.DecisionTimeout

	duration := time.Duration(*request.ExecutionStartToCloseTimeoutSeconds) * time.Second
	timerTasks := []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(duration),
	}}
	// Serialize the history
	serializedHistory, serializedError := msBuilder.GetHistoryBuilder().Serialize()
	if serializedError != nil {
		logging.LogHistorySerializationErrorEvent(e.logger, serializedError, fmt.Sprintf(
			"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v",
			execution.GetWorkflowId(), execution.GetRunId()))
		return nil, serializedError
	}

	err = e.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:  domainID,
		Execution: execution,
		// It is ok to use 0 for TransactionID because RunID is unique so there are
		// no potential duplicates to override.
		TransactionID:     0,
		FirstEventID:      startedEvent.GetEventId(),
		EventBatchVersion: startedEvent.GetVersion(),
		Events:            serializedHistory,
	})
	if err != nil {
		return nil, err
	}
	msBuilder.GetExecutionInfo().LastFirstEventID = startedEvent.GetEventId()

	var replicationState *persistence.ReplicationState
	var replicationTasks []persistence.Task
	if msBuilder.GetReplicationState() != nil {
		msBuilder.UpdateReplicationStateLastEventID(
			clusterMetadata.GetCurrentClusterName(),
			msBuilder.GetCurrentVersion(),
			msBuilder.GetNextEventID()-1,
		)
		replicationState = msBuilder.GetReplicationState()
		// this is a hack, only create replication task if have # target cluster > 1, for more see #868
		if domainEntry.CanReplicateEvent() {
			replicationTask := &persistence.HistoryReplicationTask{
				FirstEventID:        common.FirstEventID,
				NextEventID:         msBuilder.GetNextEventID(),
				Version:             msBuilder.GetCurrentVersion(),
				LastReplicationInfo: nil,
			}
			replicationTasks = append(replicationTasks, replicationTask)
		}
	}
	setTaskInfo(msBuilder.GetCurrentVersion(), time.Now(), transferTasks, timerTasks)

	createWorkflow := func(isBrandNew bool, prevRunID string) (string, error) {
		_, err = e.shard.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
			RequestID:                   common.StringDefault(request.RequestId),
			DomainID:                    domainID,
			Execution:                   execution,
			InitiatedID:                 common.EmptyEventID,
			TaskList:                    *request.TaskList.Name,
			WorkflowTypeName:            *request.WorkflowType.Name,
			WorkflowTimeout:             *request.ExecutionStartToCloseTimeoutSeconds,
			DecisionTimeoutValue:        *request.TaskStartToCloseTimeoutSeconds,
			ExecutionContext:            nil,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			ReplicationTasks:            replicationTasks,
			DecisionVersion:             decisionVersion,
			DecisionScheduleID:          decisionScheduleID,
			DecisionStartedID:           decisionStartID,
			DecisionStartToCloseTimeout: decisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               !isBrandNew,
			PreviousRunID:               prevRunID,
			ReplicationState:            replicationState,
		})

		if err != nil {
			switch t := err.(type) {
			case *persistence.WorkflowExecutionAlreadyStartedError:
				if t.StartRequestID == common.StringDefault(request.RequestId) {
					e.deleteEvents(domainID, execution)
					return t.RunID, nil
				}
			case *persistence.ShardOwnershipLostError:
				e.deleteEvents(domainID, execution)
			}
			return "", err
		}
		return execution.GetRunId(), nil
	}

	// try to create the workflow execution
	resultRunID, err := createWorkflow(isBrandNew, prevRunID) // (true, "") or (false, "prevRunID")
	if err == nil {
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)

		return &workflow.StartWorkflowExecutionResponse{
			RunId: common.StringPtr(resultRunID),
		}, nil
	} else if alreadyStartedErr, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
		return nil, &workflow.WorkflowExecutionAlreadyStartedError{
			Message:        common.StringPtr("Workflow is already running"),
			StartRequestId: common.StringPtr(alreadyStartedErr.StartRequestID),
			RunId:          common.StringPtr(alreadyStartedErr.RunID),
		}
	}
	return nil, err
}

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(ctx context.Context, request *h.RemoveSignalMutableStateRequest) error {

	domainEntry, err := e.getActiveDomainEntry(request.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, false, false,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			msBuilder.DeleteSignalRequested(request.GetRequestId())

			return nil, nil
		})
}

func (e *historyEngineImpl) TerminateWorkflowExecution(ctx context.Context, terminateRequest *h.TerminateWorkflowExecutionRequest) error {

	domainEntry, err := e.getActiveDomainEntry(terminateRequest.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	request := terminateRequest.TerminateRequest
	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, true, false,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			if msBuilder.AddWorkflowExecutionTerminatedEvent(request) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to terminate workflow execution."}
			}

			return nil, nil
		})
}

// ScheduleDecisionTask schedules a decision if no outstanding decision found
func (e *historyEngineImpl) ScheduleDecisionTask(ctx context.Context, scheduleRequest *h.ScheduleDecisionTaskRequest) error {

	domainEntry, err := e.getActiveDomainEntry(scheduleRequest.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: scheduleRequest.WorkflowExecution.WorkflowId,
		RunId:      scheduleRequest.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			// Noop

			return nil, nil
		})
}

// RecordChildExecutionCompleted records the completion of child execution into parent execution history
func (e *historyEngineImpl) RecordChildExecutionCompleted(ctx context.Context, completionRequest *h.RecordChildExecutionCompletedRequest) error {

	domainEntry, err := e.getActiveDomainEntry(completionRequest.DomainUUID)
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	execution := workflow.WorkflowExecution{
		WorkflowId: completionRequest.WorkflowExecution.WorkflowId,
		RunId:      completionRequest.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, domainID, execution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			initiatedID := *completionRequest.InitiatedId
			completedExecution := completionRequest.CompletedExecution
			completionEvent := completionRequest.CompletionEvent

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedID)
			if !isRunning || ci.StartedID == common.EmptyEventID {
				return nil, &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			switch *completionEvent.EventType {
			case workflow.EventTypeWorkflowExecutionCompleted:
				attributes := completionEvent.WorkflowExecutionCompletedEventAttributes
				msBuilder.AddChildWorkflowExecutionCompletedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionFailed:
				attributes := completionEvent.WorkflowExecutionFailedEventAttributes
				msBuilder.AddChildWorkflowExecutionFailedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionCanceled:
				attributes := completionEvent.WorkflowExecutionCanceledEventAttributes
				msBuilder.AddChildWorkflowExecutionCanceledEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTerminated:
				attributes := completionEvent.WorkflowExecutionTerminatedEventAttributes
				msBuilder.AddChildWorkflowExecutionTerminatedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTimedOut:
				attributes := completionEvent.WorkflowExecutionTimedOutEventAttributes
				msBuilder.AddChildWorkflowExecutionTimedOutEvent(initiatedID, completedExecution, attributes)
			}

			return nil, nil
		})
}

func (e *historyEngineImpl) ReplicateEvents(ctx context.Context, replicateRequest *h.ReplicateEventsRequest) error {
	return e.replicator.ApplyEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) SyncShardStatus(ctx context.Context, request *h.SyncShardStatusRequest) error {
	clusterName := request.GetSourceCluster()
	now := time.Unix(0, request.GetTimestamp())

	// here there are 3 main things
	// 1. update the view of remote cluster's shard time
	// 2. notify the timer gate in the timer queue standby processor
	// 3, notify the transfer (essentially a no op, just put it here so it looks symmetric)
	e.shard.SetCurrentTime(clusterName, now)
	e.txProcessor.NotifyNewTask(clusterName, []persistence.Task{})
	e.timerProcessor.NotifyNewTimers(clusterName, now, []persistence.Task{})
	return nil
}

type updateWorkflowAction struct {
	deleteWorkflow bool
	createDecision bool
	timerTasks     []persistence.Task
	transferTasks  []persistence.Task
}

func (e *historyEngineImpl) updateWorkflowExecutionWithAction(ctx context.Context, domainID string, execution workflow.WorkflowExecution,
	action func(builder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error)) (retError error) {
	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if err0 != nil {
		return err0
	}
	defer func() { release(retError) }()

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}
		tBuilder := e.getTimerBuilder(&context.workflowExecution)

		// conduct caller action
		postActions, err := action(msBuilder, tBuilder)
		if err != nil {
			if err == ErrStaleState {
				// Handler detected that cached workflow mutable could potentially be stale
				// Reload workflow execution history
				context.clear()
				continue Update_History_Loop
			}

			// Returned error back to the caller
			return err
		}

		transferTasks, timerTasks := postActions.transferTasks, postActions.timerTasks
		if postActions.deleteWorkflow {
			tranT, timerT, err := e.getDeleteWorkflowTasks(domainID, tBuilder)
			if err != nil {
				return err
			}
			transferTasks = append(transferTasks, tranT)
			timerTasks = append(timerTasks, timerT)
		}

		if postActions.createDecision {
			// Create a transfer task to schedule a decision task
			if !msBuilder.HasPendingDecisionTask() {
				di := msBuilder.AddDecisionTaskScheduledEvent()
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(&context.workflowExecution)
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						msBuilder.GetExecutionInfo().StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			}
		}

		// Generate a transaction ID for appending events to history
		transactionID, err2 := e.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, timerTasks, transactionID); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
			return err
		}
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
		return nil
	}
	return ErrMaxAttemptsExceeded
}

func (e *historyEngineImpl) updateWorkflowExecution(ctx context.Context, domainID string, execution workflow.WorkflowExecution,
	createDeletionTask, createDecisionTask bool,
	action func(builder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error)) error {
	return e.updateWorkflowExecutionWithAction(ctx, domainID, execution,
		func(builder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			timerTasks, err := action(builder, tBuilder)
			if err != nil {
				return nil, err
			}
			postActions := &updateWorkflowAction{
				deleteWorkflow: createDeletionTask,
				createDecision: createDecisionTask,
				timerTasks:     timerTasks,
			}
			return postActions, nil
		})
}

func (e *historyEngineImpl) getDeleteWorkflowTasks(
	domainID string,
	tBuilder *timerBuilder,
) (persistence.Task, persistence.Task, error) {
	return getDeleteWorkflowTasksFromShard(e.shard, domainID, tBuilder)
}

func getDeleteWorkflowTasksFromShard(shard ShardContext,
	domainID string,
	tBuilder *timerBuilder,
) (persistence.Task, persistence.Task, error) {
	// Create a transfer task to close workflow execution
	closeTask := &persistence.CloseExecutionTask{}

	// Generate a timer task to cleanup history events for this workflow execution
	var retentionInDays int32
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return nil, nil, err
		}
	} else {
		retentionInDays = domainEntry.GetConfig().Retention
	}
	cleanupTask := tBuilder.createDeleteHistoryEventTimerTask(time.Duration(retentionInDays) * time.Hour * 24)

	return closeTask, cleanupTask, nil
}

func (e *historyEngineImpl) createRecordDecisionTaskStartedResponse(domainID string, msBuilder mutableState,
	di *decisionInfo, identity string) *h.RecordDecisionTaskStartedResponse {
	response := &h.RecordDecisionTaskStartedResponse{}
	response.WorkflowType = msBuilder.GetWorkflowType()
	executionInfo := msBuilder.GetExecutionInfo()
	if executionInfo.LastProcessedEvent != common.EmptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(executionInfo.LastProcessedEvent)
	}

	// Starting decision could result in different scheduleID if decision was transient and new new events came in
	// before it was started.
	response.ScheduledEventId = common.Int64Ptr(di.ScheduleID)
	response.StartedEventId = common.Int64Ptr(di.StartedID)
	response.StickyExecutionEnabled = common.BoolPtr(msBuilder.IsStickyTaskListEnabled())
	response.NextEventId = common.Int64Ptr(msBuilder.GetNextEventID())
	response.Attempt = common.Int64Ptr(di.Attempt)
	response.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
		Name: &executionInfo.TaskList,
		Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
	})

	if di.Attempt > 0 {
		// This decision is retried from mutable state
		// Also return schedule and started which are not written to history yet
		scheduledEvent, startedEvent := msBuilder.CreateTransientDecisionEvents(di, identity)
		response.DecisionInfo = &workflow.TransientDecisionInfo{}
		response.DecisionInfo.ScheduledEvent = scheduledEvent
		response.DecisionInfo.StartedEvent = startedEvent
	}

	return response
}

func (e *historyEngineImpl) deleteEvents(domainID string, execution workflow.WorkflowExecution) {
	// We created the history events but failed to create workflow execution, so cleanup the history which could cause
	// us to leak history events which are never cleaned up. Cleaning up the events is absolutely safe here as they
	// are always created for a unique run_id which is not visible beyond this call yet.
	// TODO: Handle error on deletion of execution history
	e.historyMgr.DeleteWorkflowExecutionHistory(&persistence.DeleteWorkflowExecutionHistoryRequest{
		DomainID:  domainID,
		Execution: execution,
	})
}

func (e *historyEngineImpl) failDecision(context *workflowExecutionContext, scheduleID, startedID int64,
	cause workflow.DecisionTaskFailedCause, request *workflow.RespondDecisionTaskCompletedRequest) (mutableState,
	error) {
	// Clear any updates we have accumulated so far
	context.clear()

	// Reload workflow execution so we can apply the decision task failure event
	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	msBuilder.AddDecisionTaskFailedEvent(scheduleID, startedID, cause, nil, request.GetIdentity())

	// Return new builder back to the caller for further updates
	return msBuilder, nil
}

func (e *historyEngineImpl) getTimerBuilder(we *workflow.WorkflowExecution) *timerBuilder {
	lg := e.logger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: we.WorkflowId,
		logging.TagWorkflowRunID:       we.RunId,
	})
	return newTimerBuilder(e.shard.GetConfig(), lg, common.NewRealTimeSource())
}

func (s *shardContextWrapper) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
	err := s.ShardContext.UpdateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.TransferTasks)
		if len(request.ReplicationTasks) > 0 {
			s.replcatorProcessor.notifyNewTask()
		}
	}
	return err
}

func (s *shardContextWrapper) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
	resp, err := s.ShardContext.CreateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.TransferTasks)
		if len(request.ReplicationTasks) > 0 {
			s.replcatorProcessor.notifyNewTask()
		}
	}
	return resp, err
}

func (s *shardContextWrapper) NotifyNewHistoryEvent(event *historyEventNotification) error {
	s.historyEventNotifier.NotifyNewHistoryEvent(event)
	err := s.ShardContext.NotifyNewHistoryEvent(event)
	return err
}

func validateActivityScheduleAttributes(attributes *workflow.ScheduleActivityTaskDecisionAttributes, wfTimeout int32) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "ScheduleActivityTaskDecisionAttributes is not set on decision."}
	}

	if attributes.TaskList == nil || attributes.TaskList.Name == nil || *attributes.TaskList.Name == "" {
		return &workflow.BadRequestError{Message: "TaskList is not set on decision."}
	}

	if attributes.ActivityId == nil || *attributes.ActivityId == "" {
		return &workflow.BadRequestError{Message: "ActivityId is not set on decision."}
	}

	if attributes.ActivityType == nil || attributes.ActivityType.Name == nil || *attributes.ActivityType.Name == "" {
		return &workflow.BadRequestError{Message: "ActivityType is not set on decision."}
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if attributes.GetScheduleToCloseTimeoutSeconds() < 0 || attributes.GetScheduleToStartTimeoutSeconds() < 0 ||
		attributes.GetStartToCloseTimeoutSeconds() < 0 || attributes.GetHeartbeatTimeoutSeconds() < 0 {
		return &workflow.BadRequestError{Message: "A valid timeout may not be negative."}
	}

	// ensure activity's SCHEDULE_TO_START and SCHEDULE_TO_CLOSE is as long as expiration on retry policy
	p := attributes.RetryPolicy
	if p != nil {
		if attributes.GetScheduleToStartTimeoutSeconds() < p.GetExpirationIntervalInSeconds() {
			attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(p.GetExpirationIntervalInSeconds())
		}
		if attributes.GetScheduleToCloseTimeoutSeconds() < p.GetExpirationIntervalInSeconds() {
			attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(p.GetExpirationIntervalInSeconds())
		}
	}

	// ensure activity timeout never larger than workflow timeout
	if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetScheduleToStartTimeoutSeconds() > wfTimeout {
		attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetStartToCloseTimeoutSeconds() > wfTimeout {
		attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}
	if attributes.GetHeartbeatTimeoutSeconds() > wfTimeout {
		attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(wfTimeout)
	}

	validScheduleToClose := attributes.GetScheduleToCloseTimeoutSeconds() > 0
	validScheduleToStart := attributes.GetScheduleToStartTimeoutSeconds() > 0
	validStartToClose := attributes.GetStartToCloseTimeoutSeconds() > 0

	if validScheduleToClose {
		if !validScheduleToStart {
			attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToCloseTimeoutSeconds())
		}
		if !validStartToClose {
			attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToCloseTimeoutSeconds())
		}
	} else if validScheduleToStart && validStartToClose {
		attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(attributes.GetScheduleToStartTimeoutSeconds() + attributes.GetStartToCloseTimeoutSeconds())
		if attributes.GetScheduleToCloseTimeoutSeconds() > wfTimeout {
			attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(wfTimeout)
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return &workflow.BadRequestError{Message: "A valid ScheduleToCloseTimeout is not set on decision."}
	}

	return nil
}

func validateTimerScheduleAttributes(attributes *workflow.StartTimerDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartTimerDecisionAttributes is not set on decision."}
	}
	if attributes.TimerId == nil || *attributes.TimerId == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	if attributes.StartToFireTimeoutSeconds == nil || *attributes.StartToFireTimeoutSeconds <= 0 {
		return &workflow.BadRequestError{Message: "A valid StartToFireTimeoutSeconds is not set on decision."}
	}
	return nil
}

func validateActivityCancelAttributes(attributes *workflow.RequestCancelActivityTaskDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelActivityTaskDecisionAttributes is not set on decision."}
	}
	if attributes.ActivityId == nil || *attributes.ActivityId == "" {
		return &workflow.BadRequestError{Message: "ActivityId is not set on decision."}
	}
	return nil
}

func validateTimerCancelAttributes(attributes *workflow.CancelTimerDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "CancelTimerDecisionAttributes is not set on decision."}
	}
	if attributes.TimerId == nil || *attributes.TimerId == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	return nil
}

func validateRecordMarkerAttributes(attributes *workflow.RecordMarkerDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RecordMarkerDecisionAttributes is not set on decision."}
	}
	if attributes.MarkerName == nil || *attributes.MarkerName == "" {
		return &workflow.BadRequestError{Message: "MarkerName is not set on decision."}
	}
	return nil
}

func validateCompleteWorkflowExecutionAttributes(attributes *workflow.CompleteWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "CompleteWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	return nil
}

func validateFailWorkflowExecutionAttributes(attributes *workflow.FailWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "FailWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.Reason == nil {
		return &workflow.BadRequestError{Message: "Reason is not set on decision."}
	}
	return nil
}

func validateCancelWorkflowExecutionAttributes(attributes *workflow.CancelWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "CancelWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	return nil
}

func validateCancelExternalWorkflowExecutionAttributes(attributes *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	runID := attributes.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return &workflow.BadRequestError{Message: "Invalid RunId set on decision."}
	}

	return nil
}

func validateSignalExternalWorkflowExecutionAttributes(attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "SignalExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.Execution == nil {
		return &workflow.BadRequestError{Message: "Execution is nil on decision."}
	}
	if attributes.Execution.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	runID := attributes.Execution.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return &workflow.BadRequestError{Message: "Invalid RunId set on decision."}
	}
	if attributes.SignalName == nil {
		return &workflow.BadRequestError{Message: "SignalName is not set on decision."}
	}
	if attributes.Input == nil {
		return &workflow.BadRequestError{Message: "Input is not set on decision."}
	}

	return nil
}

func validateContinueAsNewWorkflowExecutionAttributes(executionInfo *persistence.WorkflowExecutionInfo,
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "ContinueAsNewWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	// Inherit workflow type from previous execution if not provided on decision
	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		attributes.WorkflowType = &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)}
	}

	// Inherit Tasklist from previous execution if not provided on decision
	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		attributes.TaskList = &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)}
	}

	// Inherit workflow timeout from previous execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from previous execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(executionInfo.DecisionTimeoutValue)
	}

	return nil
}

func validateStartChildExecutionAttributes(parentInfo *persistence.WorkflowExecutionInfo,
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartChildWorkflowExecutionDecisionAttributes is not set on decision."}
	}

	if attributes.GetWorkflowId() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowID is not set on decision."}
	}

	if attributes.WorkflowType == nil || attributes.WorkflowType.GetName() == "" {
		return &workflow.BadRequestError{Message: "Required field WorkflowType is not set on decision."}
	}

	if attributes.ChildPolicy == nil {
		return &workflow.BadRequestError{Message: "Required field ChildPolicy is not set on decision."}
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	// Inherit tasklist from parent workflow execution if not provided on decision
	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		attributes.TaskList = &workflow.TaskList{Name: common.StringPtr(parentInfo.TaskList)}
	}

	// Inherit workflow timeout from parent workflow execution if not provided on decision
	if attributes.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		attributes.ExecutionStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.WorkflowTimeout)
	}

	// Inherit decision task timeout from parent workflow execution if not provided on decision
	if attributes.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		attributes.TaskStartToCloseTimeoutSeconds = common.Int32Ptr(parentInfo.DecisionTimeoutValue)
	}

	return nil
}

func validateStartWorkflowExecutionRequest(request *workflow.StartWorkflowExecutionRequest) error {
	if request.ExecutionStartToCloseTimeoutSeconds == nil || request.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "Missing or invalid ExecutionStartToCloseTimeoutSeconds."}
	}
	if request.TaskStartToCloseTimeoutSeconds == nil || request.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "Missing or invalid TaskStartToCloseTimeoutSeconds."}
	}
	if request.TaskList == nil || request.TaskList.Name == nil || request.TaskList.GetName() == "" {
		return &workflow.BadRequestError{Message: "Missing Tasklist."}
	}
	return common.ValidateRetryPolicy(request.RetryPolicy)
}

func validateSignalInput(signalInput []byte, metricsClient metrics.Client, scope int) error {
	size := len(signalInput)
	metricsClient.RecordTimer(
		scope,
		metrics.SignalSizeTimer,
		time.Duration(size),
	)
	if size > signalInputSizeLimit {
		return ErrSignalOverSize
	}
	return nil
}

func validateDomainUUID(domainUUID *string) (string, error) {
	if domainUUID == nil {
		return "", &workflow.BadRequestError{Message: "Missing domain UUID."}
	} else if uuid.Parse(*domainUUID) == nil {
		return "", &workflow.BadRequestError{Message: "Invalid domain UUID."}
	}
	return *domainUUID, nil
}

func (e *historyEngineImpl) getActiveDomainEntry(domainUUID *string) (*cache.DomainCacheEntry, error) {
	return getActiveDomainEntryFromShard(e.shard, domainUUID)
}

func getActiveDomainEntryFromShard(shard ShardContext, domainUUID *string) (*cache.DomainCacheEntry, error) {
	domainID, err := validateDomainUUID(domainUUID)
	if err != nil {
		return nil, err
	}

	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, err
	}
	if err = domainEntry.GetDomainNotActiveErr(); err != nil {
		return nil, err
	}
	return domainEntry, nil
}

func getScheduleID(activityID string, msBuilder mutableState) (int64, error) {
	if activityID == "" {
		return 0, &workflow.BadRequestError{Message: "Neither ActivityID nor ScheduleID is provided"}
	}
	scheduleID, ok := msBuilder.GetScheduleIDByActivityID(activityID)
	if !ok {
		return 0, &workflow.BadRequestError{Message: fmt.Sprintf("No such activityID: %s\n", activityID)}
	}
	return scheduleID, nil
}

func getSignalRequest(request *workflow.SignalWithStartWorkflowExecutionRequest) *workflow.SignalWorkflowExecutionRequest {
	req := &workflow.SignalWorkflowExecutionRequest{
		Domain: request.Domain,
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: request.WorkflowId,
		},
		SignalName: request.SignalName,
		Input:      request.SignalInput,
		RequestId:  request.RequestId,
		Identity:   request.Identity,
		Control:    request.Control,
	}
	return req
}

func getStartRequest(domainID string,
	request *workflow.SignalWithStartWorkflowExecutionRequest) *h.StartWorkflowExecutionRequest {
	policy := workflow.WorkflowIdReusePolicyAllowDuplicate
	req := &workflow.StartWorkflowExecutionRequest{
		Domain:       request.Domain,
		WorkflowId:   request.WorkflowId,
		WorkflowType: request.WorkflowType,
		TaskList:     request.TaskList,
		Input:        request.Input,
		ExecutionStartToCloseTimeoutSeconds: request.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      request.TaskStartToCloseTimeoutSeconds,
		Identity:                            request.Identity,
		RequestId:                           request.RequestId,
		WorkflowIdReusePolicy:               &policy,
		RetryPolicy:                         request.RetryPolicy,
	}

	startRequest := common.CreateHistoryStartWorkflowRequest(domainID, req)
	return startRequest
}

func setTaskInfo(version int64, timestamp time.Time, transferTasks []persistence.Task, timerTasks []persistence.Task) {
	// set both the task version, as well as the timestamp on the transfer tasks
	for _, task := range transferTasks {
		task.SetVersion(version)
		task.SetVisibilityTimestamp(timestamp)
	}
	for _, task := range timerTasks {
		task.SetVersion(version)
	}
}
