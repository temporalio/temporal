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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/cron"
	"github.com/uber/cadence/common/definition"
	ce "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/archiver"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/yarpc"
)

const (
	conditionalRetryCount                     = 5
	activityCancellationMsgActivityIDUnknown  = "ACTIVITY_ID_UNKNOWN"
	activityCancellationMsgActivityNotStarted = "ACTIVITY_ID_NOT_STARTED"
	timerCancellationMsgTimerIDUnknown        = "TIMER_ID_UNKNOWN"
)

type (
	historyEngineImpl struct {
		currentClusterName   string
		shard                ShardContext
		historyMgr           persistence.HistoryManager
		historyV2Mgr         persistence.HistoryV2Manager
		executionManager     persistence.ExecutionManager
		visibilityMgr        persistence.VisibilityManager
		txProcessor          transferQueueProcessor
		timerProcessor       timerQueueProcessor
		taskAllocator        taskAllocator
		replicator           *historyReplicator
		replicatorProcessor  queueProcessor
		historyEventNotifier historyEventNotifier
		tokenSerializer      common.TaskTokenSerializer
		historyCache         *historyCache
		metricsClient        metrics.Client
		logger               log.Logger
		throttledLogger      log.Logger
		config               *Config
		archivalClient       archiver.Client
		resetor              workflowResetor
	}

	// shardContextWrapper wraps ShardContext to notify transferQueueProcessor on new tasks.
	// TODO: use to notify timerQueueProcessor as well.
	shardContextWrapper struct {
		currentClusterName string
		ShardContext
		txProcessor          transferQueueProcessor
		replicatorProcessor  queueProcessor
		historyEventNotifier historyEventNotifier
	}
)

var _ Engine = (*historyEngineImpl)(nil)

var (
	// ErrTaskDiscarded is the error indicating that the timer / transfer task is pending for too long and discarded.
	ErrTaskDiscarded = errors.New("passive task pending for too long")
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
	// ErrSignalsLimitExceeded is the error indicating limit reached for maximum number of signal events
	ErrSignalsLimitExceeded = &workflow.LimitExceededError{Message: "Exceeded workflow execution limit for signal events"}
	// ErrEventsAterWorkflowFinish is the error indicating server error trying to write events after workflow finish event
	ErrEventsAterWorkflowFinish = &shared.InternalServiceError{Message: "error validating last event being workflow finish event."}

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
func NewEngineWithShardContext(
	shard ShardContext,
	visibilityMgr persistence.VisibilityManager,
	matching matching.Client,
	historyClient hc.Client,
	publicClient workflowserviceclient.Interface,
	historyEventNotifier historyEventNotifier,
	publisher messaging.Producer,
	config *Config,
) Engine {
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
	historyV2Manager := shard.GetHistoryV2Manager()
	historyCache := newHistoryCache(shard)
	historyEngImpl := &historyEngineImpl{
		currentClusterName:   currentClusterName,
		shard:                shard,
		historyMgr:           historyManager,
		historyV2Mgr:         historyV2Manager,
		executionManager:     executionManager,
		visibilityMgr:        visibilityMgr,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		historyCache:         historyCache,
		logger:               logger.WithTags(tag.ComponentMatchingEngine),
		throttledLogger:      shard.GetThrottledLogger().WithTags(tag.ComponentMatchingEngine),
		metricsClient:        shard.GetMetricsClient(),
		historyEventNotifier: historyEventNotifier,
		config:               config,
		archivalClient:       archiver.NewClient(shard.GetMetricsClient(), shard.GetLogger(), publicClient, shard.GetConfig().NumArchiveSystemWorkflows),
	}

	txProcessor := newTransferQueueProcessor(shard, historyEngImpl, visibilityMgr, matching, historyClient, logger)
	historyEngImpl.timerProcessor = newTimerQueueProcessor(shard, historyEngImpl, matching, logger)
	historyEngImpl.txProcessor = txProcessor
	shardWrapper.txProcessor = txProcessor

	// Only start the replicator processor if valid publisher is passed in
	if publisher != nil {
		replicatorProcessor := newReplicatorQueueProcessor(shard, historyEngImpl.historyCache, publisher, executionManager, historyManager, historyV2Manager, logger)
		historyEngImpl.replicatorProcessor = replicatorProcessor
		shardWrapper.replicatorProcessor = replicatorProcessor
		historyEngImpl.replicator = newHistoryReplicator(shard, historyEngImpl, historyCache, shard.GetDomainCache(), historyManager, historyV2Manager,
			logger)
	}
	historyEngImpl.resetor = newWorkflowResetor(historyEngImpl, historyEngImpl.replicator)

	return historyEngImpl
}

// Start will spin up all the components needed to start serving this shard.
// Make sure all the components are loaded lazily so start can return immediately.  This is important because
// ShardController calls start sequentially for all the shards for a given host during startup.
func (e *historyEngineImpl) Start() {
	e.logger.Info("", tag.LifeCycleStarting)
	defer e.logger.Info("", tag.LifeCycleStarted)

	e.registerDomainFailoverCallback()

	e.txProcessor.Start()
	e.timerProcessor.Start()
	if e.replicatorProcessor != nil {
		e.replicatorProcessor.Start()
	}
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	e.logger.Info("", tag.LifeCycleStopping)
	defer e.logger.Info("", tag.LifeCycleStopped)

	e.txProcessor.Stop()
	e.timerProcessor.Stop()
	if e.replicatorProcessor != nil {
		e.replicatorProcessor.Stop()
	}

	// unset the failover callback
	e.shard.GetDomainCache().UnregisterDomainChangeCallback(e.shard.GetShardID())
}

func (e *historyEngineImpl) registerDomainFailoverCallback() {

	// NOTE: READ BEFORE MODIFICATION
	//
	// Tasks, e.g. transfer tasks and timer tasks, are created when holding the shard lock
	// meaning tasks -> release of shard lock
	//
	// Domain change notification follows the following steps, order matters
	// 1. lock all task processing.
	// 2. domain changes visible to everyone (Note: lock of task processing prevents task processing logic seeing the domain changes).
	// 3. failover min and max task levels are calaulated, then update to shard.
	// 4. failover start & task processing unlock & shard domain version notification update. (order does not matter for this discussion)
	//
	// The above guarantees that task created during the failover will be processed.
	// If the task is created after domain change:
	// 		then active processor will handle it. (simple case)
	// If the task is created before domain change:
	//		task -> release of shard lock
	//		failover min / max task levels calculated & updated to shard (using shard lock) -> failover start
	// above 2 guarantees that failover start is after persistence of the task.

	failoverPredicate := func(shardNotificationVersion int64, nextDomain *cache.DomainCacheEntry, action func()) {
		domainFailoverNotificationVersion := nextDomain.GetFailoverNotificationVersion()
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
		e.shard.GetDomainNotificationVersion(),
		func() {
			e.txProcessor.LockTaskPrrocessing()
			e.timerProcessor.LockTaskPrrocessing()
		},
		func(prevDomains []*cache.DomainCacheEntry, nextDomains []*cache.DomainCacheEntry) {
			defer func() {
				e.txProcessor.UnlockTaskPrrocessing()
				e.timerProcessor.UnlockTaskPrrocessing()
			}()

			if len(nextDomains) == 0 {
				return
			}

			shardNotificationVersion := e.shard.GetDomainNotificationVersion()
			failoverDomainIDs := map[string]struct{}{}

			for _, nextDomain := range nextDomains {
				failoverPredicate(shardNotificationVersion, nextDomain, func() {
					failoverDomainIDs[nextDomain.GetInfo().ID] = struct{}{}
				})
			}

			if len(failoverDomainIDs) > 0 {
				e.logger.Info("Domain Failover Start.", tag.WorkflowDomainIDs(failoverDomainIDs))

				e.txProcessor.FailoverDomain(failoverDomainIDs)
				e.timerProcessor.FailoverDomain(failoverDomainIDs)

				now := e.shard.GetTimeSource().Now()
				// the fake tasks will not be actually used, we just need to make sure
				// its length > 0 and has correct timestamp, to trigger a db scan
				fakeDecisionTask := []persistence.Task{&persistence.DecisionTask{}}
				fakeDecisionTimeoutTask := []persistence.Task{&persistence.DecisionTimeoutTask{VisibilityTimestamp: now}}
				e.txProcessor.NotifyNewTask(e.currentClusterName, fakeDecisionTask)
				e.timerProcessor.NotifyNewTimers(e.currentClusterName, now, fakeDecisionTimeoutTask)
			}

			e.shard.UpdateDomainNotificationVersion(nextDomains[len(nextDomains)-1].GetNotificationVersion() + 1)
		},
	)
}

func (e *historyEngineImpl) createMutableState(clusterMetadata cluster.Metadata, domainEntry *cache.DomainCacheEntry) mutableState {
	var msBuilder mutableState
	if clusterMetadata.IsGlobalDomainEnabled() && domainEntry.IsGlobalDomain() {
		// all workflows within a global domain should have replication state, no matter whether it will be replicated to multiple
		// target clusters or not
		msBuilder = newMutableStateBuilderWithReplicationState(
			clusterMetadata.GetCurrentClusterName(),
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
			domainEntry.GetFailoverVersion(),
		)
	} else {
		msBuilder = newMutableStateBuilder(
			clusterMetadata.GetCurrentClusterName(),
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
		)
	}

	return msBuilder
}

func (e *historyEngineImpl) generateFirstDecisionTask(domainID string, msBuilder mutableState, parentInfo *h.ParentExecutionInfo,
	taskListName string, cronBackoffSeconds int32) ([]persistence.Task, *decisionInfo, error) {
	di := &decisionInfo{
		TaskList:        taskListName,
		Version:         common.EmptyVersion,
		ScheduleID:      common.EmptyEventID,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: int32(0),
	}
	var transferTasks []persistence.Task
	if parentInfo == nil {
		// RecordWorkflowStartedTask is only created when it is not a Child Workflow
		transferTasks = append(transferTasks, &persistence.RecordWorkflowStartedTask{})
		if cronBackoffSeconds == 0 {
			// DecisionTask is only created when it is not a Child Workflow and no backoff is needed
			di = msBuilder.AddDecisionTaskScheduledEvent()
			if di == nil {
				return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}

			transferTasks = append(transferTasks, &persistence.DecisionTask{
				DomainID: domainID, TaskList: taskListName, ScheduleID: di.ScheduleID,
			})
		}
	}
	return transferTasks, di, nil
}

func (e *historyEngineImpl) appendFirstBatchHistoryEvents(msBuilder mutableState, domainID string, execution workflow.WorkflowExecution) (historySize int, err error) {
	// call FlushBufferedEvents to assign task id to event
	// as well as update last event task id in new state builder
	err = msBuilder.FlushBufferedEvents()
	if err != nil {
		return 0, err
	}
	events := msBuilder.GetHistoryBuilder().GetHistory().Events
	startedEvent := events[0]
	if msBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
		branchToken := msBuilder.GetCurrentBranch()
		historySize, err = e.shard.AppendHistoryV2Events(&persistence.AppendHistoryNodesRequest{
			IsNewBranch: true,
			Info:        historyGarbageCleanupInfo(domainID, execution.GetWorkflowId(), execution.GetRunId()),
			BranchToken: branchToken,
			Events:      events,
			// It is ok to use 0 for TransactionID because RunID is unique so there are
			// no potential duplicates to override.
			TransactionID: 0,
		}, domainID, execution)
	} else {
		historySize, err = e.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:  domainID,
			Execution: execution,
			// It is ok to use 0 for TransactionID because RunID is unique so there are
			// no potential duplicates to override.
			TransactionID:     0,
			FirstEventID:      startedEvent.GetEventId(),
			EventBatchVersion: startedEvent.GetVersion(),
			Events:            events,
		})
	}
	return
}

func fulfillExecutionInfo(msBuilder mutableState, domainID, taskList string, execution workflow.WorkflowExecution, lastFirstEventID int64) {
	info := msBuilder.GetExecutionInfo()
	info.DomainID = domainID
	info.WorkflowID = *execution.WorkflowId
	info.RunID = *execution.RunId
	info.SetLastFirstEventID(lastFirstEventID)
	info.TaskList = taskList
}

func generateFirstReplicationTask(msBuilder mutableState, clusterMetadata cluster.Metadata, domainEntry *cache.DomainCacheEntry) []persistence.Task {
	var replicationTasks []persistence.Task
	if msBuilder.GetReplicationState() != nil {
		msBuilder.UpdateReplicationStateLastEventID(
			clusterMetadata.GetCurrentClusterName(),
			msBuilder.GetCurrentVersion(),
			msBuilder.GetNextEventID()-1,
		)
		// this is a hack, only create replication task if have # target cluster > 1, for more see #868
		if domainEntry.CanReplicateEvent() {
			replicationTask := &persistence.HistoryReplicationTask{
				FirstEventID:        common.FirstEventID,
				NextEventID:         msBuilder.GetNextEventID(),
				Version:             msBuilder.GetCurrentVersion(),
				LastReplicationInfo: nil,
				EventStoreVersion:   msBuilder.GetEventStoreVersion(),
				BranchToken:         msBuilder.GetCurrentBranch(),
			}

			replicationTasks = append(replicationTasks, replicationTask)
		}
	}
	return replicationTasks
}

func (e *historyEngineImpl) createWorkflow(startRequest *h.StartWorkflowExecutionRequest, msBuilder mutableState, createMode int, prevRunID string, prevLastWriteVersion int64,
	firstDecisionTask *decisionInfo, transferTasks, timerTasks, replicationTasks []persistence.Task, clusterMetadata cluster.Metadata) (err error) {

	request := startRequest.StartRequest
	currExeInfo := msBuilder.GetExecutionInfo()
	execution := workflow.WorkflowExecution{
		WorkflowId: &currExeInfo.WorkflowID,
		RunId:      &currExeInfo.RunID,
	}

	var parentExecution *workflow.WorkflowExecution
	initiatedID := common.EmptyEventID
	parentDomainID := ""
	parentInfo := startRequest.ParentExecutionInfo
	if startRequest.ParentExecutionInfo != nil {
		parentDomainID = *parentInfo.DomainUUID
		parentExecution = parentInfo.Execution
		initiatedID = *parentInfo.InitiatedId
	}

	createRequest := &persistence.CreateWorkflowExecutionRequest{
		RequestID:                   common.StringDefault(request.RequestId),
		DomainID:                    currExeInfo.DomainID,
		Execution:                   execution,
		ParentDomainID:              parentDomainID,
		ParentExecution:             parentExecution,
		InitiatedID:                 initiatedID,
		TaskList:                    *request.TaskList.Name,
		WorkflowTypeName:            *request.WorkflowType.Name,
		WorkflowTimeout:             *request.ExecutionStartToCloseTimeoutSeconds,
		DecisionTimeoutValue:        *request.TaskStartToCloseTimeoutSeconds,
		ExecutionContext:            nil,
		LastEventTaskID:             currExeInfo.LastEventTaskID,
		NextEventID:                 msBuilder.GetNextEventID(),
		LastProcessedEvent:          common.EmptyEventID,
		HistorySize:                 int64(msBuilder.GetHistorySize()),
		TransferTasks:               transferTasks,
		ReplicationTasks:            replicationTasks,
		DecisionVersion:             firstDecisionTask.Version,
		DecisionScheduleID:          firstDecisionTask.ScheduleID,
		DecisionStartedID:           firstDecisionTask.StartedID,
		DecisionStartToCloseTimeout: firstDecisionTask.DecisionTimeout,
		TimerTasks:                  timerTasks,
		PreviousRunID:               prevRunID,
		PreviousLastWriteVersion:    prevLastWriteVersion,
		ReplicationState:            msBuilder.GetReplicationState(),
		HasRetryPolicy:              request.RetryPolicy != nil,
		EventStoreVersion:           msBuilder.GetEventStoreVersion(),
		BranchToken:                 msBuilder.GetCurrentBranch(),
		CreateWorkflowMode:          createMode,
		CronSchedule:                request.GetCronSchedule(),
	}

	if createRequest.HasRetryPolicy {
		createRequest.InitialInterval = request.RetryPolicy.GetInitialIntervalInSeconds()
		createRequest.BackoffCoefficient = request.RetryPolicy.GetBackoffCoefficient()
		createRequest.MaximumInterval = request.RetryPolicy.GetMaximumIntervalInSeconds()

		if startRequest.ExpirationTimestamp != nil && *startRequest.ExpirationTimestamp > 0 {
			expireTimeNano := *startRequest.ExpirationTimestamp
			createRequest.ExpirationTime = time.Unix(0, expireTimeNano)
		}
		createRequest.MaximumAttempts = request.RetryPolicy.GetMaximumAttempts()
		createRequest.NonRetriableErrors = request.RetryPolicy.NonRetriableErrorReasons
		createRequest.ExpirationSeconds = request.RetryPolicy.GetExpirationIntervalInSeconds()
	}

	_, err = e.shard.CreateWorkflowExecution(createRequest)
	return err
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(ctx context.Context, startRequest *h.StartWorkflowExecutionRequest) (
	resp *workflow.StartWorkflowExecutionResponse, retError error) {
	domainEntry, retError := e.getActiveDomainEntry(startRequest.DomainUUID)
	if retError != nil {
		return
	}
	domainID := domainEntry.GetInfo().ID

	request := startRequest.StartRequest
	retError = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if retError != nil {
		return
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()
	msBuilder := e.createMutableState(clusterMetadata, domainEntry)
	var eventStoreVersion int32
	if e.config.EnableEventsV2(request.GetDomain()) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		// NOTE: except for fork(reset), we use runID as treeID for simplicity
		if retError = msBuilder.SetHistoryTree(*execution.RunId); retError != nil {
			return
		}
	}

	startedEvent := msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if startedEvent == nil {
		retError = &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
		return
	}

	taskList := request.TaskList.GetName()
	cronBackoffSeconds := startRequest.GetFirstDecisionTaskBackoffSeconds()
	// Generate first decision task event if not child WF and no first decision task backoff
	transferTasks, firstDecisionTask, retError := e.generateFirstDecisionTask(domainID, msBuilder, startRequest.ParentExecutionInfo, taskList, cronBackoffSeconds)
	if retError != nil {
		return
	}

	// Generate first timer task : WF timeout task
	cronBackoffDuration := time.Duration(cronBackoffSeconds) * time.Second
	timeoutDuration := time.Duration(*request.ExecutionStartToCloseTimeoutSeconds)*time.Second + cronBackoffDuration
	timerTasks := []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(timeoutDuration),
	}}

	// Only schedule the backoff timer task if not child WF and there's first decision task backoff
	if cronBackoffSeconds != 0 && startRequest.ParentExecutionInfo == nil {
		timerTasks = append(timerTasks, &persistence.WorkflowBackoffTimerTask{
			VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(cronBackoffDuration),
			TimeoutType:         persistence.WorkflowBackoffTimeoutTypeCron,
		})
	}

	// generate first replication task
	replicationTasks := generateFirstReplicationTask(msBuilder, clusterMetadata, domainEntry)
	// set versions and timestamp for timer and transfer tasks
	setTaskInfo(msBuilder.GetCurrentVersion(), time.Now(), transferTasks, timerTasks)

	historySize, retError := e.appendFirstBatchHistoryEvents(msBuilder, domainID, execution)
	if retError != nil {
		return
	}
	// delete history if createWorkflow failed, otherwise history will leak
	shouldDeleteHistory := true
	defer func() {
		if shouldDeleteHistory {
			e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
		}
	}()

	// prepare for execution persistence operation
	msBuilder.IncrementHistorySize(historySize)
	fulfillExecutionInfo(msBuilder, domainID, taskList, execution, startedEvent.GetEventId())

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	retError = e.createWorkflow(startRequest, msBuilder, createMode, prevRunID, prevLastWriteVersion, firstDecisionTask, transferTasks, timerTasks, replicationTasks, clusterMetadata)
	if retError != nil {
		t, ok := retError.(*persistence.WorkflowExecutionAlreadyStartedError)
		if ok {
			if t.StartRequestID == *request.RequestId {
				return &workflow.StartWorkflowExecutionResponse{
					RunId: common.StringPtr(t.RunID),
				}, nil
				// delete history is expected here because duplicate start request will create history with different rid
			}

			if msBuilder.GetCurrentVersion() < t.LastWriteVersion {
				retError = ce.NewDomainNotActiveError(
					*request.Domain,
					clusterMetadata.GetCurrentClusterName(),
					clusterMetadata.ClusterNameForFailoverVersion(t.LastWriteVersion),
				)
				return
			}

			// create as ID reuse
			createMode = persistence.CreateWorkflowModeWorkflowIDReuse
			prevRunID = t.RunID
			prevLastWriteVersion = t.LastWriteVersion
			retError = e.applyWorkflowIDReusePolicyHelper(t.StartRequestID, prevRunID, t.State, t.CloseStatus, domainID, execution, startRequest.StartRequest.GetWorkflowIdReusePolicy())
			if retError != nil {
				return
			}
			retError = e.createWorkflow(startRequest, msBuilder, createMode, prevRunID, prevLastWriteVersion, firstDecisionTask, transferTasks, timerTasks, replicationTasks, clusterMetadata)
		}
	}

	if retError == nil {
		shouldDeleteHistory = false
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
		return &workflow.StartWorkflowExecutionResponse{
			RunId: execution.RunId,
		}, nil
	}
	return
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
		subscriberID, channel, err := e.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowId(), execution.GetRunId()))
		if err != nil {
			return nil, err
		}
		defer e.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID)

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
				response.PreviousStartedEventId = common.Int64Ptr(event.previousStartedEventID)
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
	execution.RunId = context.getExecution().RunId
	retResp = &h.GetMutableStateResponse{
		Execution:                            &execution,
		WorkflowType:                         &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
		LastFirstEventId:                     common.Int64Ptr(msBuilder.GetLastFirstEventID()),
		NextEventId:                          common.Int64Ptr(msBuilder.GetNextEventID()),
		PreviousStartedEventId:               common.Int64Ptr(msBuilder.GetPreviousStartedEventID()),
		TaskList:                             &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
		StickyTaskList:                       &workflow.TaskList{Name: common.StringPtr(executionInfo.StickyTaskList)},
		ClientLibraryVersion:                 common.StringPtr(executionInfo.ClientLibraryVersion),
		ClientFeatureVersion:                 common.StringPtr(executionInfo.ClientFeatureVersion),
		ClientImpl:                           common.StringPtr(executionInfo.ClientImpl),
		IsWorkflowRunning:                    common.BoolPtr(msBuilder.IsWorkflowExecutionRunning()),
		StickyTaskListScheduleToStartTimeout: common.Int32Ptr(executionInfo.StickyScheduleToStartTimeout),
		EventStoreVersion:                    common.Int32Ptr(msBuilder.GetEventStoreVersion()),
		BranchToken:                          msBuilder.GetCurrentBranch(),
	}

	replicationState := msBuilder.GetReplicationState()
	if replicationState != nil {
		retResp.ReplicationInfo = map[string]*workflow.ReplicationInfo{}
		for k, v := range replicationState.LastReplicationInfo {
			retResp.ReplicationInfo[k] = &workflow.ReplicationInfo{
				Version:     common.Int64Ptr(v.Version),
				LastEventId: common.Int64Ptr(v.LastEventID),
			}
		}
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

	if cacheHit && cacheCtx.(*workflowExecutionContextImpl).msBuilder != nil {
		msb := cacheCtx.(*workflowExecutionContextImpl).msBuilder
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
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionInfo.WorkflowTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(executionInfo.DecisionTimeoutValue),
			ChildPolicy:                         common.ChildPolicyPtr(workflow.ChildPolicyTerminate),
		},
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(executionInfo.WorkflowID),
				RunId:      common.StringPtr(executionInfo.RunID),
			},
			Type:          &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
			StartTime:     common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
			HistoryLength: common.Int64Ptr(msBuilder.GetNextEventID() - common.FirstEventID),
		},
	}
	if executionInfo.ParentRunID != "" {
		result.WorkflowExecutionInfo.ParentExecution = &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(executionInfo.ParentRunID),
		}
		result.WorkflowExecutionInfo.ParentDomainId = common.StringPtr(executionInfo.ParentDomainID)
	}
	if executionInfo.State == persistence.WorkflowStateCompleted {
		// for closed workflow
		closeStatus := getWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		result.WorkflowExecutionInfo.CloseStatus = &closeStatus
		completionEvent, ok := msBuilder.GetCompletionEvent()
		if !ok {
			return nil, &workflow.InternalServiceError{Message: "Unable to get workflow completion event."}
		}
		result.WorkflowExecutionInfo.CloseTime = common.Int64Ptr(completionEvent.GetTimestamp())
	}

	if len(msBuilder.GetPendingActivityInfos()) > 0 {
		for _, ai := range msBuilder.GetPendingActivityInfos() {
			p := &workflow.PendingActivityInfo{
				ActivityID: common.StringPtr(ai.ActivityID),
			}
			state := workflow.PendingActivityStateScheduled
			if ai.CancelRequested {
				state = workflow.PendingActivityStateCancelRequested
			} else if ai.StartedID != common.EmptyEventID {
				state = workflow.PendingActivityStateStarted
			}
			p.State = &state
			lastHeartbeatUnixNano := ai.LastHeartBeatUpdatedTime.UnixNano()
			if lastHeartbeatUnixNano > 0 {
				p.LastHeartbeatTimestamp = common.Int64Ptr(lastHeartbeatUnixNano)
				p.HeartbeatDetails = ai.Details
			}
			// TODO: move to mutable state instead of loading it from event
			scheduledEvent, ok := msBuilder.GetActivityScheduledEvent(ai.ScheduleID)
			if !ok {
				return nil, &workflow.InternalServiceError{Message: "Unable to get activity schedule event."}
			}
			p.ActivityType = scheduledEvent.ActivityTaskScheduledEventAttributes.ActivityType
			if state == workflow.PendingActivityStateScheduled {
				p.ScheduledTimestamp = common.Int64Ptr(ai.ScheduledTime.UnixNano())
			} else {
				p.LastStartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
			}
			if ai.HasRetryPolicy {
				p.Attempt = common.Int32Ptr(ai.Attempt)
				p.ExpirationTimestamp = common.Int64Ptr(ai.ExpirationTime.UnixNano())
				if ai.MaximumAttempts != 0 {
					p.MaximumAttempts = common.Int32Ptr(ai.MaximumAttempts)
				}
			}
			result.PendingActivities = append(result.PendingActivities, p)
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
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}

		tBuilder := e.getTimerBuilder(context.getExecution())

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
		if !isRunning {
			// Looks like DecisionTask already completed as a result of another call.
			// It is OK to drop the task at this point.
			context.getLogger().Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeDecisionTask))

			return nil, &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		if di.StartedID != common.EmptyEventID {
			// If decision is started as part of the current request scope then return a positive response
			if di.RequestID == requestID {
				return e.createRecordDecisionTaskStartedResponse(domainID, msBuilder, di, request.PollRequest.GetIdentity()), nil
			}

			// Looks like DecisionTask already started as a result of another call.
			// It is OK to drop the task at this point.
			context.getLogger().Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TaskListTypeDecision))
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

	domainInfo := domainEntry.GetInfo()

	domainID := domainInfo.ID
	domainName := domainInfo.Name

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
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
				return nil, ErrActivityTaskNotFound
			}

			scheduledEvent, ok := msBuilder.GetActivityScheduledEvent(scheduleID)
			if !ok {
				return nil, &workflow.InternalServiceError{Message: "Unable to get activity schedule event."}
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
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
				return nil, &h.EventAlreadyStartedError{Message: "Activity task already started."}
			}

			msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, requestID, request.PollRequest.GetIdentity())

			response.StartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
			response.Attempt = common.Int64Ptr(int64(ai.Attempt))
			response.HeartbeatDetails = ai.Details

			response.WorkflowType = msBuilder.GetWorkflowType()
			response.WorkflowDomain = common.StringPtr(domainName)

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

type decisionBlobSizeChecker struct {
	sizeLimitWarn  int
	sizeLimitError int
	domainID       string
	workflowID     string
	runID          string
	metricsClient  metrics.Client
	logger         log.Logger
	msBuilder      mutableState
	completedID    int64
}

func (c *decisionBlobSizeChecker) failWorkflowIfBlobSizeExceedsLimit(blob []byte, message string) (bool, error) {
	err := common.CheckEventBlobSizeLimit(
		len(blob),
		c.sizeLimitWarn,
		c.sizeLimitError,
		c.domainID,
		c.workflowID,
		c.runID,
		c.metricsClient.Scope(metrics.HistoryRespondDecisionTaskCompletedScope),
		c.logger,
	)
	if err == nil {
		return false, nil
	}

	err = failInFlightDecisionToClearBufferedEvents(c.msBuilder)
	if err != nil {
		return false, err
	}

	attributes := &workflow.FailWorkflowExecutionDecisionAttributes{
		Reason:  common.StringPtr(common.FailureReasonDecisionBlobSizeExceedsLimit),
		Details: []byte(message),
	}

	if evt := c.msBuilder.AddFailWorkflowEvent(c.completedID, attributes); evt == nil {
		return false, &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
	}

	return true, nil
}

// Before closing workflow, if there is a in-flight decision, fail the decision first. So that we don't close the workflow with buffered events
func failInFlightDecisionToClearBufferedEvents(msBuilder mutableState) error {
	if msBuilder.HasInFlightDecisionTask() {
		di, _ := msBuilder.GetInFlightDecisionTask()

		event := msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID, workflow.DecisionTaskFailedCauseResetWorkflow, nil,
			identityHistoryService, decisionFailureForBuffered, "", "", 0)
		if event == nil {
			return &workflow.InternalServiceError{Message: "Failed to add decision failed event."}
		}

		return msBuilder.FlushBufferedEvents()
	}
	return nil
}

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	req *h.RespondDecisionTaskCompletedRequest,
) (response *h.RespondDecisionTaskCompletedResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(req.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	var eventStoreVersion int32
	if e.config.EnableEventsV2(domainEntry.GetInfo().Name) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
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

	sizeLimitError := e.config.BlobSizeLimitError(domainEntry.GetInfo().Name)
	sizeLimitWarn := e.config.BlobSizeLimitWarn(domainEntry.GetInfo().Name)
	maxIDLengthLimit := e.config.MaxIDLengthLimit()

	sizeChecker := &decisionBlobSizeChecker{
		sizeLimitWarn:  sizeLimitWarn,
		sizeLimitError: sizeLimitError,
		domainID:       domainID,
		workflowID:     token.WorkflowID,
		runID:          token.RunID,
		metricsClient:  e.metricsClient,
		logger:         e.throttledLogger,
	}

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}
		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}

		executionInfo := msBuilder.GetExecutionInfo()
		tBuilder := e.getTimerBuilder(context.getExecution())

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
		var failMessage string
		var err error
		completedID := *completedEvent.EventId
		hasUnhandledEvents := msBuilder.HasBufferedEvents()
		isComplete := false
		activityNotStartedCancelled := false
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

		sizeChecker.completedID = completedID
		sizeChecker.msBuilder = msBuilder

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

				if err = validateActivityScheduleAttributes(attributes, executionInfo.WorkflowTimeout, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadScheduleActivityAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}

				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "ScheduleActivityTaskDecisionAttributes.Input exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
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
					e.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
					continue Process_Decision_Loop
				}
				attributes := d.CompleteWorkflowExecutionDecisionAttributes
				if err = validateCompleteWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCompleteWorkflowExecutionAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Result, "CompleteWorkflowExecutionDecisionAttributes.Result exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
					break Process_Decision_Loop
				}

				// check if this is a cron workflow
				cronBackoff := msBuilder.GetCronBackoffDuration()
				if cronBackoff == cron.NoBackoff {
					// not cron, so complete this workflow execution
					if e := msBuilder.AddCompletedWorkflowEvent(completedID, attributes); e == nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to add complete workflow event."}
					}
				} else {
					// this is a cron workflow
					startEvent, err := getWorkflowStartedEvent(e.historyMgr, e.historyV2Mgr, msBuilder.GetEventStoreVersion(), msBuilder.GetCurrentBranch(), e.logger, domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId(), common.IntPtr(e.shard.GetShardID()))
					if err != nil {
						return nil, err
					}

					startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
					continueAsNewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
						WorkflowType:                        startAttributes.WorkflowType,
						TaskList:                            startAttributes.TaskList,
						RetryPolicy:                         startAttributes.RetryPolicy,
						Input:                               startAttributes.Input,
						ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
						TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
						BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(cronBackoff.Seconds())),
						Initiator:                           workflow.ContinueAsNewInitiatorCronSchedule.Ptr(),
						LastCompletionResult:                attributes.Result,
						CronSchedule:                        common.StringPtr(msBuilder.GetExecutionInfo().CronSchedule),
					}

					if _, continueAsNewBuilder, err = msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry,
						startAttributes.GetParentWorkflowDomain(), continueAsNewAttributes, eventStoreVersion); err != nil {
						return nil, err
					}
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
					e.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
					continue Process_Decision_Loop
				}

				failedAttributes := d.FailWorkflowExecutionDecisionAttributes
				if err = validateFailWorkflowExecutionAttributes(failedAttributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadFailWorkflowExecutionAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}

				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(failedAttributes.Details, "FailWorkflowExecutionDecisionAttributes.Details exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
					break Process_Decision_Loop
				}

				backoffInterval := msBuilder.GetRetryBackoffDuration(failedAttributes.GetReason())
				continueAsNewInitiator := workflow.ContinueAsNewInitiatorRetryPolicy
				if backoffInterval == common.NoRetryBackoff {
					backoffInterval = msBuilder.GetCronBackoffDuration()
					continueAsNewInitiator = workflow.ContinueAsNewInitiatorCronSchedule
				}

				if backoffInterval == cron.NoBackoff {
					// no retry or cron
					if evt := msBuilder.AddFailWorkflowEvent(completedID, failedAttributes); evt == nil {
						return nil, &workflow.InternalServiceError{Message: "Unable to add fail workflow event."}
					}
				} else {
					// retry or cron with backoff
					startEvent, err := getWorkflowStartedEvent(e.historyMgr, e.historyV2Mgr, msBuilder.GetEventStoreVersion(), msBuilder.GetCurrentBranch(), e.logger, domainID, workflowExecution.GetWorkflowId(), workflowExecution.GetRunId(), common.IntPtr(e.shard.GetShardID()))
					if err != nil {
						return nil, err
					}

					startAttributes := startEvent.WorkflowExecutionStartedEventAttributes
					continueAsNewAttributes := &workflow.ContinueAsNewWorkflowExecutionDecisionAttributes{
						WorkflowType:                        startAttributes.WorkflowType,
						TaskList:                            startAttributes.TaskList,
						RetryPolicy:                         startAttributes.RetryPolicy,
						Input:                               startAttributes.Input,
						ExecutionStartToCloseTimeoutSeconds: startAttributes.ExecutionStartToCloseTimeoutSeconds,
						TaskStartToCloseTimeoutSeconds:      startAttributes.TaskStartToCloseTimeoutSeconds,
						BackoffStartIntervalInSeconds:       common.Int32Ptr(int32(backoffInterval.Seconds())),
						Initiator:                           continueAsNewInitiator.Ptr(),
						FailureReason:                       failedAttributes.Reason,
						FailureDetails:                      failedAttributes.Details,
						LastCompletionResult:                startAttributes.LastCompletionResult,
						CronSchedule:                        common.StringPtr(msBuilder.GetExecutionInfo().CronSchedule),
					}

					if _, continueAsNewBuilder, err = msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry,
						startAttributes.GetParentWorkflowDomain(), continueAsNewAttributes, eventStoreVersion); err != nil {
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
					e.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
					continue Process_Decision_Loop
				}
				attributes := d.CancelWorkflowExecutionDecisionAttributes
				if err = validateCancelWorkflowExecutionAttributes(attributes); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCancelWorkflowExecutionAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				msBuilder.AddWorkflowExecutionCanceledEvent(completedID, attributes)
				isComplete = true

			case workflow.DecisionTypeStartTimer:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeStartTimerCounter)
				attributes := d.StartTimerDecisionAttributes
				if err = validateTimerScheduleAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadStartTimerAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				_, ti := msBuilder.AddTimerStartedEvent(completedID, attributes)
				if ti == nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseStartTimerDuplicateID
					break Process_Decision_Loop
				}
				tBuilder.AddUserTimer(ti, msBuilder)

			case workflow.DecisionTypeRequestCancelActivityTask:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelActivityCounter)
				attributes := d.RequestCancelActivityTaskDecisionAttributes
				if err = validateActivityCancelAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRequestCancelActivityAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				activityID := *attributes.ActivityId
				actCancelReqEvent, ai, isRunning := msBuilder.AddActivityTaskCancelRequestedEvent(completedID, activityID,
					common.StringDefault(request.Identity))
				if !isRunning {
					msBuilder.AddRequestCancelActivityTaskFailedEvent(completedID, activityID,
						activityCancellationMsgActivityIDUnknown)
					continue Process_Decision_Loop
				}

				if ai.StartedID == common.EmptyEventID {
					// We haven't started the activity yet, we can cancel the activity right away and
					// schedule a decision task to ensure the workflow makes progress.
					msBuilder.AddActivityTaskCanceledEvent(ai.ScheduleID, ai.StartedID, *actCancelReqEvent.EventId,
						[]byte(activityCancellationMsgActivityNotStarted), common.StringDefault(request.Identity))
					activityNotStartedCancelled = true
				}

			case workflow.DecisionTypeCancelTimer:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelTimerCounter)
				attributes := d.CancelTimerDecisionAttributes
				if err = validateTimerCancelAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadCancelTimerAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				if msBuilder.AddTimerCanceledEvent(completedID, attributes, common.StringDefault(request.Identity)) == nil {
					msBuilder.AddCancelTimerFailedEvent(completedID, attributes, common.StringDefault(request.Identity))
				} else {
					// timer deletion is success. we need to rebuild the timer builder
					// since timer builder has a local cached version of timers
					tBuilder = e.getTimerBuilder(context.getExecution())
					tBuilder.loadUserTimers(msBuilder)

					// timer deletion is a success, we may have deleted a fired timer in
					// which case we should reset hasBufferedEvents
					hasUnhandledEvents = msBuilder.HasBufferedEvents()
				}

			case workflow.DecisionTypeRecordMarker:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeRecordMarkerCounter)
				attributes := d.RecordMarkerDecisionAttributes
				if err = validateRecordMarkerAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRecordMarkerAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Details, "RecordMarkerDecisionAttributes.Details exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
					break Process_Decision_Loop
				}

				msBuilder.AddRecordMarkerEvent(completedID, attributes)

			case workflow.DecisionTypeRequestCancelExternalWorkflowExecution:
				e.metricsClient.IncCounter(metrics.HistoryRespondDecisionTaskCompletedScope,
					metrics.DecisionTypeCancelExternalWorkflowCounter)
				attributes := d.RequestCancelExternalWorkflowExecutionDecisionAttributes
				if err = validateCancelExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadRequestCancelExternalWorkflowExecutionAttributes
					failMessage = err.Error()
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
				if err = validateSignalExternalWorkflowExecutionAttributes(attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadSignalWorkflowExecutionAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "SignalExternalWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
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
					e.logger.Warn("Multiple completion decisions", tag.WorkflowDecisionType(int64(*d.DecisionType)), tag.ErrorTypeMultipleCompletionDecisions)
					continue Process_Decision_Loop
				}
				attributes := d.ContinueAsNewWorkflowExecutionDecisionAttributes
				if err = validateContinueAsNewWorkflowExecutionAttributes(executionInfo, attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadContinueAsNewAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "ContinueAsNewWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
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

				_, newStateBuilder, err := msBuilder.AddContinueAsNewEvent(completedID, completedID, domainEntry, parentDomainName, attributes, eventStoreVersion)
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
				if err = validateStartChildExecutionAttributes(executionInfo, attributes, maxIDLengthLimit); err != nil {
					failDecision = true
					failCause = workflow.DecisionTaskFailedCauseBadStartChildExecutionAttributes
					failMessage = err.Error()
					break Process_Decision_Loop
				}
				failWorkflow, err := sizeChecker.failWorkflowIfBlobSizeExceedsLimit(attributes.Input, "StartChildWorkflowExecutionDecisionAttributes.Input exceeds size limit.")
				if err != nil {
					return nil, err
				}
				if failWorkflow {
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
			e.logger.Info("Failing the decision.", tag.WorkflowDecisionFailCause(int64(failCause)),
				tag.WorkflowID(token.WorkflowID),
				tag.WorkflowRunID(token.RunID),
				tag.WorkflowDomainID(domainID))
			var err1 error
			msBuilder, err1 = e.failDecision(context, scheduleID, startedID, failCause, []byte(failMessage), request)
			if err1 != nil {
				return nil, err1
			}
			tBuilder = e.getTimerBuilder(context.getExecution())
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
		createNewDecisionTask := !isComplete && (hasUnhandledEvents ||
			request.GetForceCreateNewDecisionTask() || activityNotStartedCancelled)
		var newDecisionTaskScheduledID int64
		if createNewDecisionTask {
			di := msBuilder.AddDecisionTaskScheduledEvent()
			if di == nil {
				return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}
			newDecisionTaskScheduledID = di.ScheduleID
			// skip transfer task for decision if request asking to return new decision task
			if !request.GetReturnNewDecisionTask() {
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(context.getExecution())
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
			tranT, timerT, err := e.getWorkflowHistoryCleanupTasks(
				domainID,
				workflowExecution.GetWorkflowId(),
				tBuilder)
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
				request.GetIdentity(), "", "", "", 0)

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
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
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
			retryTask := msBuilder.CreateActivityRetryTimer(ai, req.FailedRequest.GetReason())
			if retryTask != nil {
				// need retry
				postActions.timerTasks = append(postActions.timerTasks, retryTask)
			} else {
				// no more retry, and we want to record the failure event
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
				e.logger.Debug("Heartbeat failed")
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
				e.logger.Debug(fmt.Sprintf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, Exist: %v", scheduleID, ai,
					isRunning))
				return nil, ErrActivityTaskNotFound
			}

			cancelRequested = ai.CancelRequested

			e.logger.Debug(fmt.Sprintf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, CancelRequested: %v",
				scheduleID, ai, cancelRequested))

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

	return e.updateWorkflowExecution(ctx, domainID, execution, false, true,
		func(msBuilder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			executionInfo := msBuilder.GetExecutionInfo()
			maxAllowedSignals := e.config.MaximumSignalsPerExecution(domainEntry.GetInfo().Name)
			if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
				e.logger.Info("Execution limit reached for maximum signals", tag.WorkflowSignalCount(executionInfo.SignalCount),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.WorkflowDomainID(domainID))
				return nil, ErrSignalsLimitExceeded
			}

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

			if msBuilder.AddWorkflowExecutionSignaled(request.GetSignalName(), request.GetInput(), request.GetIdentity()) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			return nil, nil
		})
}

func (e *historyEngineImpl) SignalWithStartWorkflowExecution(ctx context.Context, signalWithStartRequest *h.SignalWithStartWorkflowExecutionRequest) (
	retResp *workflow.StartWorkflowExecutionResponse, retError error) {

	domainEntry, retError := e.getActiveDomainEntry(signalWithStartRequest.DomainUUID)
	if retError != nil {
		return
	}
	domainID := domainEntry.GetInfo().ID

	sRequest := signalWithStartRequest.SignalWithStartRequest
	execution := workflow.WorkflowExecution{
		WorkflowId: sRequest.WorkflowId,
	}

	var prevMutableState mutableState
	attempt := 0

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)

	if err0 == nil {
		defer func() { release(retError) }()
	Just_Signal_Loop:
		for ; attempt < conditionalRetryCount; attempt++ {
			// workflow not exist, will create workflow then signal
			msBuilder, err1 := context.loadWorkflowExecution()
			if err1 != nil {
				if _, ok := err1.(*workflow.EntityNotExistsError); ok {
					break
				}
				return nil, err1
			}
			// workflow exist but not running, will restart workflow then signal
			if !msBuilder.IsWorkflowExecutionRunning() {
				prevMutableState = msBuilder
				break
			}

			executionInfo := msBuilder.GetExecutionInfo()
			maxAllowedSignals := e.config.MaximumSignalsPerExecution(domainEntry.GetInfo().Name)
			if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
				e.logger.Info("Execution limit reached for maximum signals", tag.WorkflowSignalCount(executionInfo.SignalCount),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.WorkflowDomainID(domainID))
				return nil, ErrSignalsLimitExceeded
			}

			if msBuilder.AddWorkflowExecutionSignaled(sRequest.GetSignalName(), sRequest.GetSignalInput(), sRequest.GetIdentity()) == nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			var transferTasks []persistence.Task
			var timerTasks []persistence.Task
			// Create a transfer task to schedule a decision task
			if !msBuilder.HasPendingDecisionTask() {
				di := msBuilder.AddDecisionTaskScheduledEvent()
				if di == nil {
					return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
				}
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(context.getExecution())
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						executionInfo.StickyScheduleToStartTimeout)
					timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
				}
			}
			// Generate a transaction ID for appending events to history
			var transactionID int64
			transactionID, retError = e.shard.GetNextTransferTaskID()
			if retError != nil {
				return
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
			return &workflow.StartWorkflowExecutionResponse{RunId: context.getExecution().RunId}, nil
		} // end for Just_Signal_Loop
		if attempt == conditionalRetryCount {
			return nil, ErrMaxAttemptsExceeded
		}
	} else {
		if _, ok := err0.(*workflow.EntityNotExistsError); !ok {
			return nil, err0
		}
		// workflow not exist, will create workflow then signal
	}

	// Start workflow and signal
	startRequest := getStartRequest(domainID, sRequest)
	request := startRequest.StartRequest
	retError = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if retError != nil {
		return
	}

	execution = workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      common.StringPtr(uuid.New()),
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()
	msBuilder := e.createMutableState(clusterMetadata, domainEntry)
	var eventStoreVersion int32
	if e.config.EnableEventsV2(request.GetDomain()) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		if retError = msBuilder.SetHistoryTree(*execution.RunId); retError != nil {
			return
		}
	}

	if prevMutableState != nil {
		if prevMutableState.GetLastWriteVersion() > msBuilder.GetCurrentVersion() {
			return nil, ce.NewDomainNotActiveError(
				domainEntry.GetInfo().Name,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevMutableState.GetLastWriteVersion()),
			)
		}
		policy := workflow.WorkflowIdReusePolicyAllowDuplicate
		if request.WorkflowIdReusePolicy != nil {
			policy = *request.WorkflowIdReusePolicy
		}

		retError = e.applyWorkflowIDReusePolicyForSigWithStart(prevMutableState.GetExecutionInfo(), domainID, execution, policy)
		if retError != nil {
			return
		}
	}

	// Generate first decision task event.
	taskList := request.TaskList.GetName()
	// Add WF start event
	startedEvent := msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if startedEvent == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}
	// Add signal event
	if msBuilder.AddWorkflowExecutionSignaled(sRequest.GetSignalName(), sRequest.GetSignalInput(), sRequest.GetIdentity()) == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution signaled event."}
	}
	// first decision task
	firstDecisionTask := msBuilder.AddDecisionTaskScheduledEvent()
	if firstDecisionTask == nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
	}
	transferTasks := []persistence.Task{
		&persistence.DecisionTask{
			DomainID: domainID, TaskList: taskList, ScheduleID: firstDecisionTask.ScheduleID,
		},
		&persistence.RecordWorkflowStartedTask{},
	}
	// first timer task
	duration := time.Duration(*request.ExecutionStartToCloseTimeoutSeconds) * time.Second
	timerTasks := []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(duration),
	}}
	// first replication task
	replicationTasks := generateFirstReplicationTask(msBuilder, clusterMetadata, domainEntry)
	// set versions and timestamp for timer and transfer tasks
	setTaskInfo(msBuilder.GetCurrentVersion(), time.Now(), transferTasks, timerTasks)

	historySize, retError := e.appendFirstBatchHistoryEvents(msBuilder, domainID, execution)
	if retError != nil {
		return
	}
	// delete history if createWorkflow failed, otherwise history will leak
	shouldDeleteHistory := true
	defer func() {
		if shouldDeleteHistory {
			e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
		}
	}()

	msBuilder.IncrementHistorySize(historySize)
	fulfillExecutionInfo(msBuilder, domainID, taskList, execution, startedEvent.GetEventId())

	if prevMutableState != nil {
		createMode := persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID := prevMutableState.GetExecutionInfo().RunID
		lastWriteVersion := prevMutableState.GetLastWriteVersion()
		retError = e.createWorkflow(startRequest, msBuilder, createMode, prevRunID, lastWriteVersion, firstDecisionTask, transferTasks, timerTasks, replicationTasks, clusterMetadata)
	} else {
		createMode := persistence.CreateWorkflowModeBrandNew
		retError = e.createWorkflow(startRequest, msBuilder, createMode, "", 0, firstDecisionTask, transferTasks, timerTasks, replicationTasks, clusterMetadata)
	}

	t, ok := retError.(*persistence.WorkflowExecutionAlreadyStartedError)
	if ok {
		if t.StartRequestID == *request.RequestId {
			return &workflow.StartWorkflowExecutionResponse{
				RunId: common.StringPtr(t.RunID),
			}, nil
			// delete history is expected here because duplicate start request will create history with different rid
		}
	}

	if retError == nil {
		shouldDeleteHistory = false
		e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
		return &workflow.StartWorkflowExecutionResponse{
			RunId: execution.RunId,
		}, nil
	}
	return
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

			err := failInFlightDecisionToClearBufferedEvents(msBuilder)
			if err != nil {
				return nil, err
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

	return e.updateWorkflowExecutionWithAction(ctx, domainID, execution,
		func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			postActions := &updateWorkflowAction{
				createDecision: true,
				transferTasks:  []persistence.Task{&persistence.RecordWorkflowStartedTask{}},
			}

			startEvent, _ := msBuilder.GetStartEvent()
			executionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
			if scheduleRequest.GetIsFirstDecision() && executionTimestamp.After(time.Now()) {
				postActions.timerTasks = append(postActions.timerTasks, &persistence.WorkflowBackoffTimerTask{
					VisibilityTimestamp: executionTimestamp,
					TimeoutType:         persistence.WorkflowBackoffTimeoutTypeCron,
				})
				postActions.createDecision = false
			}

			return postActions, nil
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

func (e *historyEngineImpl) ReplicateRawEvents(ctx context.Context, replicateRequest *h.ReplicateRawEventsRequest) error {
	return e.replicator.ApplyRawEvents(ctx, replicateRequest)
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

func (e *historyEngineImpl) SyncActivity(ctx context.Context, request *h.SyncActivityRequest) (retError error) {
	return e.replicator.SyncActivity(ctx, request)
}

func (e *historyEngineImpl) ResetWorkflowExecution(ctx context.Context, resetRequest *h.ResetWorkflowExecutionRequest) (response *workflow.ResetWorkflowExecutionResponse, retError error) {
	return e.resetor.ResetWorkflowExecution(ctx, resetRequest)
}

func (e *historyEngineImpl) DeleteExecutionFromVisibility(task *persistence.TimerTaskInfo) error {
	request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
		DomainID:   task.DomainID,
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		TaskID:     task.TaskID,
	}
	return e.visibilityMgr.DeleteWorkflowExecution(request) // delete from db
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
		tBuilder := e.getTimerBuilder(context.getExecution())

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
			tranT, timerT, err := e.getWorkflowHistoryCleanupTasks(
				domainID,
				execution.GetWorkflowId(),
				tBuilder)
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
				if di == nil {
					return &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
				}
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.TaskList,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.IsStickyTaskListEnabled() {
					tBuilder := e.getTimerBuilder(context.getExecution())
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

func (e *historyEngineImpl) getWorkflowHistoryCleanupTasks(
	domainID, workflowID string,
	tBuilder *timerBuilder,
) (persistence.Task, persistence.Task, error) {
	return getWorkflowHistoryCleanupTasksFromShard(e.shard, domainID, workflowID, tBuilder)
}

func getWorkflowHistoryCleanupTasksFromShard(
	shard ShardContext,
	domainID, workflowID string,
	tBuilder *timerBuilder,
) (persistence.Task, persistence.Task, error) {

	var retentionInDays int32
	domainEntry, err := shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return nil, nil, err
		}
	} else {
		retentionInDays = domainEntry.GetRetentionDays(workflowID)
	}
	deleteTask := createDeleteHistoryEventTimerTask(tBuilder, retentionInDays)
	return &persistence.CloseExecutionTask{}, deleteTask, nil
}

func createDeleteHistoryEventTimerTask(tBuilder *timerBuilder, retentionInDays int32) *persistence.DeleteHistoryEventTask {
	retention := time.Duration(retentionInDays) * time.Hour * 24
	if tBuilder != nil {
		return tBuilder.createDeleteHistoryEventTimerTask(retention)
	}
	expiryTime := clock.NewRealTimeSource().Now().Add(retention)
	return &persistence.DeleteHistoryEventTask{
		VisibilityTimestamp: expiryTime,
	}
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
	response.EventStoreVersion = common.Int32Ptr(msBuilder.GetEventStoreVersion())
	response.BranchToken = msBuilder.GetCurrentBranch()

	return response
}

func (e *historyEngineImpl) deleteEvents(domainID string, execution workflow.WorkflowExecution, eventStoreVersion int32, branchToken []byte) {
	// We created the history events but failed to create workflow execution, so cleanup the history which could cause
	// us to leak history events which are never cleaned up. Cleaning up the events is absolutely safe here as they
	// are always created for a unique run_id which is not visible beyond this call yet.
	// TODO: Handle error on deletion of execution history
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		e.historyV2Mgr.DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     common.IntPtr(e.shard.GetShardID()),
		})
	} else {
		e.historyMgr.DeleteWorkflowExecutionHistory(&persistence.DeleteWorkflowExecutionHistoryRequest{
			DomainID:  domainID,
			Execution: execution,
		})
	}
}

func (e *historyEngineImpl) failDecision(context workflowExecutionContext, scheduleID, startedID int64,
	cause workflow.DecisionTaskFailedCause, details []byte, request *workflow.RespondDecisionTaskCompletedRequest) (mutableState, error) {
	// Clear any updates we have accumulated so far
	context.clear()

	// Reload workflow execution so we can apply the decision task failure event
	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	msBuilder.AddDecisionTaskFailedEvent(scheduleID, startedID, cause, nil, request.GetIdentity(), "", "", "", 0)

	// Return new builder back to the caller for further updates
	return msBuilder, nil
}

func (e *historyEngineImpl) getTimerBuilder(we *workflow.WorkflowExecution) *timerBuilder {
	log := e.logger.WithTags(tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
	return newTimerBuilder(e.shard.GetConfig(), log, clock.NewRealTimeSource())
}

func (s *shardContextWrapper) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
	resp, err := s.ShardContext.UpdateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.TransferTasks)
		if len(request.ReplicationTasks) > 0 {
			s.replicatorProcessor.notifyNewTask()
		}
	}
	return resp, err
}

func (s *shardContextWrapper) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
	resp, err := s.ShardContext.CreateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.TransferTasks)
		if len(request.ReplicationTasks) > 0 {
			s.replicatorProcessor.notifyNewTask()
		}
	}
	return resp, err
}

func (s *shardContextWrapper) NotifyNewHistoryEvent(event *historyEventNotification) error {
	s.historyEventNotifier.NotifyNewHistoryEvent(event)
	err := s.ShardContext.NotifyNewHistoryEvent(event)
	return err
}

func validateActivityScheduleAttributes(attributes *workflow.ScheduleActivityTaskDecisionAttributes, wfTimeout int32, maxIDLengthLimit int) error {
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

	if len(attributes.GetActivityId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityID exceeds length limit."}
	}

	if len(attributes.GetActivityType().GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityType exceeds length limit."}
	}

	if len(attributes.GetDomain()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if attributes.GetScheduleToCloseTimeoutSeconds() < 0 || attributes.GetScheduleToStartTimeoutSeconds() < 0 ||
		attributes.GetStartToCloseTimeoutSeconds() < 0 || attributes.GetHeartbeatTimeoutSeconds() < 0 {
		return &workflow.BadRequestError{Message: "A valid timeout may not be negative."}
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
	// ensure activity's SCHEDULE_TO_START and SCHEDULE_TO_CLOSE is as long as expiration on retry policy
	p := attributes.RetryPolicy
	if p != nil {
		expiration := p.GetExpirationIntervalInSeconds()
		if expiration == 0 {
			expiration = wfTimeout
		}
		if attributes.GetScheduleToStartTimeoutSeconds() < expiration {
			attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(expiration)
		}
		if attributes.GetScheduleToCloseTimeoutSeconds() < expiration {
			attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(expiration)
		}
	}
	return nil
}

func validateTimerScheduleAttributes(attributes *workflow.StartTimerDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "StartTimerDecisionAttributes is not set on decision."}
	}
	if attributes.TimerId == nil || *attributes.TimerId == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	if len(attributes.GetTimerId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TimerId exceeds length limit."}
	}
	if attributes.StartToFireTimeoutSeconds == nil || *attributes.StartToFireTimeoutSeconds <= 0 {
		return &workflow.BadRequestError{Message: "A valid StartToFireTimeoutSeconds is not set on decision."}
	}
	return nil
}

func validateActivityCancelAttributes(attributes *workflow.RequestCancelActivityTaskDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelActivityTaskDecisionAttributes is not set on decision."}
	}
	if attributes.ActivityId == nil || *attributes.ActivityId == "" {
		return &workflow.BadRequestError{Message: "ActivityId is not set on decision."}
	}
	if len(attributes.GetActivityId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "ActivityId exceeds length limit."}
	}
	return nil
}

func validateTimerCancelAttributes(attributes *workflow.CancelTimerDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "CancelTimerDecisionAttributes is not set on decision."}
	}
	if attributes.TimerId == nil || *attributes.TimerId == "" {
		return &workflow.BadRequestError{Message: "TimerId is not set on decision."}
	}
	if len(attributes.GetTimerId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TimerId exceeds length limit."}
	}
	return nil
}

func validateRecordMarkerAttributes(attributes *workflow.RecordMarkerDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RecordMarkerDecisionAttributes is not set on decision."}
	}
	if attributes.MarkerName == nil || *attributes.MarkerName == "" {
		return &workflow.BadRequestError{Message: "MarkerName is not set on decision."}
	}
	if len(attributes.GetMarkerName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "MarkerName exceeds length limit."}
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

func validateCancelExternalWorkflowExecutionAttributes(attributes *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "RequestCancelExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	if len(attributes.GetDomain()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}
	if len(attributes.GetWorkflowId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}
	runID := attributes.GetRunId()
	if runID != "" && uuid.Parse(runID) == nil {
		return &workflow.BadRequestError{Message: "Invalid RunId set on decision."}
	}

	return nil
}

func validateSignalExternalWorkflowExecutionAttributes(attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes, maxIDLengthLimit int) error {
	if attributes == nil {
		return &workflow.BadRequestError{Message: "SignalExternalWorkflowExecutionDecisionAttributes is not set on decision."}
	}
	if attributes.Execution == nil {
		return &workflow.BadRequestError{Message: "Execution is nil on decision."}
	}
	if attributes.Execution.WorkflowId == nil {
		return &workflow.BadRequestError{Message: "WorkflowId is not set on decision."}
	}
	if len(attributes.GetDomain()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}
	if len(attributes.Execution.GetWorkflowId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}

	targetRunID := attributes.Execution.GetRunId()
	if targetRunID != "" && uuid.Parse(targetRunID) == nil {
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
	attributes *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes, maxIDLengthLimit int) error {
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
	if len(attributes.TaskList.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TaskList exceeds length limit."}
	}
	if len(attributes.WorkflowType.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
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
	attributes *workflow.StartChildWorkflowExecutionDecisionAttributes, maxIDLengthLimit int) error {
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

	if len(attributes.GetDomain()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}

	if len(attributes.GetWorkflowId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}

	if len(attributes.WorkflowType.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
	}

	if err := common.ValidateRetryPolicy(attributes.RetryPolicy); err != nil {
		return err
	}

	if err := cron.ValidateSchedule(attributes.GetCronSchedule()); err != nil {
		return err
	}

	// Inherit tasklist from parent workflow execution if not provided on decision
	if attributes.TaskList == nil || attributes.TaskList.GetName() == "" {
		attributes.TaskList = &workflow.TaskList{Name: common.StringPtr(parentInfo.TaskList)}
	}

	if len(attributes.TaskList.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TaskList exceeds length limit."}
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

func validateStartWorkflowExecutionRequest(request *workflow.StartWorkflowExecutionRequest, maxIDLengthLimit int) error {
	if len(request.GetRequestId()) == 0 {
		return &workflow.BadRequestError{Message: "Missing request ID."}
	}
	if request.ExecutionStartToCloseTimeoutSeconds == nil || request.GetExecutionStartToCloseTimeoutSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "Missing or invalid ExecutionStartToCloseTimeoutSeconds."}
	}
	if request.TaskStartToCloseTimeoutSeconds == nil || request.GetTaskStartToCloseTimeoutSeconds() <= 0 {
		return &workflow.BadRequestError{Message: "Missing or invalid TaskStartToCloseTimeoutSeconds."}
	}
	if request.TaskList == nil || request.TaskList.Name == nil || request.TaskList.GetName() == "" {
		return &workflow.BadRequestError{Message: "Missing Tasklist."}
	}
	if request.WorkflowType == nil || request.WorkflowType.Name == nil || request.WorkflowType.GetName() == "" {
		return &workflow.BadRequestError{Message: "Missing WorkflowType."}
	}
	if len(request.GetDomain()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "Domain exceeds length limit."}
	}
	if len(request.GetWorkflowId()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowId exceeds length limit."}
	}
	if len(request.TaskList.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "TaskList exceeds length limit."}
	}
	if len(request.WorkflowType.GetName()) > maxIDLengthLimit {
		return &workflow.BadRequestError{Message: "WorkflowType exceeds length limit."}
	}

	return common.ValidateRetryPolicy(request.RetryPolicy)
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

func getStartRequest(domainID string,
	request *workflow.SignalWithStartWorkflowExecutionRequest) *h.StartWorkflowExecutionRequest {
	req := &workflow.StartWorkflowExecutionRequest{
		Domain:                              request.Domain,
		WorkflowId:                          request.WorkflowId,
		WorkflowType:                        request.WorkflowType,
		TaskList:                            request.TaskList,
		Input:                               request.Input,
		ExecutionStartToCloseTimeoutSeconds: request.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      request.TaskStartToCloseTimeoutSeconds,
		Identity:                            request.Identity,
		RequestId:                           request.RequestId,
		WorkflowIdReusePolicy:               request.WorkflowIdReusePolicy,
		RetryPolicy:                         request.RetryPolicy,
		CronSchedule:                        request.CronSchedule,
		Memo:                                request.Memo,
	}

	startRequest := common.CreateHistoryStartWorkflowRequest(domainID, req)
	return startRequest
}

func getWorkflowStartedEvent(historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager, eventStoreVersion int32, branchToken []byte, logger log.Logger, domainID, workflowID, runID string, shardID *int) (*workflow.HistoryEvent, error) {
	var events []*workflow.HistoryEvent
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		response, err := historyV2Mgr.ReadHistoryBranch(&persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    common.FirstEventID,
			MaxEventID:    common.FirstEventID + 1,
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nil,
			ShardID:       shardID,
		})
		if err != nil {
			logger.Error("Conflict resolution current workflow finished.", tag.Error(err))
			return nil, err
		}
		events = response.HistoryEvents
	} else {
		response, err := historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: domainID,
			Execution: workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			FirstEventID:  common.FirstEventID,
			NextEventID:   common.FirstEventID + 1,
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nil,
		})
		if err != nil {
			logger.Error("Conflict resolution current workflow finished.", tag.Error(err))
			return nil, err
		}
		events = response.History.Events
	}

	if len(events) == 0 {
		logger.WithTags(tag.WorkflowID(workflowID), tag.WorkflowRunID(runID))
		logError(logger, errNoHistoryFound.Error(), errNoHistoryFound)
		return nil, errNoHistoryFound
	}

	return events[0], nil
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

// for startWorkflowExecution & signalWithStart to handle workflow reuse policy
func (e *historyEngineImpl) applyWorkflowIDReusePolicyForSigWithStart(prevExecutionInfo *persistence.WorkflowExecutionInfo,
	domainID string, execution workflow.WorkflowExecution, wfIDReusePolicy workflow.WorkflowIdReusePolicy) error {

	prevStartRequestID := prevExecutionInfo.CreateRequestID
	prevRunID := prevExecutionInfo.RunID
	prevState := prevExecutionInfo.State
	prevCloseState := prevExecutionInfo.CloseStatus

	return e.applyWorkflowIDReusePolicyHelper(prevStartRequestID, prevRunID, prevState, prevCloseState, domainID, execution, wfIDReusePolicy)

}

func (e *historyEngineImpl) applyWorkflowIDReusePolicyHelper(prevStartRequestID, prevRunID string, prevState, prevCloseState int,
	domainID string, execution workflow.WorkflowExecution, wfIDReusePolicy workflow.WorkflowIdReusePolicy) error {

	// here we know there is some information about the prev workflow, i.e. either running right now
	// or has history check if the this workflow is finished
	if prevState != persistence.WorkflowStateCompleted {
		msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
		return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
	}
	switch wfIDReusePolicy {
	case workflow.WorkflowIdReusePolicyAllowDuplicateFailedOnly:
		if _, ok := FailedWorkflowCloseState[prevCloseState]; !ok {
			msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow ID reuse policy: allow duplicate workflow ID if last run failed."
			return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
		}
	case workflow.WorkflowIdReusePolicyAllowDuplicate:
		// as long as workflow not running, so this case has no check
	case workflow.WorkflowIdReusePolicyRejectDuplicate:
		msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow ID reuse policy: reject duplicate workflow ID."
		return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
	default:
		return &workflow.InternalServiceError{Message: "Failed to process start workflow reuse policy."}
	}

	return nil
}

func getWorkflowAlreadyStartedError(errMsg string, createRequestID string, workflowID string, runID string) error {
	return &workflow.WorkflowExecutionAlreadyStartedError{
		Message:        common.StringPtr(fmt.Sprintf(errMsg, workflowID, runID)),
		StartRequestId: common.StringPtr(fmt.Sprintf("%v", createRequestID)),
		RunId:          common.StringPtr(fmt.Sprintf("%v", runID)),
	}
}
