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
	ctx "context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	ce "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/archiver"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
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
		timeSource           clock.TimeSource
		decisionHandler      decisionHandler
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
		historyArchivers     map[string]carchiver.HistoryArchiver
		visibilityArchivers  map[string]carchiver.VisibilityArchiver
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
	ErrEventsAterWorkflowFinish = &workflow.InternalServiceError{Message: "error validating last event being workflow finish event."}

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
	historyArchivers map[string]carchiver.HistoryArchiver,
	visibilityArchivers map[string]carchiver.VisibilityArchiver,
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
		timeSource:           shard.GetTimeSource(),
		historyMgr:           historyManager,
		historyV2Mgr:         historyV2Manager,
		executionManager:     executionManager,
		visibilityMgr:        visibilityMgr,
		tokenSerializer:      common.NewJSONTaskTokenSerializer(),
		historyCache:         historyCache,
		logger:               logger.WithTags(tag.ComponentHistoryEngine),
		throttledLogger:      shard.GetThrottledLogger().WithTags(tag.ComponentHistoryEngine),
		metricsClient:        shard.GetMetricsClient(),
		historyEventNotifier: historyEventNotifier,
		config:               config,
		archivalClient:       archiver.NewClient(shard.GetMetricsClient(), shard.GetLogger(), publicClient, shard.GetConfig().NumArchiveSystemWorkflows, shard.GetConfig().ArchiveRequestRPS),
		historyArchivers:     historyArchivers,
		visibilityArchivers:  visibilityArchivers,
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
		historyEngImpl.replicator = newHistoryReplicator(shard, clock.NewRealTimeSource(), historyEngImpl, historyCache, shard.GetDomainCache(), historyManager, historyV2Manager,
			logger)
	}
	historyEngImpl.resetor = newWorkflowResetor(historyEngImpl)
	historyEngImpl.decisionHandler = newDecisionHandler(historyEngImpl)

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

func (e *historyEngineImpl) createMutableState(
	clusterMetadata cluster.Metadata,
	domainEntry *cache.DomainCacheEntry,
) mutableState {

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

func (e *historyEngineImpl) generateFirstDecisionTask(
	domainID string,
	msBuilder mutableState,
	parentInfo *h.ParentExecutionInfo,
	taskListName string,
	cronBackoffSeconds int32,
) ([]persistence.Task, *decisionInfo, error) {

	di := &decisionInfo{
		TaskList:        taskListName,
		Version:         common.EmptyVersion,
		ScheduleID:      common.EmptyEventID,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: int32(0),
	}
	var transferTasks []persistence.Task
	var err error
	if parentInfo == nil {
		// RecordWorkflowStartedTask is only created when it is not a Child Workflow
		transferTasks = append(transferTasks, &persistence.RecordWorkflowStartedTask{})
		if cronBackoffSeconds == 0 {
			// DecisionTask is only created when it is not a Child Workflow and no backoff is needed
			di, err = msBuilder.AddDecisionTaskScheduledEvent()
			if err != nil {
				return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
			}

			transferTasks = append(transferTasks, &persistence.DecisionTask{
				DomainID: domainID, TaskList: taskListName, ScheduleID: di.ScheduleID,
			})
		}
	}
	return transferTasks, di, nil
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(
	ctx ctx.Context,
	startRequest *h.StartWorkflowExecutionRequest,
) (resp *workflow.StartWorkflowExecutionResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(startRequest.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	request := startRequest.StartRequest
	err = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := request.GetWorkflowId()
	// grab the current context as a lock, nothing more
	_, currentRelease, err := e.historyCache.getOrCreateCurrentWorkflowExecution(
		ctx,
		domainID,
		workflowID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { currentRelease(retError) }()

	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
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
		if retError = msBuilder.SetHistoryTree(execution.GetRunId()); retError != nil {
			return
		}
	}

	_, err = msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if err != nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}

	taskList := request.TaskList.GetName()
	cronBackoffSeconds := startRequest.GetFirstDecisionTaskBackoffSeconds()
	// Generate first decision task event if not child WF and no first decision task backoff
	transferTasks, _, err := e.generateFirstDecisionTask(domainID, msBuilder, startRequest.ParentExecutionInfo, taskList, cronBackoffSeconds)
	if err != nil {
		return nil, err
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

	context := newWorkflowExecutionContext(domainID, execution, e.shard, e.executionManager, e.logger)
	createReplicationTask := domainEntry.CanReplicateEvent()
	replicationTasks := []persistence.Task{}
	var replicationTask persistence.Task
	_, replicationTask, err = context.appendFirstBatchEventsForActive(msBuilder, createReplicationTask)
	if err != nil {
		return nil, err
	}
	if replicationTask != nil {
		replicationTasks = append(replicationTasks, replicationTask)
	}

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = context.createWorkflowExecution(
		msBuilder, e.currentClusterName, createReplicationTask, e.timeSource.Now(),
		transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
	if err != nil {
		if t, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			if t.StartRequestID == *request.RequestId {
				e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
				return &workflow.StartWorkflowExecutionResponse{
					RunId: common.StringPtr(t.RunID),
				}, nil
				// delete history is expected here because duplicate start request will create history with different rid
			}

			if msBuilder.GetCurrentVersion() < t.LastWriteVersion {
				e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
				return nil, ce.NewDomainNotActiveError(
					*request.Domain,
					clusterMetadata.GetCurrentClusterName(),
					clusterMetadata.ClusterNameForFailoverVersion(t.LastWriteVersion),
				)
			}

			// create as ID reuse
			createMode = persistence.CreateWorkflowModeWorkflowIDReuse
			prevRunID = t.RunID
			prevLastWriteVersion = t.LastWriteVersion
			err = e.applyWorkflowIDReusePolicyHelper(t.StartRequestID, prevRunID, t.State, t.CloseStatus, domainID, execution, startRequest.StartRequest.GetWorkflowIdReusePolicy())
			if err != nil {
				e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
				return nil, err
			}
			err = context.createWorkflowExecution(
				msBuilder, e.currentClusterName, createReplicationTask, e.timeSource.Now(),
				transferTasks, replicationTasks, timerTasks,
				createMode, prevRunID, prevLastWriteVersion,
			)
		}
	}

	e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
	if err != nil {
		return nil, err
	}
	return &workflow.StartWorkflowExecutionResponse{
		RunId: execution.RunId,
	}, nil
}

// GetMutableState retrieves the mutable state of the workflow execution
func (e *historyEngineImpl) GetMutableState(
	ctx ctx.Context,
	request *h.GetMutableStateRequest,
) (*h.GetMutableStateResponse, error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	//TODO: GetMutableState can be a long poll and with 3DC, we need to handle when the current branch changes.
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

func (e *historyEngineImpl) getMutableState(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
) (retResp *h.GetMutableStateResponse, retError error) {

	context, release, retError := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
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
	versionHistories := msBuilder.GetVersionHistories()
	if versionHistories != nil {
		retResp.VersionHistories = versionHistories.ToThrift()
	}
	return
}

func (e *historyEngineImpl) DescribeMutableState(
	ctx ctx.Context,
	request *h.DescribeMutableStateRequest,
) (retResp *h.DescribeMutableStateResponse, retError error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	cacheCtx, dbCtx, release, cacheHit, err := e.historyCache.getAndCreateWorkflowExecution(ctx, domainID, execution)
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
func (e *historyEngineImpl) ResetStickyTaskList(
	ctx ctx.Context,
	resetRequest *h.ResetStickyTaskListRequest,
) (*h.ResetStickyTaskListResponse, error) {

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
func (e *historyEngineImpl) DescribeWorkflowExecution(
	ctx ctx.Context,
	request *h.DescribeWorkflowExecutionRequest,
) (retResp *workflow.DescribeWorkflowExecutionResponse, retError error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := *request.Request.Execution

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
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
			Type:            &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
			StartTime:       common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
			HistoryLength:   common.Int64Ptr(msBuilder.GetNextEventID() - common.FirstEventID),
			AutoResetPoints: executionInfo.AutoResetPoints,
		},
	}

	// TODO: we need to consider adding execution time to mutable state
	// For now execution time will be calculated based on start time and cron schedule/retry policy
	// each time DescribeWorkflowExecution is called.
	backoffDuration := time.Duration(0)
	if executionInfo.HasRetryPolicy && (executionInfo.Attempt > 0) {
		backoffDuration = time.Duration(float64(executionInfo.InitialInterval)*math.Pow(executionInfo.BackoffCoefficient, float64(executionInfo.Attempt-1))) * time.Second
	} else if len(executionInfo.CronSchedule) != 0 {
		backoffDuration = backoff.GetBackoffForNextSchedule(executionInfo.CronSchedule, executionInfo.StartTimestamp)
	}
	result.WorkflowExecutionInfo.ExecutionTime = common.Int64Ptr(result.WorkflowExecutionInfo.GetStartTime() + backoffDuration.Nanoseconds())

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

	if len(msBuilder.GetPendingChildExecutionInfos()) > 0 {
		for _, ch := range msBuilder.GetPendingChildExecutionInfos() {
			p := &workflow.PendingChildExecutionInfo{
				WorkflowID:      common.StringPtr(ch.StartedWorkflowID),
				RunID:           common.StringPtr(ch.StartedRunID),
				WorkflowTypName: common.StringPtr(ch.WorkflowTypeName),
				InitiatedID:     common.Int64Ptr(ch.InitiatedID),
			}
			result.PendingChildren = append(result.PendingChildren, p)
		}
	}

	return result, nil
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	ctx ctx.Context,
	request *h.RecordActivityTaskStartedRequest,
) (*h.RecordActivityTaskStartedResponse, error) {

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

			if _, err := msBuilder.AddActivityTaskStartedEvent(
				ai, scheduleID, requestID, request.PollRequest.GetIdentity(),
			); err != nil {
				return nil, err
			}

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

// ScheduleDecisionTask schedules a decision if no outstanding decision found
func (e *historyEngineImpl) ScheduleDecisionTask(
	ctx ctx.Context,
	req *h.ScheduleDecisionTaskRequest,
) error {
	return e.decisionHandler.handleDecisionTaskScheduled(ctx, req)
}

// RecordDecisionTaskStarted starts a decision
func (e *historyEngineImpl) RecordDecisionTaskStarted(
	ctx ctx.Context,
	request *h.RecordDecisionTaskStartedRequest,
) (*h.RecordDecisionTaskStartedResponse, error) {
	return e.decisionHandler.handleDecisionTaskStarted(ctx, request)
}

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(
	ctx ctx.Context,
	req *h.RespondDecisionTaskCompletedRequest,
) (*h.RespondDecisionTaskCompletedResponse, error) {
	return e.decisionHandler.handleDecisionTaskCompleted(ctx, req)
}

// RespondDecisionTaskFailed fails a decision
func (e *historyEngineImpl) RespondDecisionTaskFailed(
	ctx ctx.Context,
	req *h.RespondDecisionTaskFailedRequest,
) error {
	return e.decisionHandler.handleDecisionTaskFailed(ctx, req)
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(
	ctx ctx.Context,
	req *h.RespondActivityTaskCompletedRequest,
) error {

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

			if _, err := msBuilder.AddActivityTaskCompletedEvent(scheduleID, ai.StartedID, request); err != nil {
				// Unable to add ActivityTaskCompleted event to history
				return nil, &workflow.InternalServiceError{Message: "Unable to add ActivityTaskCompleted event to history."}
			}
			return nil, nil
		})
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(
	ctx ctx.Context,
	req *h.RespondActivityTaskFailedRequest,
) error {

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
				if _, err := msBuilder.AddActivityTaskFailedEvent(scheduleID, ai.StartedID, request); err != nil {
					// Unable to add ActivityTaskFailed event to history
					return nil, &workflow.InternalServiceError{Message: "Unable to add ActivityTaskFailed event to history."}
				}
				postActions.createDecision = true
			}

			return postActions, nil
		})
}

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(
	ctx ctx.Context,
	req *h.RespondActivityTaskCanceledRequest,
) error {

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

			if _, err := msBuilder.AddActivityTaskCanceledEvent(
				scheduleID,
				ai.StartedID,
				ai.CancelRequestID,
				request.Details,
				common.StringDefault(request.Identity)); err != nil {
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
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	ctx ctx.Context,
	req *h.RecordActivityTaskHeartbeatRequest,
) (*workflow.RecordActivityTaskHeartbeatResponse, error) {

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
func (e *historyEngineImpl) RequestCancelWorkflowExecution(
	ctx ctx.Context,
	req *h.RequestCancelWorkflowExecutionRequest,
) error {

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

			if _, err := msBuilder.AddWorkflowExecutionCancelRequestedEvent("", req); err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to cancel workflow execution."}
			}

			return nil, nil
		})
}

func (e *historyEngineImpl) SignalWorkflowExecution(
	ctx ctx.Context,
	signalRequest *h.SignalWorkflowExecutionRequest,
) error {

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

	return e.updateWorkflowExecutionWithAction(ctx, domainID, execution, func(msBuilder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error) {
		executionInfo := msBuilder.GetExecutionInfo()
		createDecisionTask := true
		// Do not create decision task when the workflow is cron and the cron has not been started yet
		if msBuilder.GetExecutionInfo().CronSchedule != "" && !msBuilder.HasProcessedOrPendingDecisionTask() {
			createDecisionTask = false
		}
		postActions := &updateWorkflowAction{
			deleteWorkflow: false,
			createDecision: createDecisionTask,
			timerTasks:     nil,
		}

		if !msBuilder.IsWorkflowExecutionRunning() {
			return nil, ErrWorkflowCompleted
		}

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
				return postActions, nil
			}
			msBuilder.AddSignalRequested(requestID)
		}

		if _, err := msBuilder.AddWorkflowExecutionSignaled(
			request.GetSignalName(),
			request.GetInput(),
			request.GetIdentity()); err != nil {
			return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
		}

		return postActions, nil
	})
}

func (e *historyEngineImpl) SignalWithStartWorkflowExecution(
	ctx ctx.Context,
	signalWithStartRequest *h.SignalWithStartWorkflowExecutionRequest,
) (retResp *workflow.StartWorkflowExecutionResponse, retError error) {

	domainEntry, err := e.getActiveDomainEntry(signalWithStartRequest.DomainUUID)
	if err != nil {
		return nil, err
	}
	domainID := domainEntry.GetInfo().ID

	sRequest := signalWithStartRequest.SignalWithStartRequest
	execution := workflow.WorkflowExecution{
		WorkflowId: sRequest.WorkflowId,
	}

	var prevMutableState mutableState
	attempt := 0

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)

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

			if _, err := msBuilder.AddWorkflowExecutionSignaled(
				sRequest.GetSignalName(),
				sRequest.GetSignalInput(),
				sRequest.GetIdentity()); err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			var transferTasks []persistence.Task
			var timerTasks []persistence.Task
			// Create a transfer task to schedule a decision task
			if !msBuilder.HasPendingDecisionTask() {
				di, err := msBuilder.AddDecisionTaskScheduledEvent()
				if err != nil {
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
			transactionID, err = e.shard.GetNextTransferTaskID()
			if err != nil {
				return nil, err
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
	err = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}

	workflowID := request.GetWorkflowId()
	// grab the current context as a lock, nothing more
	_, currentRelease, err := e.historyCache.getOrCreateCurrentWorkflowExecution(
		ctx,
		domainID,
		workflowID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { currentRelease(retError) }()

	execution = workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(uuid.New()),
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()
	msBuilder := e.createMutableState(clusterMetadata, domainEntry)
	var eventStoreVersion int32
	if e.config.EnableEventsV2(request.GetDomain()) {
		eventStoreVersion = persistence.EventStoreVersionV2
	}
	if eventStoreVersion == persistence.EventStoreVersionV2 {
		if err = msBuilder.SetHistoryTree(*execution.RunId); err != nil {
			return nil, err
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

		err = e.applyWorkflowIDReusePolicyForSigWithStart(prevMutableState.GetExecutionInfo(), domainID, execution, policy)
		if err != nil {
			return nil, err
		}
	}

	// Generate first decision task event.
	taskList := request.TaskList.GetName()
	// Add WF start event
	_, err = msBuilder.AddWorkflowExecutionStartedEvent(execution, startRequest)
	if err != nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution started event."}
	}
	// Add signal event
	if _, err := msBuilder.AddWorkflowExecutionSignaled(
		sRequest.GetSignalName(),
		sRequest.GetSignalInput(),
		sRequest.GetIdentity()); err != nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution signaled event."}
	}
	// first decision task
	var transferTasks []persistence.Task
	transferTasks, _, err = e.generateFirstDecisionTask(domainID, msBuilder, startRequest.ParentExecutionInfo, taskList, 0)
	if err != nil {
		return nil, err
	}

	// first timer task
	duration := time.Duration(*request.ExecutionStartToCloseTimeoutSeconds) * time.Second
	timerTasks := []persistence.Task{&persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: e.shard.GetTimeSource().Now().Add(duration),
	}}

	context = newWorkflowExecutionContext(domainID, execution, e.shard, e.executionManager, e.logger)
	createReplicationTask := domainEntry.CanReplicateEvent()
	replicationTasks := []persistence.Task{}
	var replicationTask persistence.Task
	_, replicationTask, err = context.appendFirstBatchEventsForActive(msBuilder, createReplicationTask)
	if err != nil {
		return nil, err
	}
	if replicationTask != nil {
		replicationTasks = append(replicationTasks, replicationTask)
	}

	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	if prevMutableState != nil {
		createMode = persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID = prevMutableState.GetExecutionInfo().RunID
		prevLastWriteVersion = prevMutableState.GetLastWriteVersion()
	}
	err = context.createWorkflowExecution(
		msBuilder, e.currentClusterName, createReplicationTask, e.timeSource.Now(),
		transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)

	if t, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
		e.deleteEvents(domainID, execution, eventStoreVersion, msBuilder.GetCurrentBranch())
		if t.StartRequestID == *request.RequestId {
			return &workflow.StartWorkflowExecutionResponse{
				RunId: common.StringPtr(t.RunID),
			}, nil
			// delete history is expected here because duplicate start request will create history with different rid
		}
		return nil, err
	}

	e.timerProcessor.NotifyNewTimers(e.currentClusterName, e.shard.GetCurrentTime(e.currentClusterName), timerTasks)
	if err != nil {
		return nil, err
	}
	return &workflow.StartWorkflowExecutionResponse{
		RunId: execution.RunId,
	}, nil
}

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(
	ctx ctx.Context,
	request *h.RemoveSignalMutableStateRequest,
) error {

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

func (e *historyEngineImpl) TerminateWorkflowExecution(
	ctx ctx.Context,
	terminateRequest *h.TerminateWorkflowExecutionRequest,
) error {

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

			if _, err := msBuilder.AddWorkflowExecutionTerminatedEvent(
				request.GetReason(),
				request.GetDetails(),
				request.GetIdentity(),
			); err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to terminate workflow execution."}
			}

			return nil, nil
		})
}

// RecordChildExecutionCompleted records the completion of child execution into parent execution history
func (e *historyEngineImpl) RecordChildExecutionCompleted(
	ctx ctx.Context,
	completionRequest *h.RecordChildExecutionCompletedRequest,
) error {

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

			var err error
			switch *completionEvent.EventType {
			case workflow.EventTypeWorkflowExecutionCompleted:
				attributes := completionEvent.WorkflowExecutionCompletedEventAttributes
				_, err = msBuilder.AddChildWorkflowExecutionCompletedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionFailed:
				attributes := completionEvent.WorkflowExecutionFailedEventAttributes
				_, err = msBuilder.AddChildWorkflowExecutionFailedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionCanceled:
				attributes := completionEvent.WorkflowExecutionCanceledEventAttributes
				_, err = msBuilder.AddChildWorkflowExecutionCanceledEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTerminated:
				attributes := completionEvent.WorkflowExecutionTerminatedEventAttributes
				_, err = msBuilder.AddChildWorkflowExecutionTerminatedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTimedOut:
				attributes := completionEvent.WorkflowExecutionTimedOutEventAttributes
				_, err = msBuilder.AddChildWorkflowExecutionTimedOutEvent(initiatedID, completedExecution, attributes)
			}

			return nil, err
		})
}

func (e *historyEngineImpl) ReplicateEvents(
	ctx ctx.Context,
	replicateRequest *h.ReplicateEventsRequest,
) error {

	return e.replicator.ApplyEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) ReplicateRawEvents(
	ctx ctx.Context,
	replicateRequest *h.ReplicateRawEventsRequest,
) error {

	return e.replicator.ApplyRawEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) SyncShardStatus(
	ctx ctx.Context,
	request *h.SyncShardStatusRequest,
) error {

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

func (e *historyEngineImpl) SyncActivity(
	ctx ctx.Context,
	request *h.SyncActivityRequest,
) (retError error) {

	return e.replicator.SyncActivity(ctx, request)
}

func (e *historyEngineImpl) ResetWorkflowExecution(
	ctx ctx.Context,
	resetRequest *h.ResetWorkflowExecutionRequest,
) (response *workflow.ResetWorkflowExecutionResponse, retError error) {

	domainEntry, retError := e.getActiveDomainEntry(resetRequest.DomainUUID)
	if retError != nil {
		return
	}
	domainID := domainEntry.GetInfo().ID

	request := resetRequest.ResetRequest
	if request == nil || request.WorkflowExecution == nil || len(request.WorkflowExecution.GetRunId()) == 0 || len(request.WorkflowExecution.GetWorkflowId()) == 0 {
		retError = &workflow.BadRequestError{
			Message: "Require workflowId and runId.",
		}
		return
	}
	if request.GetDecisionFinishEventId() <= common.FirstEventID {
		retError = &workflow.BadRequestError{
			Message: "Decision finish ID must be > 1.",
		}
		return
	}
	baseExecution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	baseContext, baseRelease, retError := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, baseExecution)
	if retError != nil {
		return
	}
	defer func() { baseRelease(retError) }()

	baseMutableState, retError := baseContext.loadWorkflowExecution()
	if retError != nil {
		return
	}

	// also load the current run of the workflow, it can be different from the base runID
	resp, retError := e.executionManager.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: request.WorkflowExecution.GetWorkflowId(),
	})
	if retError != nil {
		return
	}

	var currMutableState mutableState
	var currContext workflowExecutionContext
	var currExecution workflow.WorkflowExecution
	if resp.RunID == baseExecution.GetRunId() {
		currContext = baseContext
		currMutableState = baseMutableState
		currExecution = baseExecution
	} else {
		currExecution = workflow.WorkflowExecution{
			WorkflowId: request.WorkflowExecution.WorkflowId,
			RunId:      common.StringPtr(resp.RunID),
		}
		var currRelease func(err error)
		currContext, currRelease, retError = e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, currExecution)
		if retError != nil {
			return
		}
		defer func() { currRelease(retError) }()
		currMutableState, retError = currContext.loadWorkflowExecution()
		if retError != nil {
			return
		}
	}

	// dedup by requestID
	if currMutableState.GetExecutionInfo().CreateRequestID == request.GetRequestId() {
		response = &workflow.ResetWorkflowExecutionResponse{
			RunId: currExecution.RunId,
		}
		e.logger.Info("Duplicated reset request",
			tag.WorkflowID(currExecution.GetWorkflowId()),
			tag.WorkflowRunID(currExecution.GetRunId()),
			tag.WorkflowDomainID(domainID))
		return
	}

	return e.resetor.ResetWorkflowExecution(ctx, request, baseContext, baseMutableState, currContext, currMutableState)
}

func (e *historyEngineImpl) DeleteExecutionFromVisibility(
	task *persistence.TimerTaskInfo,
) error {

	request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
		DomainID:   task.DomainID,
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		TaskID:     task.TaskID,
	}
	return e.visibilityMgr.DeleteWorkflowExecution(request) // delete from db
}

type updateWorkflowAction struct {
	noop           bool
	deleteWorkflow bool
	createDecision bool
	timerTasks     []persistence.Task
	transferTasks  []persistence.Task
}

func (e *historyEngineImpl) updateWorkflowExecutionWithAction(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	action func(builder mutableState, tBuilder *timerBuilder) (*updateWorkflowAction, error),
) (retError error) {

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
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
		if postActions.noop {
			return nil
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
				di, err := msBuilder.AddDecisionTaskScheduledEvent()
				if err != nil {
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

func (e *historyEngineImpl) updateWorkflowExecution(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	createDeletionTask bool,
	createDecisionTask bool,
	action func(builder mutableState, tBuilder *timerBuilder) ([]persistence.Task, error),
) error {

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

func createDeleteHistoryEventTimerTask(
	tBuilder *timerBuilder,
	retentionInDays int32,
) *persistence.DeleteHistoryEventTask {

	retention := time.Duration(retentionInDays) * time.Hour * 24
	if tBuilder != nil {
		return tBuilder.createDeleteHistoryEventTimerTask(retention)
	}
	expiryTime := clock.NewRealTimeSource().Now().Add(retention)
	return &persistence.DeleteHistoryEventTask{
		VisibilityTimestamp: expiryTime,
	}
}

func (e *historyEngineImpl) deleteEvents(
	domainID string,
	execution workflow.WorkflowExecution,
	eventStoreVersion int32,
	branchToken []byte,
) {

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

func (e *historyEngineImpl) failDecision(
	context workflowExecutionContext,
	scheduleID int64,
	startedID int64,
	cause workflow.DecisionTaskFailedCause,
	details []byte,
	request *workflow.RespondDecisionTaskCompletedRequest,
) (mutableState, error) {

	// Clear any updates we have accumulated so far
	context.clear()

	// Reload workflow execution so we can apply the decision task failure event
	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	if _, err = msBuilder.AddDecisionTaskFailedEvent(
		scheduleID, startedID, cause, nil, request.GetIdentity(), "", "", "", 0,
	); err != nil {
		return nil, err
	}

	// Return new builder back to the caller for further updates
	return msBuilder, nil
}

func (e *historyEngineImpl) getTimerBuilder(
	we *workflow.WorkflowExecution,
) *timerBuilder {

	log := e.logger.WithTags(tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
	return newTimerBuilder(e.shard.GetConfig(), log, clock.NewRealTimeSource())
}

func (s *shardContextWrapper) UpdateWorkflowExecution(
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {

	resp, err := s.ShardContext.UpdateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.UpdateWorkflowMutation.TransferTasks)
		if len(request.UpdateWorkflowMutation.ReplicationTasks) > 0 {
			s.replicatorProcessor.notifyNewTask()
		}
	}
	return resp, err
}

func (s *shardContextWrapper) CreateWorkflowExecution(
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {

	resp, err := s.ShardContext.CreateWorkflowExecution(request)
	if err == nil {
		s.txProcessor.NotifyNewTask(s.currentClusterName, request.NewWorkflowSnapshot.TransferTasks)
		if len(request.NewWorkflowSnapshot.ReplicationTasks) > 0 {
			s.replicatorProcessor.notifyNewTask()
		}
	}
	return resp, err
}

func (s *shardContextWrapper) NotifyNewHistoryEvent(
	event *historyEventNotification,
) error {

	s.historyEventNotifier.NotifyNewHistoryEvent(event)
	err := s.ShardContext.NotifyNewHistoryEvent(event)
	return err
}

func validateStartWorkflowExecutionRequest(
	request *workflow.StartWorkflowExecutionRequest,
	maxIDLengthLimit int,
) error {

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

func validateDomainUUID(
	domainUUID *string,
) (string, error) {

	if domainUUID == nil {
		return "", &workflow.BadRequestError{Message: "Missing domain UUID."}
	} else if uuid.Parse(*domainUUID) == nil {
		return "", &workflow.BadRequestError{Message: "Invalid domain UUID."}
	}
	return *domainUUID, nil
}

func (e *historyEngineImpl) getActiveDomainEntry(
	domainUUID *string,
) (*cache.DomainCacheEntry, error) {

	return getActiveDomainEntryFromShard(e.shard, domainUUID)
}

func getActiveDomainEntryFromShard(
	shard ShardContext,
	domainUUID *string,
) (*cache.DomainCacheEntry, error) {

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

func getScheduleID(
	activityID string,
	msBuilder mutableState,
) (int64, error) {

	if activityID == "" {
		return 0, &workflow.BadRequestError{Message: "Neither ActivityID nor ScheduleID is provided"}
	}
	scheduleID, ok := msBuilder.GetScheduleIDByActivityID(activityID)
	if !ok {
		return 0, &workflow.BadRequestError{Message: fmt.Sprintf("No such activityID: %s\n", activityID)}
	}
	return scheduleID, nil
}

func getStartRequest(
	domainID string,
	request *workflow.SignalWithStartWorkflowExecutionRequest,
) *h.StartWorkflowExecutionRequest {

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
		SearchAttributes:                    request.SearchAttributes,
		Header:                              request.Header,
	}

	startRequest := common.CreateHistoryStartWorkflowRequest(domainID, req)
	return startRequest
}

func setTaskInfo(
	version int64,
	timestamp time.Time,
	transferTasks []persistence.Task,
	timerTasks []persistence.Task,
) {
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
func (e *historyEngineImpl) applyWorkflowIDReusePolicyForSigWithStart(
	prevExecutionInfo *persistence.WorkflowExecutionInfo,
	domainID string,
	execution workflow.WorkflowExecution,
	wfIDReusePolicy workflow.WorkflowIdReusePolicy,
) error {

	prevStartRequestID := prevExecutionInfo.CreateRequestID
	prevRunID := prevExecutionInfo.RunID
	prevState := prevExecutionInfo.State
	prevCloseState := prevExecutionInfo.CloseStatus

	return e.applyWorkflowIDReusePolicyHelper(prevStartRequestID, prevRunID, prevState, prevCloseState, domainID, execution, wfIDReusePolicy)

}

func (e *historyEngineImpl) applyWorkflowIDReusePolicyHelper(
	prevStartRequestID,
	prevRunID string,
	prevState int,
	prevCloseState int,
	domainID string,
	execution workflow.WorkflowExecution,
	wfIDReusePolicy workflow.WorkflowIdReusePolicy,
) error {

	// here we know there is some information about the prev workflow, i.e. either running right now
	// or has history check if the this workflow is finished
	switch prevState {
	case persistence.WorkflowStateCreated,
		persistence.WorkflowStateRunning:
		msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
		return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
	case persistence.WorkflowStateCompleted:
		// previous workflow completed, proceed
	default:
		// persistence.WorkflowStateZombie or unknown type
		return &workflow.InternalServiceError{Message: fmt.Sprintf("Failed to process workflow, workflow has invalid state: %v.", prevState)}
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
