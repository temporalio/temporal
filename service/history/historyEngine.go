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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination historyEngine_mock.go

package history

import (
	"bytes"
	ctx "context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/net/context"

	h "github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	r "github.com/uber/cadence/.gen/go/replicator"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/client"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	ce "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/xdc"
	warchiver "github.com/uber/cadence/service/worker/archiver"
)

const (
	conditionalRetryCount                     = 5
	activityCancellationMsgActivityIDUnknown  = "ACTIVITY_ID_UNKNOWN"
	activityCancellationMsgActivityNotStarted = "ACTIVITY_ID_NOT_STARTED"
	timerCancellationMsgTimerIDUnknown        = "TIMER_ID_UNKNOWN"
	queryFirstDecisionTaskWaitTime            = time.Second
	queryFirstDecisionTaskCheckInterval       = 200 * time.Millisecond
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx ctx.Context, request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx ctx.Context, request *h.GetMutableStateRequest) (*h.GetMutableStateResponse, error)
		PollMutableState(ctx ctx.Context, request *h.PollMutableStateRequest) (*h.PollMutableStateResponse, error)
		DescribeMutableState(ctx ctx.Context, request *h.DescribeMutableStateRequest) (*h.DescribeMutableStateResponse, error)
		ResetStickyTaskList(ctx ctx.Context, resetRequest *h.ResetStickyTaskListRequest) (*h.ResetStickyTaskListResponse, error)
		DescribeWorkflowExecution(ctx ctx.Context, request *h.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error)
		RecordDecisionTaskStarted(ctx ctx.Context, request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx ctx.Context, request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(ctx ctx.Context, request *h.RespondDecisionTaskCompletedRequest) (*h.RespondDecisionTaskCompletedResponse, error)
		RespondDecisionTaskFailed(ctx ctx.Context, request *h.RespondDecisionTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx ctx.Context, request *h.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx ctx.Context, request *h.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx ctx.Context, request *h.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx ctx.Context, request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx ctx.Context, request *h.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx ctx.Context, request *h.SignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx ctx.Context, request *h.SignalWithStartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx ctx.Context, request *h.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx ctx.Context, request *h.TerminateWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx ctx.Context, request *h.ResetWorkflowExecutionRequest) (*workflow.ResetWorkflowExecutionResponse, error)
		ScheduleDecisionTask(ctx ctx.Context, request *h.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(ctx ctx.Context, request *h.RecordChildExecutionCompletedRequest) error
		ReplicateEvents(ctx ctx.Context, request *h.ReplicateEventsRequest) error
		ReplicateRawEvents(ctx ctx.Context, request *h.ReplicateRawEventsRequest) error
		ReplicateEventsV2(ctx ctx.Context, request *h.ReplicateEventsV2Request) error
		SyncShardStatus(ctx ctx.Context, request *h.SyncShardStatusRequest) error
		SyncActivity(ctx ctx.Context, request *h.SyncActivityRequest) error
		GetReplicationMessages(ctx ctx.Context, pollingCluster string, lastReadMessageID int64) (*r.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx ctx.Context, taskInfos []*r.ReplicationTaskInfo) ([]*r.ReplicationTask, error)
		QueryWorkflow(ctx ctx.Context, request *h.QueryWorkflowRequest) (*h.QueryWorkflowResponse, error)
		ReapplyEvents(ctx ctx.Context, domainUUID string, workflowID string, runID string, events []*workflow.HistoryEvent) error
		ReadDLQMessages(ctx ctx.Context, messagesRequest *r.ReadDLQMessagesRequest) (*r.ReadDLQMessagesResponse, error)
		PurgeDLQMessages(ctx ctx.Context, messagesRequest *r.PurgeDLQMessagesRequest) error
		MergeDLQMessages(ctx ctx.Context, messagesRequest *r.MergeDLQMessagesRequest) (*r.MergeDLQMessagesResponse, error)
		RefreshWorkflowTasks(ctx ctx.Context, domainUUID string, execution workflow.WorkflowExecution) error

		NotifyNewHistoryEvent(event *historyEventNotification)
		NotifyNewTransferTasks(tasks []persistence.Task)
		NotifyNewReplicationTasks(tasks []persistence.Task)
		NotifyNewTimerTasks(tasks []persistence.Task)
	}

	historyEngineImpl struct {
		currentClusterName        string
		shard                     ShardContext
		timeSource                clock.TimeSource
		decisionHandler           decisionHandler
		clusterMetadata           cluster.Metadata
		historyV2Mgr              persistence.HistoryManager
		executionManager          persistence.ExecutionManager
		visibilityMgr             persistence.VisibilityManager
		txProcessor               transferQueueProcessor
		timerProcessor            timerQueueProcessor
		replicator                *historyReplicator
		nDCReplicator             nDCHistoryReplicator
		nDCActivityReplicator     nDCActivityReplicator
		replicatorProcessor       ReplicatorQueueProcessor
		historyEventNotifier      historyEventNotifier
		tokenSerializer           common.TaskTokenSerializer
		historyCache              *historyCache
		metricsClient             metrics.Client
		logger                    log.Logger
		throttledLogger           log.Logger
		config                    *Config
		archivalClient            warchiver.Client
		resetor                   workflowResetor
		workflowResetter          workflowResetter
		replicationTaskProcessors []ReplicationTaskProcessor
		publicClient              workflowserviceclient.Interface
		eventsReapplier           nDCEventsReapplier
		matchingClient            matching.Client
		rawMatchingClient         matching.Client
		clientChecker             client.VersionChecker
		replicationDLQHandler     replicationDLQHandler
	}
)

var _ Engine = (*historyEngineImpl)(nil)

var (
	// ErrTaskDiscarded is the error indicating that the timer / transfer task is pending for too long and discarded.
	ErrTaskDiscarded = errors.New("passive task pending for too long")
	// ErrTaskRetry is the error indicating that the timer / transfer task should be retried.
	ErrTaskRetry = errors.New("passive task should retry due to condition in mutable state is not met")
	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("duplicate task, completing it")
	// ErrConflict is exported temporarily for integration test
	ErrConflict = errors.New("conditional update failed")
	// ErrMaxAttemptsExceeded is exported temporarily for integration test
	ErrMaxAttemptsExceeded = errors.New("maximum attempts exceeded to update history")
	// ErrStaleState is the error returned during state update indicating that cached mutable state could be stale
	ErrStaleState = errors.New("cache mutable state could potentially be stale")
	// ErrActivityTaskNotFound is the error to indicate activity task could be duplicate and activity already completed
	ErrActivityTaskNotFound = &workflow.EntityNotExistsError{Message: "activity task not found"}
	// ErrWorkflowCompleted is the error to indicate workflow execution already completed
	ErrWorkflowCompleted = &workflow.EntityNotExistsError{Message: "workflow execution already completed"}
	// ErrWorkflowParent is the error to parent execution is given and mismatch
	ErrWorkflowParent = &workflow.EntityNotExistsError{Message: "workflow parent does not match"}
	// ErrDeserializingToken is the error to indicate task token is invalid
	ErrDeserializingToken = &workflow.BadRequestError{Message: "error deserializing task token"}
	// ErrSignalOverSize is the error to indicate signal input size is > 256K
	ErrSignalOverSize = &workflow.BadRequestError{Message: "signal input size is over 256K"}
	// ErrCancellationAlreadyRequested is the error indicating cancellation for target workflow is already requested
	ErrCancellationAlreadyRequested = &workflow.CancellationAlreadyRequestedError{Message: "cancellation already requested for this workflow execution"}
	// ErrSignalsLimitExceeded is the error indicating limit reached for maximum number of signal events
	ErrSignalsLimitExceeded = &workflow.LimitExceededError{Message: "exceeded workflow execution limit for signal events"}
	// ErrEventsAterWorkflowFinish is the error indicating server error trying to write events after workflow finish event
	ErrEventsAterWorkflowFinish = &workflow.InternalServiceError{Message: "error validating last event being workflow finish event"}
	// ErrQueryEnteredInvalidState is error indicating query entered invalid state
	ErrQueryEnteredInvalidState = &workflow.BadRequestError{Message: "query entered invalid state, this should be impossible"}
	// ErrQueryWorkflowBeforeFirstDecision is error indicating that query was attempted before first decision task completed
	ErrQueryWorkflowBeforeFirstDecision = &workflow.BadRequestError{Message: "workflow must handle at least one decision task before it can be queried"}
	// ErrConsistentQueryNotEnabled is error indicating that consistent query was requested but either cluster or domain does not enable consistent query
	ErrConsistentQueryNotEnabled = &workflow.BadRequestError{Message: "cluster or domain does not enable strongly consistent query but strongly consistent query was requested"}
	// ErrConsistentQueryBufferExceeded is error indicating that too many consistent queries have been buffered and until buffered queries are finished new consistent queries cannot be buffered
	ErrConsistentQueryBufferExceeded = &workflow.InternalServiceError{Message: "consistent query buffer is full, cannot accept new consistent queries"}

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
	replicationTaskFetchers ReplicationTaskFetchers,
	rawMatchingClient matching.Client,
) Engine {
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()

	logger := shard.GetLogger()
	executionManager := shard.GetExecutionManager()
	historyV2Manager := shard.GetHistoryManager()
	historyCache := newHistoryCache(shard)
	historyEngImpl := &historyEngineImpl{
		currentClusterName:   currentClusterName,
		shard:                shard,
		clusterMetadata:      shard.GetClusterMetadata(),
		timeSource:           shard.GetTimeSource(),
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
		archivalClient: warchiver.NewClient(
			shard.GetMetricsClient(),
			logger,
			publicClient,
			shard.GetConfig().NumArchiveSystemWorkflows,
			shard.GetConfig().ArchiveRequestRPS,
			shard.GetService().GetArchiverProvider(),
		),
		publicClient:      publicClient,
		matchingClient:    matching,
		rawMatchingClient: rawMatchingClient,
		clientChecker:     client.NewVersionChecker(),
	}

	historyEngImpl.txProcessor = newTransferQueueProcessor(shard, historyEngImpl, visibilityMgr, matching, historyClient, logger)
	historyEngImpl.timerProcessor = newTimerQueueProcessor(shard, historyEngImpl, matching, logger)
	historyEngImpl.eventsReapplier = newNDCEventsReapplier(shard.GetMetricsClient(), logger)

	// Only start the replicator processor if valid publisher is passed in
	if publisher != nil {
		historyEngImpl.replicatorProcessor = newReplicatorQueueProcessor(
			shard,
			historyEngImpl.historyCache,
			publisher,
			executionManager,
			historyV2Manager,
			logger,
		)
		historyEngImpl.replicator = newHistoryReplicator(
			shard,
			clock.NewRealTimeSource(),
			historyEngImpl,
			historyCache,
			shard.GetDomainCache(),
			historyV2Manager,
			logger,
		)
		historyEngImpl.nDCReplicator = newNDCHistoryReplicator(
			shard,
			historyCache,
			historyEngImpl.eventsReapplier,
			logger,
		)
		historyEngImpl.nDCActivityReplicator = newNDCActivityReplicator(
			shard,
			historyCache,
			logger,
		)
	}
	historyEngImpl.resetor = newWorkflowResetor(historyEngImpl)
	historyEngImpl.workflowResetter = newWorkflowResetter(
		shard,
		historyCache,
		logger,
	)
	historyEngImpl.decisionHandler = newDecisionHandler(historyEngImpl)

	nDCHistoryResender := xdc.NewNDCHistoryResender(
		shard.GetDomainCache(),
		shard.GetService().GetClientBean().GetRemoteAdminClient(currentClusterName),
		func(ctx context.Context, request *h.ReplicateEventsV2Request) error {
			return shard.GetService().GetHistoryClient().ReplicateEventsV2(ctx, request)
		},
		shard.GetService().GetPayloadSerializer(),
		shard.GetLogger(),
	)
	historyRereplicator := xdc.NewHistoryRereplicator(
		currentClusterName,
		shard.GetDomainCache(),
		shard.GetService().GetClientBean().GetRemoteAdminClient(currentClusterName),
		func(ctx context.Context, request *h.ReplicateRawEventsRequest) error {
			return shard.GetService().GetHistoryClient().ReplicateRawEvents(ctx, request)
		},
		shard.GetService().GetPayloadSerializer(),
		replicationTimeout,
		shard.GetLogger(),
	)
	replicationTaskExecutor := newReplicationTaskExecutor(
		currentClusterName,
		shard.GetDomainCache(),
		nDCHistoryResender,
		historyRereplicator,
		historyEngImpl,
		shard.GetMetricsClient(),
		shard.GetLogger(),
	)
	var replicationTaskProcessors []ReplicationTaskProcessor
	for _, replicationTaskFetcher := range replicationTaskFetchers.GetFetchers() {
		replicationTaskProcessor := NewReplicationTaskProcessor(
			shard,
			historyEngImpl,
			config,
			shard.GetMetricsClient(),
			replicationTaskFetcher,
			replicationTaskExecutor,
		)
		replicationTaskProcessors = append(replicationTaskProcessors, replicationTaskProcessor)
	}
	historyEngImpl.replicationTaskProcessors = replicationTaskProcessors
	replicationMessageHandler := newReplicationDLQHandler(shard, replicationTaskExecutor)
	historyEngImpl.replicationDLQHandler = replicationMessageHandler

	shard.SetEngine(historyEngImpl)
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

	clusterMetadata := e.shard.GetClusterMetadata()
	if e.replicatorProcessor != nil && clusterMetadata.GetReplicationConsumerConfig().Type != config.ReplicationConsumerTypeRPC {
		e.replicatorProcessor.Start()
	}

	for _, replicationTaskProcessor := range e.replicationTaskProcessors {
		replicationTaskProcessor.Start()
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

	for _, replicationTaskProcessor := range e.replicationTaskProcessors {
		replicationTaskProcessor.Stop()
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
			e.txProcessor.LockTaskProcessing()
			e.timerProcessor.LockTaskProcessing()
		},
		func(prevDomains []*cache.DomainCacheEntry, nextDomains []*cache.DomainCacheEntry) {
			defer func() {
				e.txProcessor.UnlockTaskPrrocessing()
				e.timerProcessor.UnlockTaskProcessing()
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
				e.timerProcessor.NotifyNewTimers(e.currentClusterName, fakeDecisionTimeoutTask)
			}

			//nolint:errcheck
			e.shard.UpdateDomainNotificationVersion(nextDomains[len(nextDomains)-1].GetNotificationVersion() + 1)
		},
	)
}

func (e *historyEngineImpl) createMutableState(
	clusterMetadata cluster.Metadata,
	domainEntry *cache.DomainCacheEntry,
	runID string,
) (mutableState, error) {

	domainName := domainEntry.GetInfo().Name
	enableNDC := e.config.EnableNDC(domainName)

	var newMutableState mutableState
	if enableNDC {
		// version history applies to both local and global domain
		newMutableState = newMutableStateBuilderWithVersionHistories(
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
			domainEntry,
		)
	} else if domainEntry.IsGlobalDomain() {
		// 2DC XDC protocol
		// all workflows within a global domain should have replication state,
		// no matter whether it will be replicated to multiple target clusters or not
		newMutableState = newMutableStateBuilderWithReplicationState(
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
			domainEntry,
		)
	} else {
		newMutableState = newMutableStateBuilder(
			e.shard,
			e.shard.GetEventsCache(),
			e.logger,
			domainEntry,
		)
	}

	if err := newMutableState.SetHistoryTree(runID); err != nil {
		return nil, err
	}

	return newMutableState, nil
}

func (e *historyEngineImpl) generateFirstDecisionTask(
	mutableState mutableState,
	parentInfo *h.ParentExecutionInfo,
	startEvent *workflow.HistoryEvent,
) error {

	if parentInfo == nil {
		// DecisionTask is only created when it is not a Child Workflow and no backoff is needed
		if err := mutableState.AddFirstDecisionTaskScheduled(
			startEvent,
		); err != nil {
			return err
		}
	}
	return nil
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
	e.overrideStartWorkflowExecutionRequest(domainEntry, request, metrics.HistoryStartWorkflowExecutionScope)

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
	mutableState, err := e.createMutableState(clusterMetadata, domainEntry, execution.GetRunId())
	if err != nil {
		return nil, err
	}

	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		startRequest,
	)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: "Failed to add workflow execution started event.",
		}
	}

	// Generate first decision task event if not child WF and no first decision task backoff
	if err := e.generateFirstDecisionTask(
		mutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, err
	}

	context := newWorkflowExecutionContext(domainID, execution, e.shard, e.executionManager, e.logger)

	now := e.timeSource.Now()
	newWorkflow, newWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		now,
		transactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	historySize, err := context.persistFirstWorkflowEvents(newWorkflowEventsSeq[0])
	if err != nil {
		return nil, err
	}

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = context.createWorkflowExecution(
		newWorkflow, historySize, now,
		createMode, prevRunID, prevLastWriteVersion,
	)
	if err != nil {
		if t, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			if t.StartRequestID == *request.RequestId {
				return &workflow.StartWorkflowExecutionResponse{
					RunId: common.StringPtr(t.RunID),
				}, nil
				// delete history is expected here because duplicate start request will create history with different rid
			}

			if mutableState.GetCurrentVersion() < t.LastWriteVersion {
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
			if err = e.applyWorkflowIDReusePolicyHelper(
				t.StartRequestID,
				prevRunID,
				t.State,
				t.CloseStatus,
				domainID,
				execution,
				startRequest.StartRequest.GetWorkflowIdReusePolicy(),
			); err != nil {
				return nil, err
			}
			err = context.createWorkflowExecution(
				newWorkflow, historySize, now,
				createMode, prevRunID, prevLastWriteVersion,
			)
		}
	}

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

	return e.getMutableStateOrPolling(ctx, request)
}

// PollMutableState retrieves the mutable state of the workflow execution with long polling
func (e *historyEngineImpl) PollMutableState(
	ctx ctx.Context,
	request *h.PollMutableStateRequest,
) (*h.PollMutableStateResponse, error) {

	response, err := e.getMutableStateOrPolling(ctx, &h.GetMutableStateRequest{
		DomainUUID:          request.DomainUUID,
		Execution:           request.Execution,
		ExpectedNextEventId: request.ExpectedNextEventId,
		CurrentBranchToken:  request.CurrentBranchToken})

	if err != nil {
		return nil, err
	}
	return &h.PollMutableStateResponse{
		Execution:                            response.Execution,
		WorkflowType:                         response.WorkflowType,
		NextEventId:                          response.NextEventId,
		PreviousStartedEventId:               response.PreviousStartedEventId,
		LastFirstEventId:                     response.LastFirstEventId,
		TaskList:                             response.TaskList,
		StickyTaskList:                       response.StickyTaskList,
		ClientLibraryVersion:                 response.ClientLibraryVersion,
		ClientFeatureVersion:                 response.ClientFeatureVersion,
		ClientImpl:                           response.ClientImpl,
		StickyTaskListScheduleToStartTimeout: response.StickyTaskListScheduleToStartTimeout,
		CurrentBranchToken:                   response.CurrentBranchToken,
		ReplicationInfo:                      response.ReplicationInfo,
		VersionHistories:                     response.VersionHistories,
		WorkflowState:                        response.WorkflowState,
		WorkflowCloseState:                   response.WorkflowCloseState,
	}, nil
}

func (e *historyEngineImpl) getMutableStateOrPolling(
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
	response, err := e.getMutableState(ctx, domainID, execution)
	if err != nil {
		return nil, err
	}
	if request.CurrentBranchToken == nil {
		request.CurrentBranchToken = response.CurrentBranchToken
	}
	if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
		return nil, &workflow.CurrentBranchChangedError{
			Message:            "current branch token and request branch token doesn't match.",
			CurrentBranchToken: response.CurrentBranchToken}
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
		defer e.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID) //nolint:errcheck
		// check again in case the next event ID is updated
		response, err = e.getMutableState(ctx, domainID, execution)
		if err != nil {
			return nil, err
		}
		// check again if the current branch token changed
		if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
			return nil, &workflow.CurrentBranchChangedError{
				Message:            "current branch token and request branch token doesn't match.",
				CurrentBranchToken: response.CurrentBranchToken}
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
				response.IsWorkflowRunning = common.BoolPtr(event.workflowCloseState == persistence.WorkflowCloseStatusNone)
				response.PreviousStartedEventId = common.Int64Ptr(event.previousStartedEventID)
				response.WorkflowState = common.Int32Ptr(int32(event.workflowState))
				response.WorkflowCloseState = common.Int32Ptr(int32(event.workflowCloseState))
				if !bytes.Equal(request.CurrentBranchToken, event.currentBranchToken) {
					return nil, &workflow.CurrentBranchChangedError{
						Message:            "Current branch token and request branch token doesn't match.",
						CurrentBranchToken: event.currentBranchToken}
				}
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

func (e *historyEngineImpl) QueryWorkflow(
	ctx ctx.Context,
	request *h.QueryWorkflowRequest,
) (retResp *h.QueryWorkflowResponse, retErr error) {

	scope := e.metricsClient.Scope(metrics.HistoryQueryWorkflowScope)

	consistentQueryEnabled := e.config.EnableConsistentQuery() && e.config.EnableConsistentQueryByDomain(request.GetRequest().GetDomain())
	if request.GetRequest().GetQueryConsistencyLevel() == workflow.QueryConsistencyLevelStrong && !consistentQueryEnabled {
		return nil, ErrConsistentQueryNotEnabled
	}

	mutableStateResp, err := e.getMutableState(ctx, request.GetDomainUUID(), *request.GetRequest().GetExecution())
	if err != nil {
		return nil, err
	}
	req := request.GetRequest()
	if !mutableStateResp.GetIsWorkflowRunning() && req.QueryRejectCondition != nil {
		notOpenReject := req.GetQueryRejectCondition() == workflow.QueryRejectConditionNotOpen
		closeStatus := mutableStateResp.GetWorkflowCloseState()
		notCompletedCleanlyReject := req.GetQueryRejectCondition() == workflow.QueryRejectConditionNotCompletedCleanly && closeStatus != persistence.WorkflowCloseStatusCompleted
		if notOpenReject || notCompletedCleanlyReject {
			return &h.QueryWorkflowResponse{
				Response: &workflow.QueryWorkflowResponse{
					QueryRejected: &workflow.QueryRejected{
						CloseStatus: persistence.ToThriftWorkflowExecutionCloseStatus(int(closeStatus)).Ptr(),
					},
				},
			}, nil
		}
	}

	// query cannot be processed unless at least one decision task has finished
	// if first decision task has not finished wait for up to a second for it to complete
	deadline := time.Now().Add(queryFirstDecisionTaskWaitTime)
	for mutableStateResp.GetPreviousStartedEventId() <= 0 && time.Now().Before(deadline) {
		<-time.After(queryFirstDecisionTaskCheckInterval)
		mutableStateResp, err = e.getMutableState(ctx, request.GetDomainUUID(), *request.GetRequest().GetExecution())
		if err != nil {
			return nil, err
		}
	}

	if mutableStateResp.GetPreviousStartedEventId() <= 0 {
		scope.IncCounter(metrics.QueryBeforeFirstDecisionCount)
		return nil, ErrQueryWorkflowBeforeFirstDecision
	}

	de, err := e.shard.GetDomainCache().GetDomainByID(request.GetDomainUUID())
	if err != nil {
		return nil, err
	}

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(ctx, request.GetDomainUUID(), *request.GetRequest().GetExecution())
	if err != nil {
		return nil, err
	}
	defer func() { release(retErr) }()
	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	// There are two ways in which queries get dispatched to decider. First, queries can be dispatched on decision tasks.
	// These decision tasks potentially contain new events and queries. The events are treated as coming before the query in time.
	// The second way in which queries are dispatched to decider is directly through matching; in this approach queries can be
	// dispatched to decider immediately even if there are outstanding events that came before the query. The following logic
	// is used to determine if a query can be safely dispatched directly through matching or if given the desired consistency
	// level must be dispatched on a decision task. There are four cases in which a query can be dispatched directly through
	// matching safely, without violating the desired consistency level:
	// 1. the domain is not active, in this case history is immutable so a query dispatched at any time is consistent
	// 2. the workflow is not running, whenever a workflow is not running dispatching query directly is consistent
	// 3. the client requested eventual consistency, in this case there are no consistency requirements so dispatching directly through matching is safe
	// 4. if there is no pending or started decision it means no events came before query arrived, so its safe to dispatch directly
	safeToDispatchDirectly := !de.IsDomainActive() ||
		!mutableState.IsWorkflowExecutionRunning() ||
		req.GetQueryConsistencyLevel() == workflow.QueryConsistencyLevelEventual ||
		(!mutableState.HasPendingDecision() && !mutableState.HasInFlightDecision())
	if safeToDispatchDirectly {
		release(nil)
		msResp, err := e.getMutableState(ctx, request.GetDomainUUID(), *request.GetRequest().GetExecution())
		if err != nil {
			return nil, err
		}
		req.Execution.RunId = msResp.Execution.RunId
		return e.queryDirectlyThroughMatching(ctx, msResp, request.GetDomainUUID(), req, scope)
	}

	// If we get here it means query could not be dispatched through matching directly, so it must block
	// until either an result has been obtained on a decision task response or until it is safe to dispatch directly through matching.
	sw := scope.StartTimer(metrics.DecisionTaskQueryLatency)
	defer sw.Stop()
	queryReg := mutableState.GetQueryRegistry()
	if len(queryReg.getBufferedIDs()) >= e.config.MaxBufferedQueryCount() {
		scope.IncCounter(metrics.QueryBufferExceededCount)
		return nil, ErrConsistentQueryBufferExceeded
	}
	queryID, termCh := queryReg.bufferQuery(req.GetQuery())
	defer queryReg.removeQuery(queryID)
	release(nil)
	select {
	case <-termCh:
		state, err := queryReg.getTerminationState(queryID)
		if err != nil {
			scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			return nil, err
		}
		switch state.queryTerminationType {
		case queryTerminationTypeCompleted:
			result := state.queryResult
			switch result.GetResultType() {
			case workflow.QueryResultTypeAnswered:
				return &h.QueryWorkflowResponse{
					Response: &workflow.QueryWorkflowResponse{
						QueryResult: result.GetAnswer(),
					},
				}, nil
			case workflow.QueryResultTypeFailed:
				return nil, &workflow.QueryFailedError{Message: result.GetErrorMessage()}
			default:
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
				return nil, ErrQueryEnteredInvalidState
			}
		case queryTerminationTypeUnblocked:
			msResp, err := e.getMutableState(ctx, request.GetDomainUUID(), *request.GetRequest().GetExecution())
			if err != nil {
				return nil, err
			}
			req.Execution.RunId = msResp.Execution.RunId
			return e.queryDirectlyThroughMatching(ctx, msResp, request.GetDomainUUID(), req, scope)
		case queryTerminationTypeFailed:
			return nil, state.failure
		default:
			scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
			return nil, ErrQueryEnteredInvalidState
		}
	case <-ctx.Done():
		scope.IncCounter(metrics.ConsistentQueryTimeoutCount)
		return nil, ctx.Err()
	}
}

func (e *historyEngineImpl) queryDirectlyThroughMatching(
	ctx ctx.Context,
	msResp *h.GetMutableStateResponse,
	domainID string,
	queryRequest *workflow.QueryWorkflowRequest,
	scope metrics.Scope,
) (*h.QueryWorkflowResponse, error) {

	sw := scope.StartTimer(metrics.DirectQueryDispatchLatency)
	defer sw.Stop()

	supportsStickyQuery := e.clientChecker.SupportsStickyQuery(msResp.GetClientImpl(), msResp.GetClientFeatureVersion()) == nil
	if msResp.GetIsStickyTaskListEnabled() &&
		len(msResp.GetStickyTaskList().GetName()) != 0 &&
		supportsStickyQuery &&
		e.config.EnableStickyQuery(queryRequest.GetDomain()) {

		stickyMatchingRequest := &m.QueryWorkflowRequest{
			DomainUUID:   common.StringPtr(domainID),
			QueryRequest: queryRequest,
			TaskList:     msResp.GetStickyTaskList(),
		}

		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickiness
		stickyContext, cancel := context.WithTimeout(context.Background(), time.Duration(msResp.GetStickyTaskListScheduleToStartTimeout())*time.Second)
		stickyStopWatch := scope.StartTimer(metrics.DirectQueryDispatchStickyLatency)
		matchingResp, err := e.rawMatchingClient.QueryWorkflow(stickyContext, stickyMatchingRequest)
		stickyStopWatch.Stop()
		cancel()
		if err == nil {
			scope.IncCounter(metrics.DirectQueryDispatchStickySuccessCount)
			return &h.QueryWorkflowResponse{Response: matchingResp}, nil
		}
		if yarpcError, ok := err.(*yarpcerrors.Status); !ok || yarpcError.Code() != yarpcerrors.CodeDeadlineExceeded {
			e.logger.Error("query directly though matching on sticky failed, will not attempt query on non-sticky",
				tag.WorkflowDomainName(queryRequest.GetDomain()),
				tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
				tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
				tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
				tag.Error(err))
			return nil, err
		}
		if msResp.GetIsWorkflowRunning() {
			e.logger.Info("query direct through matching failed on sticky, clearing sticky before attempting on non-sticky",
				tag.WorkflowDomainName(queryRequest.GetDomain()),
				tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
				tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
				tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
			resetContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			clearStickinessStopWatch := scope.StartTimer(metrics.DirectQueryDispatchClearStickinessLatency)
			_, err := e.ResetStickyTaskList(resetContext, &h.ResetStickyTaskListRequest{
				DomainUUID: common.StringPtr(domainID),
				Execution:  queryRequest.GetExecution(),
			})
			clearStickinessStopWatch.Stop()
			cancel()
			if err != nil && err != ErrWorkflowCompleted {
				return nil, err
			}
			scope.IncCounter(metrics.DirectQueryDispatchClearStickinessSuccessCount)
		}
	}

	if err := common.IsValidContext(ctx); err != nil {
		e.logger.Info("query context timed out before query on non-sticky task list could be attempted",
			tag.WorkflowDomainName(queryRequest.GetDomain()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
		scope.IncCounter(metrics.DirectQueryDispatchTimeoutBeforeNonStickyCount)
		return nil, err
	}

	e.logger.Info("query directly through matching on sticky timed out, attempting to query on non-sticky",
		tag.WorkflowDomainName(queryRequest.GetDomain()),
		tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
		tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
		tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
		tag.WorkflowTaskListName(msResp.GetStickyTaskList().GetName()),
		tag.WorkflowNextEventID(msResp.GetNextEventId()))

	nonStickyMatchingRequest := &m.QueryWorkflowRequest{
		DomainUUID:   common.StringPtr(domainID),
		QueryRequest: queryRequest,
		TaskList:     msResp.TaskList,
	}

	nonStickyStopWatch := scope.StartTimer(metrics.DirectQueryDispatchNonStickyLatency)
	matchingResp, err := e.matchingClient.QueryWorkflow(ctx, nonStickyMatchingRequest)
	nonStickyStopWatch.Stop()
	if err != nil {
		e.logger.Error("query directly though matching on non-sticky failed",
			tag.WorkflowDomainName(queryRequest.GetDomain()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
			tag.Error(err))
		return nil, err
	}
	scope.IncCounter(metrics.DirectQueryDispatchNonStickySuccessCount)
	return &h.QueryWorkflowResponse{Response: matchingResp}, err
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

	mutableState, retError := context.loadWorkflowExecution()
	if retError != nil {
		return
	}

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	executionInfo := mutableState.GetExecutionInfo()
	execution.RunId = context.getExecution().RunId
	workflowState, workflowCloseState := mutableState.GetWorkflowStateCloseStatus()
	retResp = &h.GetMutableStateResponse{
		Execution:                            &execution,
		WorkflowType:                         &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
		LastFirstEventId:                     common.Int64Ptr(mutableState.GetLastFirstEventID()),
		NextEventId:                          common.Int64Ptr(mutableState.GetNextEventID()),
		PreviousStartedEventId:               common.Int64Ptr(mutableState.GetPreviousStartedEventID()),
		TaskList:                             &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
		StickyTaskList:                       &workflow.TaskList{Name: common.StringPtr(executionInfo.StickyTaskList)},
		ClientLibraryVersion:                 common.StringPtr(executionInfo.ClientLibraryVersion),
		ClientFeatureVersion:                 common.StringPtr(executionInfo.ClientFeatureVersion),
		ClientImpl:                           common.StringPtr(executionInfo.ClientImpl),
		IsWorkflowRunning:                    common.BoolPtr(mutableState.IsWorkflowExecutionRunning()),
		StickyTaskListScheduleToStartTimeout: common.Int32Ptr(executionInfo.StickyScheduleToStartTimeout),
		CurrentBranchToken:                   currentBranchToken,
		WorkflowState:                        common.Int32Ptr(int32(workflowState)),
		WorkflowCloseState:                   common.Int32Ptr(int32(workflowCloseState)),
		IsStickyTaskListEnabled:              common.BoolPtr(mutableState.IsStickyTaskListEnabled()),
	}
	replicationState := mutableState.GetReplicationState()
	if replicationState != nil {
		retResp.ReplicationInfo = map[string]*workflow.ReplicationInfo{}
		for k, v := range replicationState.LastReplicationInfo {
			retResp.ReplicationInfo[k] = &workflow.ReplicationInfo{
				Version:     common.Int64Ptr(v.Version),
				LastEventId: common.Int64Ptr(v.LastEventID),
			}
		}
	}
	versionHistories := mutableState.GetVersionHistories()
	if versionHistories != nil {
		retResp.VersionHistories = versionHistories.ToThrift()
	}
	return
}

func (e *historyEngineImpl) DescribeMutableState(
	ctx ctx.Context,
	request *h.DescribeMutableStateRequest,
) (response *h.DescribeMutableStateResponse, retError error) {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return nil, err
	}

	execution := workflow.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	cacheCtx, dbCtx, release, cacheHit, err := e.historyCache.getAndCreateWorkflowExecution(
		ctx, domainID, execution,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	response = &h.DescribeMutableStateResponse{}

	if cacheHit && cacheCtx.(*workflowExecutionContextImpl).mutableState != nil {
		msb := cacheCtx.(*workflowExecutionContextImpl).mutableState
		response.MutableStateInCache, err = e.toMutableStateJSON(msb)
		if err != nil {
			return nil, err
		}
	}

	msb, err := dbCtx.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	response.MutableStateInDatabase, err = e.toMutableStateJSON(msb)
	if err != nil {
		return nil, err
	}

	return response, nil
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

	err = e.updateWorkflowExecution(ctx, domainID, *resetRequest.Execution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}
			mutableState.ClearStickyness()
			return nil
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

	mutableState, err1 := context.loadWorkflowExecution()
	if err1 != nil {
		return nil, err1
	}
	executionInfo := mutableState.GetExecutionInfo()

	result := &workflow.DescribeWorkflowExecutionResponse{
		ExecutionConfiguration: &workflow.WorkflowExecutionConfiguration{
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(executionInfo.TaskList)},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionInfo.WorkflowTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(executionInfo.DecisionStartToCloseTimeout),
		},
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(executionInfo.WorkflowID),
				RunId:      common.StringPtr(executionInfo.RunID),
			},
			Type:             &workflow.WorkflowType{Name: common.StringPtr(executionInfo.WorkflowTypeName)},
			StartTime:        common.Int64Ptr(executionInfo.StartTimestamp.UnixNano()),
			HistoryLength:    common.Int64Ptr(mutableState.GetNextEventID() - common.FirstEventID),
			AutoResetPoints:  executionInfo.AutoResetPoints,
			Memo:             &workflow.Memo{Fields: executionInfo.Memo},
			SearchAttributes: &workflow.SearchAttributes{IndexedFields: executionInfo.SearchAttributes},
		},
	}

	// TODO: we need to consider adding execution time to mutable state
	// For now execution time will be calculated based on start time and cron schedule/retry policy
	// each time DescribeWorkflowExecution is called.
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return nil, err
	}
	backoffDuration := time.Duration(startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstDecisionTaskBackoffSeconds()) * time.Second
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
		closeStatus := persistence.ToThriftWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
		result.WorkflowExecutionInfo.CloseStatus = &closeStatus
		completionEvent, err := mutableState.GetCompletionEvent()
		if err != nil {
			return nil, err
		}
		result.WorkflowExecutionInfo.CloseTime = common.Int64Ptr(completionEvent.GetTimestamp())
	}

	if len(mutableState.GetPendingActivityInfos()) > 0 {
		for _, ai := range mutableState.GetPendingActivityInfos() {
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
			scheduledEvent, err := mutableState.GetActivityScheduledEvent(ai.ScheduleID)
			if err != nil {
				return nil, err
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
				if ai.LastFailureReason != "" {
					p.LastFailureReason = common.StringPtr(ai.LastFailureReason)
					p.LastFailureDetails = ai.LastFailureDetails
				}
				if ai.LastWorkerIdentity != "" {
					p.LastWorkerIdentity = common.StringPtr(ai.LastWorkerIdentity)
				}
			}
			result.PendingActivities = append(result.PendingActivities, p)
		}
	}

	if len(mutableState.GetPendingChildExecutionInfos()) > 0 {
		for _, ch := range mutableState.GetPendingChildExecutionInfos() {
			p := &workflow.PendingChildExecutionInfo{
				WorkflowID:        common.StringPtr(ch.StartedWorkflowID),
				RunID:             common.StringPtr(ch.StartedRunID),
				WorkflowTypName:   common.StringPtr(ch.WorkflowTypeName),
				InitiatedID:       common.Int64Ptr(ch.InitiatedID),
				ParentClosePolicy: common.ParentClosePolicyPtr(ch.ParentClosePolicy),
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
	err = e.updateWorkflowExecution(ctx, domainID, execution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := request.GetScheduleId()
			requestID := request.GetRequestId()
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskStartedScope, metrics.StaleMutableStateCounter)
				return ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if !isRunning {
				// Looks like ActivityTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
				return ErrActivityTaskNotFound
			}

			scheduledEvent, err := mutableState.GetActivityScheduledEvent(scheduleID)
			if err != nil {
				return err
			}
			response.ScheduledEvent = scheduledEvent
			response.ScheduledTimestampOfThisAttempt = common.Int64Ptr(ai.ScheduledTime.UnixNano())

			if ai.StartedID != common.EmptyEventID {
				// If activity is started as part of the current request scope then return a positive response
				if ai.RequestID == requestID {
					response.StartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
					response.Attempt = common.Int64Ptr(int64(ai.Attempt))
					return nil
				}

				// Looks like ActivityTask already started as a result of another call.
				// It is OK to drop the task at this point.
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
				return &h.EventAlreadyStartedError{Message: "Activity task already started."}
			}

			if _, err := mutableState.AddActivityTaskStartedEvent(
				ai, scheduleID, requestID, request.PollRequest.GetIdentity(),
			); err != nil {
				return err
			}

			response.StartedTimestamp = common.Int64Ptr(ai.StartedTime.UnixNano())
			response.Attempt = common.Int64Ptr(int64(ai.Attempt))
			response.HeartbeatDetails = ai.Details

			response.WorkflowType = mutableState.GetWorkflowType()
			response.WorkflowDomain = common.StringPtr(domainName)

			return nil
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

	return e.updateWorkflowExecution(ctx, domainID, workflowExecution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCompletedScope, metrics.StaleMutableStateCounter)
				return ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return ErrActivityTaskNotFound
			}

			if _, err := mutableState.AddActivityTaskCompletedEvent(scheduleID, ai.StartedID, request); err != nil {
				// Unable to add ActivityTaskCompleted event to history
				return &workflow.InternalServiceError{Message: "Unable to add ActivityTaskCompleted event to history."}
			}
			return nil
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
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return nil, err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskFailedScope, metrics.StaleMutableStateCounter)
				return nil, ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return nil, ErrActivityTaskNotFound
			}

			postActions := &updateWorkflowAction{}
			ok, err := mutableState.RetryActivity(ai, req.FailedRequest.GetReason(), req.FailedRequest.GetDetails())
			if err != nil {
				return nil, err
			}
			if !ok {
				// no more retry, and we want to record the failure event
				if _, err := mutableState.AddActivityTaskFailedEvent(scheduleID, ai.StartedID, request); err != nil {
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

	return e.updateWorkflowExecution(ctx, domainID, workflowExecution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRespondActivityTaskCanceledScope, metrics.StaleMutableStateCounter)
				return ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return ErrActivityTaskNotFound
			}

			if _, err := mutableState.AddActivityTaskCanceledEvent(
				scheduleID,
				ai.StartedID,
				ai.CancelRequestID,
				request.Details,
				common.StringDefault(request.Identity)); err != nil {
				// Unable to add ActivityTaskCanceled event to history
				return &workflow.InternalServiceError{Message: "Unable to add ActivityTaskCanceled event to history."}
			}

			return nil
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
	err = e.updateWorkflowExecution(ctx, domainID, workflowExecution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				e.logger.Debug("Heartbeat failed")
				return ErrWorkflowCompleted
			}

			scheduleID := token.ScheduleID
			if scheduleID == common.EmptyEventID { // client call RecordActivityHeartbeatByID, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.ActivityID, mutableState)
				if err0 != nil {
					return err0
				}
			}
			ai, isRunning := mutableState.GetActivityInfo(scheduleID)

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if !isRunning && scheduleID >= mutableState.GetNextEventID() {
				e.metricsClient.IncCounter(metrics.HistoryRecordActivityTaskHeartbeatScope, metrics.StaleMutableStateCounter)
				return ErrStaleState
			}

			if !isRunning || ai.StartedID == common.EmptyEventID ||
				(token.ScheduleID != common.EmptyEventID && token.ScheduleAttempt != int64(ai.Attempt)) {
				return ErrActivityTaskNotFound
			}

			cancelRequested = ai.CancelRequested

			e.logger.Debug(fmt.Sprintf("Activity HeartBeat: scheduleEventID: %v, ActivityInfo: %+v, CancelRequested: %v",
				scheduleID, ai, cancelRequested))

			// Save progress and last HB reported time.
			mutableState.UpdateActivityProgress(ai, request)

			return nil
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

	return e.updateWorkflow(ctx, domainID, execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			executionInfo := mutableState.GetExecutionInfo()
			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowID
				parentRunID := executionInfo.ParentRunID
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, ErrWorkflowParent
				}
			}

			isCancelRequested, cancelRequestID := mutableState.IsCancelRequested()
			if isCancelRequested {
				cancelRequest := req.CancelRequest
				if cancelRequest.RequestId != nil {
					requestID := *cancelRequest.RequestId
					if requestID != "" && cancelRequestID == requestID {
						return updateWorkflowWithNewDecision, nil
					}
				}
				// if we consider workflow cancellation idempotent, then this error is redundant
				// this error maybe useful if this API is invoked by external, not decision from transfer queue
				return nil, ErrCancellationAlreadyRequested
			}

			if _, err := mutableState.AddWorkflowExecutionCancelRequestedEvent("", req); err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to cancel workflow execution."}
			}

			return updateWorkflowWithNewDecision, nil
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

	return e.updateWorkflow(
		ctx,
		domainID,
		execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			executionInfo := mutableState.GetExecutionInfo()
			createDecisionTask := true
			// Do not create decision task when the workflow is cron and the cron has not been started yet
			if mutableState.GetExecutionInfo().CronSchedule != "" && !mutableState.HasProcessedOrPendingDecision() {
				createDecisionTask = false
			}
			postActions := &updateWorkflowAction{
				createDecision: createDecisionTask,
			}

			if !mutableState.IsWorkflowExecutionRunning() {
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
				if mutableState.IsSignalRequested(requestID) {
					return postActions, nil
				}
				mutableState.AddSignalRequested(requestID)
			}

			if _, err := mutableState.AddWorkflowExecutionSignaled(
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
			mutableState, err1 := context.loadWorkflowExecution()
			if err1 != nil {
				if _, ok := err1.(*workflow.EntityNotExistsError); ok {
					break
				}
				return nil, err1
			}
			// workflow exist but not running, will restart workflow then signal
			if !mutableState.IsWorkflowExecutionRunning() {
				prevMutableState = mutableState
				break
			}

			executionInfo := mutableState.GetExecutionInfo()
			maxAllowedSignals := e.config.MaximumSignalsPerExecution(domainEntry.GetInfo().Name)
			if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
				e.logger.Info("Execution limit reached for maximum signals", tag.WorkflowSignalCount(executionInfo.SignalCount),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.WorkflowDomainID(domainID))
				return nil, ErrSignalsLimitExceeded
			}

			if _, err := mutableState.AddWorkflowExecutionSignaled(
				sRequest.GetSignalName(),
				sRequest.GetSignalInput(),
				sRequest.GetIdentity()); err != nil {
				return nil, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}

			// Create a transfer task to schedule a decision task
			if !mutableState.HasPendingDecision() {
				_, err := mutableState.AddDecisionTaskScheduledEvent(false)
				if err != nil {
					return nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
				}
			}

			// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
			// the history and try the operation again.
			if err := context.updateWorkflowExecutionAsActive(e.shard.GetTimeSource().Now()); err != nil {
				if err == ErrConflict {
					continue Just_Signal_Loop
				}
				return nil, err
			}
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
	e.overrideStartWorkflowExecutionRequest(domainEntry, request, metrics.HistorySignalWorkflowExecutionScope)

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
	mutableState, err := e.createMutableState(clusterMetadata, domainEntry, execution.GetRunId())
	if err != nil {
		return nil, err
	}

	if prevMutableState != nil {
		prevLastWriteVersion, err := prevMutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		if prevLastWriteVersion > mutableState.GetCurrentVersion() {
			return nil, ce.NewDomainNotActiveError(
				domainEntry.GetInfo().Name,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevLastWriteVersion),
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

	// Add WF start event
	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		startRequest,
	)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: "Failed to add workflow execution started event.",
		}
	}

	// Add signal event
	if _, err := mutableState.AddWorkflowExecutionSignaled(
		sRequest.GetSignalName(),
		sRequest.GetSignalInput(),
		sRequest.GetIdentity()); err != nil {
		return nil, &workflow.InternalServiceError{Message: "Failed to add workflow execution signaled event."}
	}

	if err = e.generateFirstDecisionTask(
		mutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, err
	}

	context = newWorkflowExecutionContext(domainID, execution, e.shard, e.executionManager, e.logger)

	now := e.timeSource.Now()
	newWorkflow, newWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		now,
		transactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	historySize, err := context.persistFirstWorkflowEvents(newWorkflowEventsSeq[0])
	if err != nil {
		return nil, err
	}

	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	if prevMutableState != nil {
		createMode = persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID = prevMutableState.GetExecutionInfo().RunID
		prevLastWriteVersion, err = prevMutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
	}
	err = context.createWorkflowExecution(
		newWorkflow, historySize, now,
		createMode, prevRunID, prevLastWriteVersion,
	)

	if t, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
		if t.StartRequestID == *request.RequestId {
			return &workflow.StartWorkflowExecutionResponse{
				RunId: common.StringPtr(t.RunID),
			}, nil
			// delete history is expected here because duplicate start request will create history with different rid
		}
		return nil, err
	}

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

	return e.updateWorkflowExecution(ctx, domainID, execution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			mutableState.DeleteSignalRequested(request.GetRequestId())

			return nil
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

	return e.updateWorkflow(
		ctx,
		domainID,
		execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			eventBatchFirstEventID := mutableState.GetNextEventID()
			return updateWorkflowWithoutDecision, terminateWorkflow(
				mutableState,
				eventBatchFirstEventID,
				request.GetReason(),
				request.GetDetails(),
				request.GetIdentity(),
			)
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

	return e.updateWorkflowExecution(ctx, domainID, execution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			initiatedID := *completionRequest.InitiatedId
			completedExecution := completionRequest.CompletedExecution
			completionEvent := completionRequest.CompletionEvent

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(initiatedID)
			if !isRunning || ci.StartedID == common.EmptyEventID {
				return &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			switch *completionEvent.EventType {
			case workflow.EventTypeWorkflowExecutionCompleted:
				attributes := completionEvent.WorkflowExecutionCompletedEventAttributes
				_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionFailed:
				attributes := completionEvent.WorkflowExecutionFailedEventAttributes
				_, err = mutableState.AddChildWorkflowExecutionFailedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionCanceled:
				attributes := completionEvent.WorkflowExecutionCanceledEventAttributes
				_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTerminated:
				attributes := completionEvent.WorkflowExecutionTerminatedEventAttributes
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(initiatedID, completedExecution, attributes)
			case workflow.EventTypeWorkflowExecutionTimedOut:
				attributes := completionEvent.WorkflowExecutionTimedOutEventAttributes
				_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(initiatedID, completedExecution, attributes)
			}

			return err
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

func (e *historyEngineImpl) ReplicateEventsV2(
	ctx ctx.Context,
	replicateRequest *h.ReplicateEventsV2Request,
) error {

	return e.nDCReplicator.ApplyEvents(ctx, replicateRequest)
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
	e.timerProcessor.NotifyNewTimers(clusterName, []persistence.Task{})
	return nil
}

func (e *historyEngineImpl) SyncActivity(
	ctx ctx.Context,
	request *h.SyncActivityRequest,
) (retError error) {

	return e.nDCActivityReplicator.SyncActivity(ctx, request)
}

func (e *historyEngineImpl) ResetWorkflowExecution(
	ctx ctx.Context,
	resetRequest *h.ResetWorkflowExecutionRequest,
) (response *workflow.ResetWorkflowExecutionResponse, retError error) {

	request := resetRequest.ResetRequest
	domainID := resetRequest.GetDomainUUID()
	workflowID := request.WorkflowExecution.GetWorkflowId()
	baseRunID := request.WorkflowExecution.GetRunId()

	baseContext, baseReleaseFn, err := e.historyCache.getOrCreateWorkflowExecution(
		ctx,
		domainID,
		workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(baseRunID),
		},
	)
	if err != nil {
		return nil, err
	}
	defer func() { baseReleaseFn(retError) }()

	baseMutableState, err := baseContext.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}
	if request.GetDecisionFinishEventId() <= common.FirstEventID ||
		request.GetDecisionFinishEventId() >= baseMutableState.GetNextEventID() {
		return nil, &workflow.BadRequestError{
			Message: "Decision finish ID must be > 1 && <= workflow next event ID.",
		}
	}

	// also load the current run of the workflow, it can be different from the base runID
	resp, err := e.executionManager.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: request.WorkflowExecution.GetWorkflowId(),
	})
	if err != nil {
		return nil, err
	}

	currentRunID := resp.RunID
	var currentContext workflowExecutionContext
	var currentMutableState mutableState
	var currentReleaseFn releaseWorkflowExecutionFunc
	if currentRunID == baseRunID {
		currentContext = baseContext
		currentMutableState = baseMutableState
	} else {
		currentContext, currentReleaseFn, err = e.historyCache.getOrCreateWorkflowExecution(
			ctx,
			domainID,
			workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(currentRunID),
			},
		)
		if err != nil {
			return nil, err
		}
		defer func() { currentReleaseFn(retError) }()

		currentMutableState, err = currentContext.loadWorkflowExecution()
		if err != nil {
			return nil, err
		}
	}

	// dedup by requestID
	if currentMutableState.GetExecutionInfo().CreateRequestID == request.GetRequestId() {
		e.logger.Info("Duplicated reset request",
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(currentRunID),
			tag.WorkflowDomainID(domainID))
		return &workflow.ResetWorkflowExecutionResponse{
			RunId: common.StringPtr(currentRunID),
		}, nil
	}

	// TODO when NDC is rolled out, remove this block
	if baseMutableState.GetVersionHistories() == nil {
		return e.resetor.ResetWorkflowExecution(
			ctx,
			request,
			baseContext,
			baseMutableState,
			currentContext,
			currentMutableState,
		)
	}

	resetRunID := uuid.New()
	baseRebuildLastEventID := request.GetDecisionFinishEventId() - 1
	baseVersionHistories := baseMutableState.GetVersionHistories()
	baseCurrentVersionHistory, err := baseVersionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	baseRebuildLastEventVersion, err := baseCurrentVersionHistory.GetEventVersion(baseRebuildLastEventID)
	if err != nil {
		return nil, err
	}
	baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
	baseNextEventID := baseMutableState.GetNextEventID()

	if err := e.workflowResetter.resetWorkflow(
		ctx,
		domainID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		request.GetRequestId(),
		newNDCWorkflow(
			ctx,
			e.shard.GetDomainCache(),
			e.shard.GetClusterMetadata(),
			currentContext,
			currentMutableState,
			currentReleaseFn,
		),
		request.GetReason(),
		nil,
	); err != nil {
		return nil, err
	}
	return &workflow.ResetWorkflowExecutionResponse{
		RunId: common.StringPtr(resetRunID),
	}, nil
}

func (e *historyEngineImpl) updateWorkflow(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	action updateWorkflowActionFunc,
) (retError error) {

	workflowContext, err := e.loadWorkflow(ctx, domainID, execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		return err
	}
	defer func() { workflowContext.getReleaseFn()(retError) }()

	return e.updateWorkflowHelper(workflowContext, action)
}

func (e *historyEngineImpl) updateWorkflowExecutionWithAction(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	action updateWorkflowActionFunc,
) (retError error) {

	workflowContext, err := e.loadWorkflowOnce(ctx, domainID, execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		return err
	}
	defer func() { workflowContext.getReleaseFn()(retError) }()

	return e.updateWorkflowHelper(workflowContext, action)
}

func (e *historyEngineImpl) updateWorkflowHelper(
	workflowContext workflowContext,
	action updateWorkflowActionFunc,
) (retError error) {

UpdateHistoryLoop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		context := workflowContext.getContext()
		mutableState := workflowContext.getMutableState()

		// conduct caller action
		postActions, err := action(context, mutableState)
		if err != nil {
			if err == ErrStaleState {
				// Handler detected that cached workflow mutable could potentially be stale
				// Reload workflow execution history
				workflowContext.getContext().clear()
				if attempt != conditionalRetryCount-1 {
					_, err = workflowContext.reloadMutableState()
					if err != nil {
						return err
					}
				}
				continue UpdateHistoryLoop
			}

			// Returned error back to the caller
			return err
		}
		if postActions.noop {
			return nil
		}

		if postActions.createDecision {
			// Create a transfer task to schedule a decision task
			if !mutableState.HasPendingDecision() {
				_, err := mutableState.AddDecisionTaskScheduledEvent(false)
				if err != nil {
					return &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
				}
			}
		}

		err = workflowContext.getContext().updateWorkflowExecutionAsActive(e.shard.GetTimeSource().Now())
		if err == ErrConflict {
			if attempt != conditionalRetryCount-1 {
				_, err = workflowContext.reloadMutableState()
				if err != nil {
					return err
				}
			}
			continue UpdateHistoryLoop
		}
		return err
	}
	return ErrMaxAttemptsExceeded
}

// TODO: remove and use updateWorkflowExecutionWithAction
func (e *historyEngineImpl) updateWorkflowExecution(
	ctx ctx.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	createDecisionTask bool,
	action func(context workflowExecutionContext, mutableState mutableState) error,
) error {

	return e.updateWorkflowExecutionWithAction(
		ctx,
		domainID,
		execution,
		getUpdateWorkflowActionFunc(createDecisionTask, action),
	)
}

func getUpdateWorkflowActionFunc(
	createDecisionTask bool,
	action func(context workflowExecutionContext, mutableState mutableState) error,
) updateWorkflowActionFunc {

	return func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
		err := action(context, mutableState)
		if err != nil {
			return nil, err
		}
		postActions := &updateWorkflowAction{
			createDecision: createDecisionTask,
		}
		return postActions, nil
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
	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	if _, err = mutableState.AddDecisionTaskFailedEvent(
		scheduleID, startedID, cause, details, request.GetIdentity(), "", request.GetBinaryChecksum(), "", "", 0,
	); err != nil {
		return nil, err
	}

	// Return new builder back to the caller for further updates
	return mutableState, nil
}

func (e *historyEngineImpl) NotifyNewHistoryEvent(
	event *historyEventNotification,
) {

	e.historyEventNotifier.NotifyNewHistoryEvent(event)
}

func (e *historyEngineImpl) NotifyNewTransferTasks(
	tasks []persistence.Task,
) {

	if len(tasks) > 0 {
		task := tasks[0]
		clusterName := e.clusterMetadata.ClusterNameForFailoverVersion(task.GetVersion())
		e.txProcessor.NotifyNewTask(clusterName, tasks)
	}
}

func (e *historyEngineImpl) NotifyNewReplicationTasks(
	tasks []persistence.Task,
) {

	if len(tasks) > 0 {
		e.replicatorProcessor.notifyNewTask()
	}
}

func (e *historyEngineImpl) NotifyNewTimerTasks(
	tasks []persistence.Task,
) {

	if len(tasks) > 0 {
		task := tasks[0]
		clusterName := e.clusterMetadata.ClusterNameForFailoverVersion(task.GetVersion())
		e.timerProcessor.NotifyNewTimers(clusterName, tasks)
	}
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

func (e *historyEngineImpl) overrideStartWorkflowExecutionRequest(
	domainEntry *cache.DomainCacheEntry,
	request *workflow.StartWorkflowExecutionRequest,
	metricsScope int,
) {

	domainName := domainEntry.GetInfo().Name
	maxDecisionStartToCloseTimeoutSeconds := int32(e.config.MaxDecisionStartToCloseSeconds(domainName))

	taskStartToCloseTimeoutSecs := request.GetTaskStartToCloseTimeoutSeconds()
	taskStartToCloseTimeoutSecs = common.MinInt32(taskStartToCloseTimeoutSecs, maxDecisionStartToCloseTimeoutSeconds)
	taskStartToCloseTimeoutSecs = common.MinInt32(taskStartToCloseTimeoutSecs, request.GetExecutionStartToCloseTimeoutSeconds())

	if taskStartToCloseTimeoutSecs != request.GetTaskStartToCloseTimeoutSeconds() {
		request.TaskStartToCloseTimeoutSeconds = &taskStartToCloseTimeoutSecs
		e.metricsClient.Scope(
			metricsScope,
			metrics.DomainTag(domainName),
		).IncCounter(metrics.DecisionStartToCloseTimeoutOverrideCount)
	}
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
	mutableState mutableState,
) (int64, error) {

	if activityID == "" {
		return 0, &workflow.BadRequestError{Message: "Neither ActivityID nor ScheduleID is provided"}
	}
	activityInfo, ok := mutableState.GetActivityByActivityID(activityID)
	if !ok {
		return 0, &workflow.BadRequestError{Message: "Cannot locate Activity ScheduleID"}
	}
	return activityInfo.ScheduleID, nil
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

	return e.applyWorkflowIDReusePolicyHelper(
		prevStartRequestID,
		prevRunID,
		prevState,
		prevCloseState,
		domainID,
		execution,
		wfIDReusePolicy,
	)

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

func (e *historyEngineImpl) GetReplicationMessages(
	ctx ctx.Context,
	pollingCluster string,
	lastReadMessageID int64,
) (*r.ReplicationMessages, error) {

	scope := metrics.HistoryGetReplicationMessagesScope
	sw := e.metricsClient.StartTimer(scope, metrics.GetReplicationMessagesForShardLatency)
	defer sw.Stop()

	replicationMessages, err := e.replicatorProcessor.getTasks(
		ctx,
		pollingCluster,
		lastReadMessageID,
	)
	if err != nil {
		e.logger.Error("Failed to retrieve replication messages.", tag.Error(err))
		return nil, err
	}

	//Set cluster status for sync shard info
	replicationMessages.SyncShardStatus = &r.SyncShardStatus{
		Timestamp: common.Int64Ptr(e.timeSource.Now().UnixNano()),
	}
	e.logger.Debug("Successfully fetched replication messages.", tag.Counter(len(replicationMessages.ReplicationTasks)))
	return replicationMessages, nil
}

func (e *historyEngineImpl) GetDLQReplicationMessages(
	ctx ctx.Context,
	taskInfos []*r.ReplicationTaskInfo,
) ([]*r.ReplicationTask, error) {

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	sw := e.metricsClient.StartTimer(scope, metrics.GetDLQReplicationMessagesLatency)
	defer sw.Stop()

	tasks := make([]*r.ReplicationTask, len(taskInfos))
	for _, taskInfo := range taskInfos {
		task, err := e.replicatorProcessor.getTask(ctx, taskInfo)
		if err != nil {
			e.logger.Error("Failed to fetch DLQ replication messages.", tag.Error(err))
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (e *historyEngineImpl) ReapplyEvents(
	ctx ctx.Context,
	domainUUID string,
	workflowID string,
	runID string,
	reapplyEvents []*workflow.HistoryEvent,
) error {

	domainEntry, err := e.getActiveDomainEntry(common.StringPtr(domainUUID))
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID
	// remove run id from the execution so that reapply events to the current run
	currentExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
	}

	return e.updateWorkflowExecutionWithAction(
		ctx,
		domainID,
		currentExecution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {

			if !mutableState.IsWorkflowExecutionRunning() {
				// need to reset target workflow (which is also the current workflow)
				// to accept events to be reapplied
				baseRunID := mutableState.GetExecutionInfo().RunID
				resetRunID := uuid.New()
				baseRebuildLastEventID := mutableState.GetPreviousStartedEventID()

				// TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
				//  since cannot reapply event to a finished workflow which had no decisions started
				if baseRebuildLastEventID == common.EmptyEventID {
					e.logger.Warn("cannot reapply event to a finished workflow",
						tag.WorkflowDomainID(domainID),
						tag.WorkflowID(currentExecution.GetWorkflowId()),
					)
					e.metricsClient.IncCounter(metrics.HistoryReapplyEventsScope, metrics.EventReapplySkippedCount)
					return &updateWorkflowAction{noop: true}, nil
				}

				baseVersionHistories := mutableState.GetVersionHistories()
				baseCurrentVersionHistory, err := baseVersionHistories.GetCurrentVersionHistory()
				if err != nil {
					return nil, err
				}
				baseRebuildLastEventVersion, err := baseCurrentVersionHistory.GetEventVersion(baseRebuildLastEventID)
				if err != nil {
					return nil, err
				}
				baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
				baseNextEventID := mutableState.GetNextEventID()

				if err = e.workflowResetter.resetWorkflow(
					ctx,
					domainID,
					workflowID,
					baseRunID,
					baseCurrentBranchToken,
					baseRebuildLastEventID,
					baseRebuildLastEventVersion,
					baseNextEventID,
					resetRunID,
					uuid.New(),
					newNDCWorkflow(
						ctx,
						e.shard.GetDomainCache(),
						e.shard.GetClusterMetadata(),
						context,
						mutableState,
						noopReleaseFn,
					),
					eventsReapplicationResetWorkflowReason,
					reapplyEvents,
				); err != nil {
					return nil, err
				}
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			postActions := &updateWorkflowAction{
				createDecision: true,
			}
			// Do not create decision task when the workflow is cron and the cron has not been started yet
			if mutableState.GetExecutionInfo().CronSchedule != "" && !mutableState.HasProcessedOrPendingDecision() {
				postActions.createDecision = false
			}
			reappliedEvents, err := e.eventsReapplier.reapplyEvents(
				ctx,
				mutableState,
				reapplyEvents,
				runID,
			)
			if err != nil {
				e.logger.Error("failed to re-apply stale events", tag.Error(err))
				return nil, &workflow.InternalServiceError{Message: "unable to re-apply stale events"}
			}
			if len(reappliedEvents) == 0 {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}
			return postActions, nil
		})
}

func (e *historyEngineImpl) ReadDLQMessages(
	ctx context.Context,
	request *r.ReadDLQMessagesRequest,
) (*r.ReadDLQMessagesResponse, error) {

	tasks, token, err := e.replicationDLQHandler.readMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return &r.ReadDLQMessagesResponse{
		Type:             request.GetType().Ptr(),
		ReplicationTasks: tasks,
		NextPageToken:    token,
	}, nil
}

func (e *historyEngineImpl) PurgeDLQMessages(
	ctx context.Context,
	request *r.PurgeDLQMessagesRequest,
) error {

	return e.replicationDLQHandler.purgeMessages(
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
	)
}

func (e *historyEngineImpl) MergeDLQMessages(
	ctx context.Context,
	request *r.MergeDLQMessagesRequest,
) (*r.MergeDLQMessagesResponse, error) {

	token, err := e.replicationDLQHandler.mergeMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageID(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return &r.MergeDLQMessagesResponse{
		NextPageToken: token,
	}, nil
}

func (e *historyEngineImpl) RefreshWorkflowTasks(
	ctx ctx.Context,
	domainUUID string,
	execution workflow.WorkflowExecution,
) (retError error) {

	domainEntry, err := e.getActiveDomainEntry(common.StringPtr(domainUUID))
	if err != nil {
		return err
	}
	domainID := domainEntry.GetInfo().ID

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(ctx, domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	mutableStateTaskRefresher := newMutableStateTaskRefresher(
		e.shard.GetConfig(),
		e.shard.GetDomainCache(),
		e.shard.GetEventsCache(),
		e.shard.GetLogger(),
	)

	now := e.shard.GetTimeSource().Now()

	err = mutableStateTaskRefresher.refreshTasks(now, mutableState)
	if err != nil {
		return err
	}

	err = context.updateWorkflowExecutionAsActive(now)
	if err != nil {
		return err
	}
	return nil
}

func (e *historyEngineImpl) loadWorkflowOnce(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (workflowContext, error) {

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(
		ctx,
		domainID,
		workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	if err != nil {
		return nil, err
	}

	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		release(err)
		return nil, err
	}

	return newWorkflowContext(context, release, mutableState), nil
}

func (e *historyEngineImpl) loadWorkflow(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (workflowContext, error) {

	if runID != "" {
		return e.loadWorkflowOnce(ctx, domainID, workflowID, runID)
	}

	for attempt := 0; attempt < conditionalRetryCount; attempt++ {

		workflowContext, err := e.loadWorkflowOnce(ctx, domainID, workflowID, "")
		if err != nil {
			return nil, err
		}

		if workflowContext.getMutableState().IsWorkflowExecutionRunning() {
			return workflowContext, nil
		}

		// workflow not running, need to check current record
		resp, err := e.shard.GetExecutionManager().GetCurrentExecution(
			&persistence.GetCurrentExecutionRequest{
				DomainID:   domainID,
				WorkflowID: workflowID,
			},
		)
		if err != nil {
			workflowContext.getReleaseFn()(err)
			return nil, err
		}

		if resp.RunID == workflowContext.getRunID() {
			return workflowContext, nil
		}
		workflowContext.getReleaseFn()(nil)
	}

	return nil, &workflow.InternalServiceError{Message: "unable to locate current workflow execution"}
}
