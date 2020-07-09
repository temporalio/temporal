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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination historyEngine_mock.go

package history

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/config"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/worker/archiver"
)

const (
	conditionalRetryCount                     = 5
	activityCancellationMsgActivityIDUnknown  = "ACTIVITY_ID_UNKNOWN"
	activityCancellationMsgActivityNotStarted = "ACTIVITY_ID_NOT_STARTED"
	timerCancellationMsgTimerIDUnknown        = "TIMER_ID_UNKNOWN"
	defaultQueryFirstWorkflowTaskWaitTime     = time.Second
	queryFirstWorkflowTaskCheckInterval       = 200 * time.Millisecond
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx context.Context, request *historyservice.StartWorkflowExecutionRequest) (*historyservice.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *historyservice.GetMutableStateRequest) (*historyservice.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *historyservice.PollMutableStateRequest) (*historyservice.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *historyservice.DescribeMutableStateRequest) (*historyservice.DescribeMutableStateResponse, error)
		ResetStickyTaskQueue(ctx context.Context, resetRequest *historyservice.ResetStickyTaskQueueRequest) (*historyservice.ResetStickyTaskQueueResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *historyservice.DescribeWorkflowExecutionRequest) (*historyservice.DescribeWorkflowExecutionResponse, error)
		RecordWorkflowTaskStarted(ctx context.Context, request *historyservice.RecordWorkflowTaskStartedRequest) (*historyservice.RecordWorkflowTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx context.Context, request *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error)
		RespondWorkflowTaskCompleted(ctx context.Context, request *historyservice.RespondWorkflowTaskCompletedRequest) (*historyservice.RespondWorkflowTaskCompletedResponse, error)
		RespondWorkflowTaskFailed(ctx context.Context, request *historyservice.RespondWorkflowTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *historyservice.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx context.Context, request *historyservice.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx context.Context, request *historyservice.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx context.Context, request *historyservice.RecordActivityTaskHeartbeatRequest) (*historyservice.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *historyservice.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx context.Context, request *historyservice.SignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx context.Context, request *historyservice.SignalWithStartWorkflowExecutionRequest) (*historyservice.SignalWithStartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *historyservice.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *historyservice.TerminateWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx context.Context, request *historyservice.ResetWorkflowExecutionRequest) (*historyservice.ResetWorkflowExecutionResponse, error)
		ScheduleWorkflowTask(ctx context.Context, request *historyservice.ScheduleWorkflowTaskRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *historyservice.RecordChildExecutionCompletedRequest) error
		ReplicateEventsV2(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error
		SyncShardStatus(ctx context.Context, request *historyservice.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *historyservice.SyncActivityRequest) error
		GetReplicationMessages(ctx context.Context, pollingCluster string, lastReadMessageID int64) (*replicationspb.ReplicationMessages, error)
		GetDLQReplicationMessages(ctx context.Context, taskInfos []*replicationspb.ReplicationTaskInfo) ([]*replicationspb.ReplicationTask, error)
		QueryWorkflow(ctx context.Context, request *historyservice.QueryWorkflowRequest) (*historyservice.QueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, namespaceUUID string, workflowID string, runID string, events []*historypb.HistoryEvent) error
		GetDLQMessages(ctx context.Context, messagesRequest *historyservice.GetDLQMessagesRequest) (*historyservice.GetDLQMessagesResponse, error)
		PurgeDLQMessages(ctx context.Context, messagesRequest *historyservice.PurgeDLQMessagesRequest) error
		MergeDLQMessages(ctx context.Context, messagesRequest *historyservice.MergeDLQMessagesRequest) (*historyservice.MergeDLQMessagesResponse, error)
		RefreshWorkflowTasks(ctx context.Context, namespaceUUID string, execution commonpb.WorkflowExecution) error

		NotifyNewHistoryEvent(event *historyEventNotification)
		NotifyNewTransferTasks(tasks []persistence.Task)
		NotifyNewReplicationTasks(tasks []persistence.Task)
		NotifyNewTimerTasks(tasks []persistence.Task)
	}

	historyEngineImpl struct {
		currentClusterName        string
		shard                     ShardContext
		timeSource                clock.TimeSource
		workflowTaskHandler       workflowTaskHandlerCallbacks
		clusterMetadata           cluster.Metadata
		historyV2Mgr              persistence.HistoryManager
		executionManager          persistence.ExecutionManager
		visibilityMgr             persistence.VisibilityManager
		txProcessor               transferQueueProcessor
		timerProcessor            timerQueueProcessor
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
		archivalClient            archiver.Client
		workflowResetter          workflowResetter
		queueTaskProcessor        queueTaskProcessor
		replicationTaskProcessors []ReplicationTaskProcessor
		publicClient              sdkclient.Client
		eventsReapplier           nDCEventsReapplier
		matchingClient            matching.Client
		rawMatchingClient         matching.Client
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
	ErrActivityTaskNotFound = serviceerror.NewNotFound("invalid activityID or activity already timed out or invoking workflow is completed")
	// ErrWorkflowCompleted is the error to indicate workflow execution already completed
	ErrWorkflowCompleted = serviceerror.NewNotFound("workflow execution already completed")
	// ErrWorkflowExecutionNotFound is the error to indicate workflow execution does not exist
	ErrWorkflowExecutionNotFound = serviceerror.NewNotFound("workflow execution not found")
	// ErrWorkflowParent is the error to parent execution is given and mismatch
	ErrWorkflowParent = serviceerror.NewNotFound("workflow parent does not match")
	// ErrDeserializingToken is the error to indicate task token is invalid
	ErrDeserializingToken = serviceerror.NewInvalidArgument("error deserializing task token")
	// ErrCancellationAlreadyRequested is the error indicating cancellation for target workflow is already requested
	ErrCancellationAlreadyRequested = serviceerror.NewCancellationAlreadyRequested("cancellation already requested for this workflow execution")
	// ErrSignalsLimitExceeded is the error indicating limit reached for maximum number of signal events
	ErrSignalsLimitExceeded = serviceerror.NewResourceExhausted("exceeded workflow execution limit for signal events")
	// ErrEventsAterWorkflowFinish is the error indicating server error trying to write events after workflow finish event
	ErrEventsAterWorkflowFinish = serviceerror.NewInternal("error validating last event being workflow finish event")
	// ErrQueryEnteredInvalidState is error indicating query entered invalid state
	ErrQueryEnteredInvalidState = serviceerror.NewInvalidArgument("query entered invalid state, this should be impossible")
	// ErrConsistentQueryBufferExceeded is error indicating that too many consistent queries have been buffered and until buffered queries are finished new consistent queries cannot be buffered
	ErrConsistentQueryBufferExceeded = serviceerror.NewInternal("consistent query buffer is full, cannot accept new consistent queries")
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = serviceerror.NewInvalidArgument("encounter empty history batch")
	// ErrSizeExceedsLimit is error indicating workflow execution has exceeded system defined limit
	ErrSizeExceedsLimit = serviceerror.NewResourceExhausted(common.FailureReasonSizeExceedsLimit)

	// FailedWorkflowStatuses is a set of failed workflow close states, used for start workflow policy
	// for start workflow execution API
	FailedWorkflowStatuses = map[enumspb.WorkflowExecutionStatus]bool{
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:     true,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:   true,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED: true,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:  true,
	}
)

// NewEngineWithShardContext creates an instance of history engine
func NewEngineWithShardContext(
	shard ShardContext,
	visibilityMgr persistence.VisibilityManager,
	matching matching.Client,
	historyClient history.Client,
	publicClient sdkclient.Client,
	historyEventNotifier historyEventNotifier,
	publisher messaging.Producer,
	config *Config,
	replicationTaskFetchers ReplicationTaskFetchers,
	rawMatchingClient matching.Client,
	queueTaskProcessor queueTaskProcessor,
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
		tokenSerializer:      common.NewProtoTaskTokenSerializer(),
		historyCache:         historyCache,
		logger:               logger.WithTags(tag.ComponentHistoryEngine),
		throttledLogger:      shard.GetThrottledLogger().WithTags(tag.ComponentHistoryEngine),
		metricsClient:        shard.GetMetricsClient(),
		historyEventNotifier: historyEventNotifier,
		config:               config,
		archivalClient: archiver.NewClient(
			shard.GetMetricsClient(),
			logger,
			publicClient,
			shard.GetConfig().NumArchiveSystemWorkflows,
			shard.GetConfig().ArchiveRequestRPS,
			shard.GetService().GetArchiverProvider(),
		),
		publicClient:       publicClient,
		matchingClient:     matching,
		rawMatchingClient:  rawMatchingClient,
		queueTaskProcessor: queueTaskProcessor,
	}

	historyEngImpl.txProcessor = newTransferQueueProcessor(shard, historyEngImpl, visibilityMgr, matching, historyClient, queueTaskProcessor, logger)
	historyEngImpl.timerProcessor = newTimerQueueProcessor(shard, historyEngImpl, matching, queueTaskProcessor, logger)
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
	historyEngImpl.workflowResetter = newWorkflowResetter(
		shard,
		historyCache,
		logger,
	)
	historyEngImpl.workflowTaskHandler = newWorkflowTaskHandlerCallback(historyEngImpl)

	var replicationTaskProcessors []ReplicationTaskProcessor
	replicationTaskExecutors := make(map[string]replicationTaskExecutor)
	for _, replicationTaskFetcher := range replicationTaskFetchers.GetFetchers() {
		sourceCluster := replicationTaskFetcher.GetSourceCluster()
		// Intentionally use the raw client to create its own retry policy
		adminClient := shard.GetService().GetClientBean().GetRemoteAdminClient(sourceCluster)
		adminRetryableClient := admin.NewRetryableClient(
			adminClient,
			common.CreateReplicationServiceBusyRetryPolicy(),
			common.IsResourceExhausted,
		)
		// Intentionally use the raw client to create its own retry policy
		historyClient := shard.GetService().GetClientBean().GetHistoryClient()
		historyRetryableClient := history.NewRetryableClient(
			historyClient,
			common.CreateReplicationServiceBusyRetryPolicy(),
			common.IsResourceExhausted,
		)
		nDCHistoryResender := xdc.NewNDCHistoryResender(
			shard.GetNamespaceCache(),
			adminRetryableClient,
			func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
				_, err := historyRetryableClient.ReplicateEventsV2(ctx, request)
				return err
			},
			shard.GetService().GetPayloadSerializer(),
			shard.GetConfig().StandbyTaskReReplicationContextTimeout,
			shard.GetLogger(),
		)
		replicationTaskExecutor := newReplicationTaskExecutor(
			sourceCluster,
			shard,
			shard.GetNamespaceCache(),
			nDCHistoryResender,
			historyEngImpl,
			shard.GetMetricsClient(),
			shard.GetLogger(),
		)
		replicationTaskExecutors[sourceCluster] = replicationTaskExecutor

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
	replicationMessageHandler := newReplicationDLQHandler(shard, replicationTaskExecutors)
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

	e.txProcessor.Start()
	e.timerProcessor.Start()

	// failover callback will try to create a failover queue processor to scan all inflight tasks
	// if domain needs to be failovered. However, in the multicursor queue logic, the scan range
	// can't be retrieved before the processor is started. If failover callback is registered
	// before queue processor is started, it may result in a deadline as to create the failover queue,
	// queue processor need to be started.
	e.registerNamespaceFailoverCallback()

	clusterMetadata := e.shard.GetClusterMetadata()
	if e.replicatorProcessor != nil &&
		(clusterMetadata.GetReplicationConsumerConfig().Type != config.ReplicationConsumerTypeRPC ||
			e.config.EnableKafkaReplication()) {
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

	if e.queueTaskProcessor != nil {
		e.queueTaskProcessor.StopShardProcessor(e.shard)
	}

	// unset the failover callback
	e.shard.GetNamespaceCache().UnregisterNamespaceChangeCallback(e.shard.GetShardID())
}

func (e *historyEngineImpl) registerNamespaceFailoverCallback() {

	// NOTE: READ BEFORE MODIFICATION
	//
	// Tasks, e.g. transfer tasks and timer tasks, are created when holding the shard lock
	// meaning tasks -> release of shard lock
	//
	// Namespace change notification follows the following steps, order matters
	// 1. lock all task processing.
	// 2. namespace changes visible to everyone (Note: lock of task processing prevents task processing logic seeing the namespace changes).
	// 3. failover min and max task levels are calaulated, then update to shard.
	// 4. failover start & task processing unlock & shard namespace version notification update. (order does not matter for this discussion)
	//
	// The above guarantees that task created during the failover will be processed.
	// If the task is created after namespace change:
	// 		then active processor will handle it. (simple case)
	// If the task is created before namespace change:
	//		task -> release of shard lock
	//		failover min / max task levels calculated & updated to shard (using shard lock) -> failover start
	// above 2 guarantees that failover start is after persistence of the task.

	failoverPredicate := func(shardNotificationVersion int64, nextNamespace *cache.NamespaceCacheEntry, action func()) {
		namespaceFailoverNotificationVersion := nextNamespace.GetFailoverNotificationVersion()
		namespaceActiveCluster := nextNamespace.GetReplicationConfig().ActiveClusterName

		if nextNamespace.IsGlobalNamespace() &&
			namespaceFailoverNotificationVersion >= shardNotificationVersion &&
			namespaceActiveCluster == e.currentClusterName {
			action()
		}
	}

	// first set the failover callback
	e.shard.GetNamespaceCache().RegisterNamespaceChangeCallback(
		e.shard.GetShardID(),
		e.shard.GetNamespaceNotificationVersion(),
		func() {
			e.txProcessor.LockTaskProcessing()
			e.timerProcessor.LockTaskProcessing()
		},
		func(prevNamespaces []*cache.NamespaceCacheEntry, nextNamespaces []*cache.NamespaceCacheEntry) {
			defer func() {
				e.txProcessor.UnlockTaskPrrocessing()
				e.timerProcessor.UnlockTaskProcessing()
			}()

			if len(nextNamespaces) == 0 {
				return
			}

			shardNotificationVersion := e.shard.GetNamespaceNotificationVersion()
			failoverNamespaceIDs := map[string]struct{}{}

			for _, nextNamespace := range nextNamespaces {
				failoverPredicate(shardNotificationVersion, nextNamespace, func() {
					failoverNamespaceIDs[nextNamespace.GetInfo().Id] = struct{}{}
				})
			}

			if len(failoverNamespaceIDs) > 0 {
				e.logger.Info("Namespace Failover Start.", tag.WorkflowNamespaceIDs(failoverNamespaceIDs))

				e.txProcessor.FailoverNamespace(failoverNamespaceIDs)
				e.timerProcessor.FailoverNamespace(failoverNamespaceIDs)

				now := e.shard.GetTimeSource().Now()
				// the fake tasks will not be actually used, we just need to make sure
				// its length > 0 and has correct timestamp, to trigger a db scan
				fakeWorkflowTask := []persistence.Task{&persistence.WorkflowTask{}}
				fakeWorkflowTaskTimeoutTask := []persistence.Task{&persistence.WorkflowTaskTimeoutTask{VisibilityTimestamp: now}}
				e.txProcessor.NotifyNewTask(e.currentClusterName, fakeWorkflowTask)
				e.timerProcessor.NotifyNewTimers(e.currentClusterName, fakeWorkflowTaskTimeoutTask)
			}

			// nolint:errcheck
			e.shard.UpdateNamespaceNotificationVersion(nextNamespaces[len(nextNamespaces)-1].GetNotificationVersion() + 1)
		},
	)
}

func (e *historyEngineImpl) createMutableState(
	clusterMetadata cluster.Metadata,
	namespaceEntry *cache.NamespaceCacheEntry,
	runID string,
) (mutableState, error) {

	var newMutableState mutableState
	// version history applies to both local and global namespace
	newMutableState = newMutableStateBuilderWithVersionHistories(
		e.shard,
		e.shard.GetEventsCache(),
		e.logger,
		namespaceEntry,
	)

	if err := newMutableState.SetHistoryTree(runID); err != nil {
		return nil, err
	}

	return newMutableState, nil
}

func (e *historyEngineImpl) generateFirstWorkflowTask(
	mutableState mutableState,
	parentInfo *workflowspb.ParentExecutionInfo,
	startEvent *historypb.HistoryEvent,
) error {

	if parentInfo == nil {
		// WorkflowTask is only created when it is not a Child Workflow and no backoff is needed
		if err := mutableState.AddFirstWorkflowTaskScheduled(
			startEvent,
		); err != nil {
			return err
		}
	}
	return nil
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(
	ctx context.Context,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (resp *historyservice.StartWorkflowExecutionResponse, retError error) {

	namespaceEntry, err := e.getActiveNamespaceEntry(startRequest.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := startRequest.StartRequest
	err = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	e.overrideStartWorkflowExecutionRequest(namespaceEntry, request, metrics.HistoryStartWorkflowExecutionScope)

	workflowID := request.GetWorkflowId()
	// grab the current context as a lock, nothing more
	_, currentRelease, err := e.historyCache.getOrCreateCurrentWorkflowExecution(
		ctx,
		namespaceID,
		workflowID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { currentRelease(retError) }()

	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}
	clusterMetadata := e.shard.GetService().GetClusterMetadata()
	mutableState, err := e.createMutableState(clusterMetadata, namespaceEntry, execution.GetRunId())
	if err != nil {
		return nil, err
	}

	startEvent, err := mutableState.AddWorkflowExecutionStartedEvent(
		execution,
		startRequest,
	)
	if err != nil {
		return nil, serviceerror.NewInternal("Failed to add workflow execution started event.")
	}

	// Generate first workflow task event if not child WF and no first workflow task backoff
	if err := e.generateFirstWorkflowTask(
		mutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, err
	}

	weContext := newWorkflowExecutionContext(namespaceID, execution, e.shard, e.executionManager, e.logger)

	now := e.timeSource.Now()
	newWorkflow, newWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		now,
		transactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	historySize, err := weContext.persistFirstWorkflowEvents(newWorkflowEventsSeq[0])
	if err != nil {
		return nil, err
	}

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = weContext.createWorkflowExecution(
		newWorkflow, historySize, now,
		createMode, prevRunID, prevLastWriteVersion,
	)
	if err != nil {
		if t, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			if t.StartRequestID == request.GetRequestId() {
				return &historyservice.StartWorkflowExecutionResponse{
					RunId: t.RunID,
				}, nil
				// delete history is expected here because duplicate start request will create history with different rid
			}

			if mutableState.GetCurrentVersion() < t.LastWriteVersion {
				return nil, serviceerror.NewNamespaceNotActive(
					request.GetNamespace(),
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
				t.Status,
				namespaceID,
				execution,
				startRequest.StartRequest.GetWorkflowIdReusePolicy(),
			); err != nil {
				return nil, err
			}
			err = weContext.createWorkflowExecution(
				newWorkflow, historySize, now,
				createMode, prevRunID, prevLastWriteVersion,
			)
		}
	}

	if err != nil {
		return nil, err
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId: execution.GetRunId(),
	}, nil
}

// GetMutableState retrieves the mutable state of the workflow execution
func (e *historyEngineImpl) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
) (*historyservice.GetMutableStateResponse, error) {

	return e.getMutableStateOrPolling(ctx, request)
}

// PollMutableState retrieves the mutable state of the workflow execution with long polling
func (e *historyEngineImpl) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
) (*historyservice.PollMutableStateResponse, error) {

	response, err := e.getMutableStateOrPolling(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId:         request.GetNamespaceId(),
		Execution:           request.Execution,
		ExpectedNextEventId: request.ExpectedNextEventId,
		CurrentBranchToken:  request.CurrentBranchToken})

	if err != nil {
		return nil, e.updateEntityNotExistsErrorOnPassiveCluster(err, request.GetNamespaceId())
	}
	return &historyservice.PollMutableStateResponse{
		Execution:                             response.Execution,
		WorkflowType:                          response.WorkflowType,
		NextEventId:                           response.NextEventId,
		PreviousStartedEventId:                response.PreviousStartedEventId,
		LastFirstEventId:                      response.LastFirstEventId,
		TaskQueue:                             response.TaskQueue,
		StickyTaskQueue:                       response.StickyTaskQueue,
		StickyTaskQueueScheduleToStartTimeout: response.StickyTaskQueueScheduleToStartTimeout,
		CurrentBranchToken:                    response.CurrentBranchToken,
		VersionHistories:                      response.VersionHistories,
		WorkflowState:                         response.WorkflowState,
		WorkflowStatus:                        response.WorkflowStatus,
	}, nil
}

func (e *historyEngineImpl) updateEntityNotExistsErrorOnPassiveCluster(err error, namespaceID string) error {
	switch err.(type) {
	case *serviceerror.NotFound:
		namespaceCache, namespaceCacheErr := e.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
		if namespaceCacheErr != nil {
			return err // if could not access namespace cache simply return original error
		}

		if namespaceNotActiveErr, ok := namespaceCache.GetNamespaceNotActiveErr().(*serviceerror.NamespaceNotActive); ok && namespaceNotActiveErr != nil {
			updatedErr := serviceerror.NewNotFound("Workflow execution not found in non-active cluster")
			updatedErr.ActiveCluster = namespaceNotActiveErr.ActiveCluster
			updatedErr.CurrentCluster = namespaceNotActiveErr.CurrentCluster

			return updatedErr
		}
	}
	return err
}

func (e *historyEngineImpl) getMutableStateOrPolling(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
) (*historyservice.GetMutableStateResponse, error) {

	namespaceID, err := validateNamespaceUUID(request.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	execution := commonpb.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}
	response, err := e.getMutableState(ctx, namespaceID, execution)
	if err != nil {
		return nil, err
	}
	if request.CurrentBranchToken == nil {
		request.CurrentBranchToken = response.CurrentBranchToken
	}
	if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
		return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
	}
	// set the run id in case query the current running workflow
	execution.RunId = response.Execution.RunId

	// expectedNextEventID is 0 when caller want to get the current next event ID without blocking
	expectedNextEventID := common.FirstEventID
	if request.ExpectedNextEventId != common.EmptyEventID {
		expectedNextEventID = request.GetExpectedNextEventId()
	}

	// if caller decide to long poll on workflow execution
	// and the event ID we are looking for is smaller than current next event ID
	if expectedNextEventID >= response.GetNextEventId() && response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		subscriberID, channel, err := e.historyEventNotifier.WatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()))
		if err != nil {
			return nil, err
		}
		defer e.historyEventNotifier.UnwatchHistoryEvent(definition.NewWorkflowIdentifier(namespaceID, execution.GetWorkflowId(), execution.GetRunId()), subscriberID) // nolint:errcheck
		// check again in case the next event ID is updated
		response, err = e.getMutableState(ctx, namespaceID, execution)
		if err != nil {
			return nil, err
		}
		// check again if the current branch token changed
		if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
			return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
		}
		if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return response, nil
		}

		namespaceCache, err := e.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(e.shard.GetConfig().LongPollExpirationInterval(namespaceCache.GetInfo().Name))
		defer timer.Stop()
		for {
			select {
			case event := <-channel:
				response.LastFirstEventId = event.lastFirstEventID
				response.NextEventId = event.nextEventID
				response.PreviousStartedEventId = event.previousStartedEventID
				response.WorkflowState = event.workflowState
				response.WorkflowStatus = event.workflowStatus
				if !bytes.Equal(request.CurrentBranchToken, event.currentBranchToken) {
					return nil, serviceerrors.NewCurrentBranchChanged(event.currentBranchToken, request.CurrentBranchToken)
				}
				if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
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
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
) (retResp *historyservice.QueryWorkflowResponse, retErr error) {

	scope := e.metricsClient.Scope(metrics.HistoryQueryWorkflowScope)

	mutableStateResp, err := e.getMutableState(ctx, request.GetNamespaceId(), *request.GetRequest().GetExecution())
	if err != nil {
		return nil, err
	}
	req := request.GetRequest()
	if mutableStateResp.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && req.QueryRejectCondition != enumspb.QUERY_REJECT_CONDITION_NONE {
		notOpenReject := req.GetQueryRejectCondition() == enumspb.QUERY_REJECT_CONDITION_NOT_OPEN
		status := mutableStateResp.GetWorkflowStatus()
		notCompletedCleanlyReject := req.GetQueryRejectCondition() == enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY && status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		if notOpenReject || notCompletedCleanlyReject {
			return &historyservice.QueryWorkflowResponse{
				Response: &workflowservice.QueryWorkflowResponse{
					QueryRejected: &querypb.QueryRejected{
						Status: status,
					},
				},
			}, nil
		}
	}

	de, err := e.shard.GetNamespaceCache().GetNamespaceByID(request.GetNamespaceId())
	if err != nil {
		return nil, err
	}

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(ctx, request.GetNamespaceId(), *request.GetRequest().GetExecution())
	if err != nil {
		return nil, err
	}
	defer func() { release(retErr) }()
	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	// There are two ways in which queries get dispatched to workflow worker. First, queries can be dispatched on workflow tasks.
	// These workflow tasks potentially contain new events and queries. The events are treated as coming before the query in time.
	// The second way in which queries are dispatched to workflow worker is directly through matching; in this approach queries can be
	// dispatched to workflow worker immediately even if there are outstanding events that came before the query. The following logic
	// is used to determine if a query can be safely dispatched directly through matching or must be dispatched on a workflow task.
	//
	// There are three cases in which a query can be dispatched directly through matching safely, without violating strong consistency level:
	// 1. the namespace is not active, in this case history is immutable so a query dispatched at any time is consistent
	// 2. the workflow is not running, whenever a workflow is not running dispatching query directly is consistent
	// 3. if there is no pending or started workflow tasks it means no events came before query arrived, so its safe to dispatch directly
	safeToDispatchDirectly := !de.IsNamespaceActive() ||
		!mutableState.IsWorkflowExecutionRunning() ||
		(!mutableState.HasPendingWorkflowTask() && !mutableState.HasInFlightWorkflowTask())
	if safeToDispatchDirectly {
		release(nil)
		msResp, err := e.getMutableState(ctx, request.GetNamespaceId(), *request.GetRequest().GetExecution())
		if err != nil {
			return nil, err
		}
		req.Execution.RunId = msResp.Execution.RunId
		return e.queryDirectlyThroughMatching(ctx, msResp, request.GetNamespaceId(), req, scope)
	}

	// If we get here it means query could not be dispatched through matching directly, so it must block
	// until either an result has been obtained on a workflow task response or until it is safe to dispatch directly through matching.
	sw := scope.StartTimer(metrics.WorkflowTaskQueryLatency)
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
			case enumspb.QUERY_RESULT_TYPE_ANSWERED:
				return &historyservice.QueryWorkflowResponse{
					Response: &workflowservice.QueryWorkflowResponse{
						QueryResult: result.GetAnswer(),
					},
				}, nil
			case enumspb.QUERY_RESULT_TYPE_FAILED:
				return nil, serviceerror.NewQueryFailed(result.GetErrorMessage())
			default:
				scope.IncCounter(metrics.QueryRegistryInvalidStateCount)
				return nil, ErrQueryEnteredInvalidState
			}
		case queryTerminationTypeUnblocked:
			msResp, err := e.getMutableState(ctx, request.GetNamespaceId(), *request.GetRequest().GetExecution())
			if err != nil {
				return nil, err
			}
			req.Execution.RunId = msResp.Execution.RunId
			return e.queryDirectlyThroughMatching(ctx, msResp, request.GetNamespaceId(), req, scope)
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
	ctx context.Context,
	msResp *historyservice.GetMutableStateResponse,
	namespaceID string,
	queryRequest *workflowservice.QueryWorkflowRequest,
	scope metrics.Scope,
) (*historyservice.QueryWorkflowResponse, error) {

	sw := scope.StartTimer(metrics.DirectQueryDispatchLatency)
	defer sw.Stop()

	if msResp.GetIsStickyTaskQueueEnabled() &&
		len(msResp.GetStickyTaskQueue().GetName()) != 0 &&
		e.config.EnableStickyQuery(queryRequest.GetNamespace()) {

		stickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
			NamespaceId:  namespaceID,
			QueryRequest: queryRequest,
			TaskQueue:    msResp.GetStickyTaskQueue(),
		}

		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickiness
		stickyContext, cancel := context.WithTimeout(context.Background(), timestamp.DurationValue(msResp.GetStickyTaskQueueScheduleToStartTimeout()))
		stickyStopWatch := scope.StartTimer(metrics.DirectQueryDispatchStickyLatency)
		matchingResp, err := e.rawMatchingClient.QueryWorkflow(stickyContext, stickyMatchingRequest)
		stickyStopWatch.Stop()
		cancel()
		if err == nil {
			scope.IncCounter(metrics.DirectQueryDispatchStickySuccessCount)
			return &historyservice.QueryWorkflowResponse{
				Response: &workflowservice.QueryWorkflowResponse{
					QueryResult:   matchingResp.GetQueryResult(),
					QueryRejected: matchingResp.GetQueryRejected(),
				}}, nil
		}
		if _, ok := err.(*serviceerror.DeadlineExceeded); !ok {
			e.logger.Error("query directly though matching on sticky failed, will not attempt query on non-sticky",
				tag.WorkflowNamespace(queryRequest.GetNamespace()),
				tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
				tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
				tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
				tag.Error(err))
			return nil, err
		}
		if msResp.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			e.logger.Info("query direct through matching failed on sticky, clearing sticky before attempting on non-sticky",
				tag.WorkflowNamespace(queryRequest.GetNamespace()),
				tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
				tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
				tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
			resetContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			clearStickinessStopWatch := scope.StartTimer(metrics.DirectQueryDispatchClearStickinessLatency)
			_, err := e.ResetStickyTaskQueue(resetContext, &historyservice.ResetStickyTaskQueueRequest{
				NamespaceId: namespaceID,
				Execution:   queryRequest.GetExecution(),
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
		e.logger.Info("query context timed out before query on non-sticky task queue could be attempted",
			tag.WorkflowNamespace(queryRequest.GetNamespace()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()))
		scope.IncCounter(metrics.DirectQueryDispatchTimeoutBeforeNonStickyCount)
		return nil, err
	}

	e.logger.Info("query directly through matching on sticky timed out, attempting to query on non-sticky",
		tag.WorkflowNamespace(queryRequest.GetNamespace()),
		tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
		tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
		tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
		tag.WorkflowTaskQueueName(msResp.GetStickyTaskQueue().GetName()),
		tag.WorkflowNextEventID(msResp.GetNextEventId()))

	nonStickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
		NamespaceId:  namespaceID,
		QueryRequest: queryRequest,
		TaskQueue:    msResp.TaskQueue,
	}

	nonStickyStopWatch := scope.StartTimer(metrics.DirectQueryDispatchNonStickyLatency)
	matchingResp, err := e.matchingClient.QueryWorkflow(ctx, nonStickyMatchingRequest)
	nonStickyStopWatch.Stop()
	if err != nil {
		e.logger.Error("query directly though matching on non-sticky failed",
			tag.WorkflowNamespace(queryRequest.GetNamespace()),
			tag.WorkflowID(queryRequest.Execution.GetWorkflowId()),
			tag.WorkflowRunID(queryRequest.Execution.GetRunId()),
			tag.WorkflowQueryType(queryRequest.Query.GetQueryType()),
			tag.Error(err))
		return nil, err
	}
	scope.IncCounter(metrics.DirectQueryDispatchNonStickySuccessCount)
	return &historyservice.QueryWorkflowResponse{
		Response: &workflowservice.QueryWorkflowResponse{
			QueryResult:   matchingResp.GetQueryResult(),
			QueryRejected: matchingResp.GetQueryRejected(),
		}}, err
}

func (e *historyEngineImpl) getMutableState(
	ctx context.Context,
	namespaceID string,
	execution commonpb.WorkflowExecution,
) (retResp *historyservice.GetMutableStateResponse, retError error) {

	context, release, retError := e.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)
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
	workflowState, workflowStatus := mutableState.GetWorkflowStateStatus()
	retResp = &historyservice.GetMutableStateResponse{
		Execution:              &execution,
		WorkflowType:           &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
		LastFirstEventId:       mutableState.GetLastFirstEventID(),
		NextEventId:            mutableState.GetNextEventID(),
		PreviousStartedEventId: mutableState.GetPreviousStartedEventID(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: executionInfo.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		StickyTaskQueue: &taskqueuepb.TaskQueue{
			Name: executionInfo.StickyTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_STICKY,
		},
		StickyTaskQueueScheduleToStartTimeout: executionInfo.StickyScheduleToStartTimeout,
		CurrentBranchToken:                    currentBranchToken,
		WorkflowState:                         workflowState,
		WorkflowStatus:                        workflowStatus,
		IsStickyTaskQueueEnabled:              mutableState.IsStickyTaskQueueEnabled(),
	}
	versionHistories := mutableState.GetVersionHistories()
	if versionHistories != nil {
		retResp.VersionHistories = versionHistories.ToProto()
	}
	return
}

func (e *historyEngineImpl) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
) (response *historyservice.DescribeMutableStateResponse, retError error) {

	namespaceID, err := validateNamespaceUUID(request.GetNamespaceId())
	if err != nil {
		return nil, err
	}

	execution := commonpb.WorkflowExecution{
		WorkflowId: request.Execution.WorkflowId,
		RunId:      request.Execution.RunId,
	}

	cacheCtx, dbCtx, release, cacheHit, err := e.historyCache.getAndCreateWorkflowExecution(
		ctx, namespaceID, execution,
	)
	if err != nil {
		return nil, err
	}
	defer func() { release(retError) }()

	response = &historyservice.DescribeMutableStateResponse{}

	if cacheHit && cacheCtx.(*workflowExecutionContextImpl).mutableState != nil {
		msb := cacheCtx.(*workflowExecutionContextImpl).mutableState
		wms := msb.CopyToPersistence()
		response.CacheMutableState = workflowMutableStateToJSON(wms)
	}

	msb, err := dbCtx.loadWorkflowExecution()
	if err != nil {
		response.DatabaseMutableState = err.Error()
		return response, nil
	}
	wms := msb.CopyToPersistence()
	response.DatabaseMutableState = workflowMutableStateToJSON(wms)

	currentBranchToken := wms.ExecutionInfo.EventBranchToken
	if wms.VersionHistories != nil {
		// if VersionHistories is set, then all branch infos are stored in VersionHistories
		currentVersionHistory, err := wms.VersionHistories.GetCurrentVersionHistory()
		if err != nil {
			response.TreeId = err.Error()
			return response, nil
		}
		currentBranchToken = currentVersionHistory.GetBranchToken()
	}
	branchInfo := persistenceblobs.HistoryBranch{}
	err = branchInfo.Unmarshal(currentBranchToken)
	if err != nil {
		response.TreeId = err.Error()
		return response, nil
	}

	response.TreeId = branchInfo.TreeId
	response.BranchId = branchInfo.BranchId

	return response, nil
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (e *historyEngineImpl) ResetStickyTaskQueue(
	ctx context.Context,
	resetRequest *historyservice.ResetStickyTaskQueueRequest,
) (*historyservice.ResetStickyTaskQueueResponse, error) {

	namespaceID, err := validateNamespaceUUID(resetRequest.GetNamespaceId())
	if err != nil {
		return nil, err
	}

	err = e.updateWorkflowExecution(ctx, namespaceID, *resetRequest.Execution, false,
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
	return &historyservice.ResetStickyTaskQueueResponse{}, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (e *historyEngineImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
) (retResp *historyservice.DescribeWorkflowExecutionResponse, retError error) {

	namespaceID, err := validateNamespaceUUID(request.GetNamespaceId())
	if err != nil {
		return nil, err
	}

	execution := *request.Request.Execution

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)
	if err0 != nil {
		return nil, err0
	}
	defer func() { release(retError) }()

	mutableState, err1 := context.loadWorkflowExecution()
	if err1 != nil {
		return nil, err1
	}
	executionInfo := mutableState.GetExecutionInfo()
	result := &historyservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig: &workflowpb.WorkflowExecutionConfig{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: executionInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowExecutionTimeout:   executionInfo.WorkflowExecutionTimeout,
			WorkflowRunTimeout:         executionInfo.WorkflowRunTimeout,
			DefaultWorkflowTaskTimeout: executionInfo.DefaultWorkflowTaskTimeout,
		},
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.WorkflowId,
				RunId:      executionInfo.ExecutionState.RunId,
			},
			Type:             &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
			StartTime:        executionInfo.StartTime,
			HistoryLength:    mutableState.GetNextEventID() - common.FirstEventID,
			AutoResetPoints:  executionInfo.AutoResetPoints,
			Memo:             &commonpb.Memo{Fields: executionInfo.Memo},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: executionInfo.SearchAttributes},
			Status:           executionInfo.ExecutionState.Status,
		},
	}

	// TODO: we need to consider adding execution time to mutable state
	// For now execution time will be calculated based on start time and cron schedule/retry policy
	// each time DescribeWorkflowExecution is called.
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return nil, err
	}
	backoffDuration := timestamp.DurationValue(startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstWorkflowTaskBackoff())
	result.WorkflowExecutionInfo.ExecutionTime = timestamp.TimePtr(timestamp.TimeValue(result.WorkflowExecutionInfo.GetStartTime()).Add(backoffDuration))

	if executionInfo.ParentRunId != "" {
		result.WorkflowExecutionInfo.ParentExecution = &commonpb.WorkflowExecution{
			WorkflowId: executionInfo.ParentWorkflowId,
			RunId:      executionInfo.ParentRunId,
		}
		result.WorkflowExecutionInfo.ParentNamespaceId = executionInfo.ParentNamespaceId
	}
	if executionInfo.ExecutionState.State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// for closed workflow
		result.WorkflowExecutionInfo.Status = executionInfo.ExecutionState.Status
		completionEvent, err := mutableState.GetCompletionEvent()
		if err != nil {
			return nil, err
		}
		result.WorkflowExecutionInfo.CloseTime = completionEvent.GetEventTime()
	}

	if len(mutableState.GetPendingActivityInfos()) > 0 {
		for _, ai := range mutableState.GetPendingActivityInfos() {
			p := &workflowpb.PendingActivityInfo{
				ActivityId: ai.ActivityId,
			}
			if ai.CancelRequested {
				p.State = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
			} else if ai.StartedId != common.EmptyEventID {
				p.State = enumspb.PENDING_ACTIVITY_STATE_STARTED
			} else {
				p.State = enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
			}
			if !timestamp.TimeValue(ai.LastHeartbeatUpdateTime).IsZero() {
				p.LastHeartbeatTime = ai.LastHeartbeatUpdateTime
				p.HeartbeatDetails = ai.LastHeartbeatDetails
			}
			// TODO: move to mutable state instead of loading it from event
			scheduledEvent, err := mutableState.GetActivityScheduledEvent(ai.ScheduleId)
			if err != nil {
				return nil, err
			}
			p.ActivityType = scheduledEvent.GetActivityTaskScheduledEventAttributes().ActivityType
			if p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
				p.ScheduledTime = ai.ScheduledTime
			} else {
				p.LastStartedTime = ai.StartedTime
			}
			if ai.HasRetryPolicy {
				p.Attempt = int32(ai.Attempt)
				p.ExpirationTime = ai.RetryExpirationTime
				if ai.RetryMaximumAttempts != 0 {
					p.MaximumAttempts = ai.RetryMaximumAttempts
				}
				if ai.RetryLastFailure != nil {
					p.LastFailure = ai.RetryLastFailure
				}
				if ai.RetryLastWorkerIdentity != "" {
					p.LastWorkerIdentity = ai.RetryLastWorkerIdentity
				}
			} else {
				p.Attempt = 1
			}
			result.PendingActivities = append(result.PendingActivities, p)
		}
	}

	if len(mutableState.GetPendingChildExecutionInfos()) > 0 {
		for _, ch := range mutableState.GetPendingChildExecutionInfos() {
			p := &workflowpb.PendingChildExecutionInfo{
				WorkflowId:        ch.StartedWorkflowId,
				RunId:             ch.StartedRunId,
				WorkflowTypeName:  ch.WorkflowTypeName,
				InitiatedId:       ch.InitiatedId,
				ParentClosePolicy: ch.ParentClosePolicy,
			}
			result.PendingChildren = append(result.PendingChildren, p)
		}
	}

	return result, nil
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
) (*historyservice.RecordActivityTaskStartedResponse, error) {

	namespaceEntry, err := e.getActiveNamespaceEntry(request.GetNamespaceId())
	if err != nil {
		return nil, err
	}

	namespaceInfo := namespaceEntry.GetInfo()

	namespaceID := namespaceInfo.Id
	namespace := namespaceInfo.Name

	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	response := &historyservice.RecordActivityTaskStartedResponse{}
	err = e.updateWorkflowExecution(ctx, namespaceID, execution, false,
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
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK))
				return ErrActivityTaskNotFound
			}

			scheduledEvent, err := mutableState.GetActivityScheduledEvent(scheduleID)
			if err != nil {
				return err
			}
			response.ScheduledEvent = scheduledEvent
			response.CurrentAttemptScheduledTime = ai.ScheduledTime

			if ai.StartedId != common.EmptyEventID {
				// If activity is started as part of the current request scope then return a positive response
				if ai.RequestId == requestID {
					response.StartedTime = ai.StartedTime
					response.Attempt = ai.Attempt
					return nil
				}

				// Looks like ActivityTask already started as a result of another call.
				// It is OK to drop the task at this point.
				e.logger.Debug("Potentially duplicate task.", tag.TaskID(request.GetTaskId()), tag.WorkflowScheduleID(scheduleID), tag.TaskType(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK))
				return serviceerrors.NewTaskAlreadyStarted("Activity")
			}

			if _, err := mutableState.AddActivityTaskStartedEvent(
				ai, scheduleID, requestID, request.PollRequest.GetIdentity(),
			); err != nil {
				return err
			}

			response.StartedTime = ai.StartedTime
			response.Attempt = ai.Attempt
			response.HeartbeatDetails = ai.LastHeartbeatDetails

			response.WorkflowType = mutableState.GetWorkflowType()
			response.WorkflowNamespace = namespace

			return nil
		})

	if err != nil {
		return nil, err
	}

	return response, err
}

// ScheduleWorkflowTask schedules a workflow task if no outstanding workflow task found
func (e *historyEngineImpl) ScheduleWorkflowTask(
	ctx context.Context,
	req *historyservice.ScheduleWorkflowTaskRequest,
) error {
	return e.workflowTaskHandler.handleWorkflowTaskScheduled(ctx, req)
}

// RecordWorkflowTaskStarted starts a workflow task
func (e *historyEngineImpl) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	return e.workflowTaskHandler.handleWorkflowTaskStarted(ctx, request)
}

// RespondWorkflowTaskCompleted completes a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	return e.workflowTaskHandler.handleWorkflowTaskCompleted(ctx, req)
}

// RespondWorkflowTaskFailed fails a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
) error {
	return e.workflowTaskHandler.handleWorkflowTaskFailed(ctx, req)
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id
	namespace := namespaceEntry.GetInfo().Name

	request := req.CompleteRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	var activityStartedTime time.Time
	var taskQueue string
	err = e.updateWorkflowExecution(ctx, namespaceID, workflowExecution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.GetActivityId(), mutableState)
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

			if !isRunning || ai.StartedId == common.EmptyEventID ||
				(token.GetScheduleId() != common.EmptyEventID && token.ScheduleAttempt != ai.Attempt) {
				return ErrActivityTaskNotFound
			}

			if _, err := mutableState.AddActivityTaskCompletedEvent(scheduleID, ai.StartedId, request); err != nil {
				// Unable to add ActivityTaskCompleted event to history
				return serviceerror.NewInternal("Unable to add ActivityTaskCompleted event to history.")
			}
			activityStartedTime = *ai.StartedTime
			taskQueue = ai.TaskQueue
			return nil
		})
	if err == nil && !activityStartedTime.IsZero() {
		scope := e.metricsClient.Scope(metrics.HistoryRespondActivityTaskCompletedScope).
			Tagged(
				metrics.NamespaceTag(namespace),
				metrics.WorkflowTypeTag(token.WorkflowType),
				metrics.ActivityTypeTag(token.ActivityType),
				metrics.TaskQueueTag(taskQueue),
			)
		scope.RecordTimer(metrics.ActivityE2ELatency, time.Since(activityStartedTime))
	}
	return err
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(
	ctx context.Context,
	req *historyservice.RespondActivityTaskFailedRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id
	namespace := namespaceEntry.GetInfo().Name

	request := req.FailedRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	var activityStartedTime time.Time
	var taskQueue string
	err = e.updateWorkflowExecutionWithAction(ctx, namespaceID, workflowExecution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.GetActivityId(), mutableState)
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

			if !isRunning || ai.StartedId == common.EmptyEventID ||
				(token.GetScheduleId() != common.EmptyEventID && token.ScheduleAttempt != ai.Attempt) {
				return nil, ErrActivityTaskNotFound
			}

			postActions := &updateWorkflowAction{}
			failure := request.GetFailure()
			retryState, err := mutableState.RetryActivity(ai, failure)
			if err != nil {
				return nil, err
			}
			if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
				// no more retry, and we want to record the failure event
				if _, err := mutableState.AddActivityTaskFailedEvent(scheduleID, ai.StartedId, failure, retryState, request.GetIdentity()); err != nil {
					// Unable to add ActivityTaskFailed event to history
					return nil, serviceerror.NewInternal("Unable to add ActivityTaskFailed event to history.")
				}
				postActions.createWorkflowTask = true
			}

			activityStartedTime = *ai.StartedTime
			taskQueue = ai.TaskQueue
			return postActions, nil
		})
	if err == nil && !activityStartedTime.IsZero() {
		scope := e.metricsClient.Scope(metrics.HistoryRespondActivityTaskFailedScope).
			Tagged(
				metrics.NamespaceTag(namespace),
				metrics.WorkflowTypeTag(token.WorkflowType),
				metrics.ActivityTypeTag(token.ActivityType),
				metrics.TaskQueueTag(taskQueue),
			)
		scope.RecordTimer(metrics.ActivityE2ELatency, time.Since(activityStartedTime))
	}
	return err
}

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCanceledRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id
	namespace := namespaceEntry.GetInfo().Name

	request := req.CancelRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	var activityStartedTime time.Time
	var taskQueue string
	err = e.updateWorkflowExecution(ctx, namespaceID, workflowExecution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			if scheduleID == common.EmptyEventID { // client call CompleteActivityById, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.GetActivityId(), mutableState)
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

			if !isRunning || ai.StartedId == common.EmptyEventID ||
				(token.GetScheduleId() != common.EmptyEventID && token.ScheduleAttempt != ai.Attempt) {
				return ErrActivityTaskNotFound
			}

			if _, err := mutableState.AddActivityTaskCanceledEvent(
				scheduleID,
				ai.StartedId,
				ai.CancelRequestId,
				request.Details,
				request.Identity); err != nil {
				// Unable to add ActivityTaskCanceled event to history
				return serviceerror.NewInternal("Unable to add ActivityTaskCanceled event to history.")
			}

			activityStartedTime = *ai.StartedTime
			taskQueue = ai.TaskQueue
			return nil
		})
	if err == nil && !activityStartedTime.IsZero() {
		scope := e.metricsClient.Scope(metrics.HistoryClientRespondActivityTaskCanceledScope).
			Tagged(
				metrics.NamespaceTag(namespace),
				metrics.WorkflowTypeTag(token.WorkflowType),
				metrics.ActivityTypeTag(token.ActivityType),
				metrics.TaskQueueTag(taskQueue),
			)
		scope.RecordTimer(metrics.ActivityE2ELatency, time.Since(activityStartedTime))
	}
	return err
}

// RecordActivityTaskHeartbeat records an hearbeat for a task.
// This method can be used for two purposes.
// - For reporting liveness of the activity.
// - For reporting progress of the activity, this can be done even if the liveness is not configured.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *historyservice.RecordActivityTaskHeartbeatRequest,
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {

	namespaceEntry, err := e.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := req.HeartbeatRequest
	token, err0 := e.tokenSerializer.Deserialize(request.TaskToken)
	if err0 != nil {
		return nil, ErrDeserializingToken
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: token.GetWorkflowId(),
		RunId:      token.GetRunId(),
	}

	var cancelRequested bool
	err = e.updateWorkflowExecution(ctx, namespaceID, workflowExecution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				e.logger.Debug("Heartbeat failed")
				return ErrWorkflowCompleted
			}

			scheduleID := token.GetScheduleId()
			if scheduleID == common.EmptyEventID { // client call RecordActivityHeartbeatByID, so get scheduleID by activityID
				scheduleID, err0 = getScheduleID(token.GetActivityId(), mutableState)
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

			if !isRunning || ai.StartedId == common.EmptyEventID ||
				(token.GetScheduleId() != common.EmptyEventID && token.ScheduleAttempt != ai.Attempt) {
				return ErrActivityTaskNotFound
			}

			cancelRequested = ai.CancelRequested

			e.logger.Debug("Activity heartbeat", tag.WorkflowScheduleID(scheduleID), tag.ActivityInfo(ai), tag.Bool(cancelRequested))

			// Save progress and last HB reported time.
			mutableState.UpdateActivityProgress(ai, request)

			return nil
		})

	if err != nil {
		return &historyservice.RecordActivityTaskHeartbeatResponse{}, err
	}

	return &historyservice.RecordActivityTaskHeartbeatResponse{CancelRequested: cancelRequested}, nil
}

// RequestCancelWorkflowExecution records request cancellation event for workflow execution
func (e *historyEngineImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	req *historyservice.RequestCancelWorkflowExecutionRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(req.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := req.CancelRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()
	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
	}

	firstExecutionRunID := request.GetFirstExecutionRunId()
	// If firstExecutionRunID is set on the request always try to cancel currently running execution
	if len(firstExecutionRunID) == 0 {
		execution.RunId = request.WorkflowExecution.RunId
	}

	return e.updateWorkflow(ctx, namespaceID, execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow cancel to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we cancel the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, ErrWorkflowExecutionNotFound
			}

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, ErrWorkflowParent
				}
			}

			isCancelRequested, cancelRequestID := mutableState.IsCancelRequested()
			if isCancelRequested {
				cancelRequest := req.CancelRequest
				if cancelRequest.GetRequestId() != "" {
					requestID := cancelRequest.GetRequestId()
					if requestID != "" && cancelRequestID == requestID {
						return updateWorkflowWithNewWorkflowTask, nil
					}
				}
				// if we consider workflow cancellation idempotent, then this error is redundant
				// this error maybe useful if this API is invoked by external, not workflow task from transfer queue.
				return nil, ErrCancellationAlreadyRequested
			}

			if _, err := mutableState.AddWorkflowExecutionCancelRequestedEvent("", req); err != nil {
				return nil, serviceerror.NewInternal("Unable to cancel workflow execution.")
			}

			return updateWorkflowWithNewWorkflowTask, nil
		})
}

func (e *historyEngineImpl) SignalWorkflowExecution(
	ctx context.Context,
	signalRequest *historyservice.SignalWorkflowExecutionRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(signalRequest.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := signalRequest.SignalRequest
	parentExecution := signalRequest.ExternalWorkflowExecution
	childWorkflowOnly := signalRequest.GetChildWorkflowOnly()
	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	return e.updateWorkflow(
		ctx,
		namespaceID,
		execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			executionInfo := mutableState.GetExecutionInfo()
			createWorkflowTask := true
			// Do not create workflow task when the workflow is cron and the cron has not been started yet
			if mutableState.GetExecutionInfo().CronSchedule != "" && !mutableState.HasProcessedOrPendingWorkflowTask() {
				createWorkflowTask = false
			}
			postActions := &updateWorkflowAction{
				createWorkflowTask: createWorkflowTask,
			}

			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			maxAllowedSignals := e.config.MaximumSignalsPerExecution(namespaceEntry.GetInfo().Name)
			if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
				e.logger.Info("Execution limit reached for maximum signals", tag.WorkflowSignalCount(executionInfo.SignalCount),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.WorkflowNamespaceID(namespaceID))
				return nil, ErrSignalsLimitExceeded
			}

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, ErrWorkflowParent
				}
			}

			// deduplicate by request id for signal workflow task
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
				return nil, serviceerror.NewInternal("Unable to signal workflow execution.")
			}

			return postActions, nil
		})
}

func (e *historyEngineImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	signalWithStartRequest *historyservice.SignalWithStartWorkflowExecutionRequest,
) (retResp *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {

	namespaceEntry, err := e.getActiveNamespaceEntry(signalWithStartRequest.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	sRequest := signalWithStartRequest.SignalWithStartRequest
	execution := commonpb.WorkflowExecution{
		WorkflowId: sRequest.WorkflowId,
	}

	var prevMutableState mutableState
	attempt := 1

	context, release, err0 := e.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)

	if err0 == nil {
		defer func() { release(retError) }()
	Just_Signal_Loop:
		for ; attempt <= conditionalRetryCount; attempt++ {
			// workflow not exist, will create workflow then signal
			mutableState, err1 := context.loadWorkflowExecution()
			if err1 != nil {
				if _, ok := err1.(*serviceerror.NotFound); ok {
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
			maxAllowedSignals := e.config.MaximumSignalsPerExecution(namespaceEntry.GetInfo().Name)
			if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
				e.logger.Info("Execution limit reached for maximum signals", tag.WorkflowSignalCount(executionInfo.SignalCount),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.WorkflowNamespaceID(namespaceID))
				return nil, ErrSignalsLimitExceeded
			}

			if _, err := mutableState.AddWorkflowExecutionSignaled(
				sRequest.GetSignalName(),
				sRequest.GetSignalInput(),
				sRequest.GetIdentity()); err != nil {
				return nil, serviceerror.NewInternal("Unable to signal workflow execution.")
			}

			// Create a transfer task to schedule a workflow task
			if !mutableState.HasPendingWorkflowTask() {
				_, err := mutableState.AddWorkflowTaskScheduledEvent(false)
				if err != nil {
					return nil, serviceerror.NewInternal("Failed to add workflow task scheduled event.")
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
			return &historyservice.SignalWithStartWorkflowExecutionResponse{RunId: context.getExecution().RunId}, nil
		} // end for Just_Signal_Loop
		if attempt == conditionalRetryCount+1 {
			return nil, ErrMaxAttemptsExceeded
		}
	} else {
		if _, ok := err0.(*serviceerror.NotFound); !ok {
			return nil, err0
		}
		// workflow not exist, will create workflow then signal
	}

	// Start workflow and signal
	startRequest := e.getStartRequest(namespaceID, sRequest)
	request := startRequest.StartRequest
	err = validateStartWorkflowExecutionRequest(request, e.config.MaxIDLengthLimit())
	if err != nil {
		return nil, err
	}
	e.overrideStartWorkflowExecutionRequest(namespaceEntry, request, metrics.HistorySignalWorkflowExecutionScope)

	workflowID := request.GetWorkflowId()
	// grab the current context as a lock, nothing more
	_, currentRelease, err := e.historyCache.getOrCreateCurrentWorkflowExecution(
		ctx,
		namespaceID,
		workflowID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { currentRelease(retError) }()

	execution = commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      uuid.New(),
	}

	clusterMetadata := e.shard.GetService().GetClusterMetadata()
	mutableState, err := e.createMutableState(clusterMetadata, namespaceEntry, execution.GetRunId())
	if err != nil {
		return nil, err
	}

	if prevMutableState != nil {
		prevLastWriteVersion, err := prevMutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		if prevLastWriteVersion > mutableState.GetCurrentVersion() {
			return nil, serviceerror.NewNamespaceNotActive(
				namespaceEntry.GetInfo().Name,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevLastWriteVersion),
			)
		}

		err = e.applyWorkflowIDReusePolicyForSigWithStart(prevMutableState.GetExecutionInfo(), namespaceID, execution, request.WorkflowIdReusePolicy)
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
		return nil, serviceerror.NewInternal("Failed to add workflow execution started event.")
	}

	// Add signal event
	if _, err := mutableState.AddWorkflowExecutionSignaled(
		sRequest.GetSignalName(),
		sRequest.GetSignalInput(),
		sRequest.GetIdentity()); err != nil {
		return nil, serviceerror.NewInternal("Failed to add workflow execution signaled event.")
	}

	if err = e.generateFirstWorkflowTask(
		mutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, err
	}

	context = newWorkflowExecutionContext(namespaceID, execution, e.shard, e.executionManager, e.logger)

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
		prevRunID = prevMutableState.GetExecutionInfo().GetRunId()
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
		if t.StartRequestID == request.GetRequestId() {
			return &historyservice.SignalWithStartWorkflowExecutionResponse{
				RunId: t.RunID,
			}, nil
			// delete history is expected here because duplicate start request will create history with different rid
		}
		return nil, err
	}

	if err != nil {
		return nil, err
	}
	return &historyservice.SignalWithStartWorkflowExecutionResponse{
		RunId: execution.RunId,
	}, nil
}

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(
	ctx context.Context,
	request *historyservice.RemoveSignalMutableStateRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(request.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, namespaceID, execution, false,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			mutableState.DeleteSignalRequested(request.GetRequestId())

			return nil
		})
}

func (e *historyEngineImpl) TerminateWorkflowExecution(
	ctx context.Context,
	terminateRequest *historyservice.TerminateWorkflowExecutionRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(terminateRequest.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	request := terminateRequest.TerminateRequest
	execution := commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
	}

	firstExecutionRunID := request.GetFirstExecutionRunId()
	// If firstExecutionRunID is set on the request always try to terminate currently running execution
	if len(firstExecutionRunID) == 0 {
		execution.RunId = request.WorkflowExecution.RunId
	}

	return e.updateWorkflow(
		ctx,
		namespaceID,
		execution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, ErrWorkflowCompleted
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow terminate to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we terminate the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, ErrWorkflowExecutionNotFound
			}

			eventBatchFirstEventID := mutableState.GetNextEventID()
			return updateWorkflowWithoutWorkflowTask, terminateWorkflow(
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
	ctx context.Context,
	completionRequest *historyservice.RecordChildExecutionCompletedRequest,
) error {

	namespaceEntry, err := e.getActiveNamespaceEntry(completionRequest.GetNamespaceId())
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	execution := commonpb.WorkflowExecution{
		WorkflowId: completionRequest.WorkflowExecution.WorkflowId,
		RunId:      completionRequest.WorkflowExecution.RunId,
	}

	return e.updateWorkflowExecution(ctx, namespaceID, execution, true,
		func(context workflowExecutionContext, mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			initiatedID := completionRequest.InitiatedId
			completedExecution := completionRequest.CompletedExecution
			completionEvent := completionRequest.CompletionEvent

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(initiatedID)
			if !isRunning || ci.StartedId == common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			switch completionEvent.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				attributes := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(initiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				attributes := completionEvent.GetWorkflowExecutionFailedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionFailedEvent(initiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				attributes := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(initiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
				attributes := completionEvent.GetWorkflowExecutionTerminatedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(initiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
				attributes := completionEvent.GetWorkflowExecutionTimedOutEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(initiatedID, completedExecution, attributes)
			}

			return err
		})
}

func (e *historyEngineImpl) ReplicateEventsV2(
	ctx context.Context,
	replicateRequest *historyservice.ReplicateEventsV2Request,
) error {

	return e.nDCReplicator.ApplyEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
) error {

	clusterName := request.GetSourceCluster()
	now := timestamp.TimeValue(request.GetStatusTime())

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
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
) (retError error) {

	return e.nDCActivityReplicator.SyncActivity(ctx, request)
}

func (e *historyEngineImpl) ResetWorkflowExecution(
	ctx context.Context,
	resetRequest *historyservice.ResetWorkflowExecutionRequest,
) (response *historyservice.ResetWorkflowExecutionResponse, retError error) {

	request := resetRequest.ResetRequest
	namespaceID := resetRequest.GetNamespaceId()
	workflowID := request.WorkflowExecution.GetWorkflowId()
	baseRunID := request.WorkflowExecution.GetRunId()

	baseContext, baseReleaseFn, err := e.historyCache.getOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      baseRunID,
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
	if request.GetWorkflowTaskFinishEventId() <= common.FirstEventID ||
		request.GetWorkflowTaskFinishEventId() >= baseMutableState.GetNextEventID() {
		return nil, serviceerror.NewInvalidArgument("Workflow task finish ID must be > 1 && <= workflow next event ID.")
	}
	// also load the current run of the workflow, it can be different from the base runID
	resp, err := e.executionManager.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  request.WorkflowExecution.GetWorkflowId(),
	})
	if err != nil {
		return nil, err
	}

	currentRunID := resp.RunID
	if baseRunID == "" {
		baseRunID = currentRunID
	}
	var currentContext workflowExecutionContext
	var currentMutableState mutableState
	var currentReleaseFn releaseWorkflowExecutionFunc
	if currentRunID == baseRunID {
		currentContext = baseContext
		currentMutableState = baseMutableState
	} else {
		currentContext, currentReleaseFn, err = e.historyCache.getOrCreateWorkflowExecution(
			ctx,
			namespaceID,
			commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      currentRunID,
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
	if currentMutableState.GetExecutionInfo().GetExecutionState().CreateRequestId == request.GetRequestId() {
		e.logger.Info("Duplicated reset request",
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(currentRunID),
			tag.WorkflowNamespaceID(namespaceID))
		return &historyservice.ResetWorkflowExecutionResponse{
			RunId: currentRunID,
		}, nil
	}

	resetRunID := uuid.New()
	baseRebuildLastEventID := request.GetWorkflowTaskFinishEventId() - 1
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
		namespaceID,
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
			e.shard.GetNamespaceCache(),
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
	return &historyservice.ResetWorkflowExecutionResponse{
		RunId: resetRunID,
	}, nil
}

func (e *historyEngineImpl) updateWorkflow(
	ctx context.Context,
	namespaceID string,
	execution commonpb.WorkflowExecution,
	action updateWorkflowActionFunc,
) (retError error) {

	workflowContext, err := e.loadWorkflow(ctx, namespaceID, execution.GetWorkflowId(), execution.GetRunId())
	if err != nil {
		return err
	}
	defer func() { workflowContext.getReleaseFn()(retError) }()

	return e.updateWorkflowHelper(workflowContext, action)
}

func (e *historyEngineImpl) updateWorkflowExecutionWithAction(
	ctx context.Context,
	namespaceID string,
	execution commonpb.WorkflowExecution,
	action updateWorkflowActionFunc,
) (retError error) {

	workflowContext, err := e.loadWorkflowOnce(ctx, namespaceID, execution.GetWorkflowId(), execution.GetRunId())
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
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		weContext := workflowContext.getContext()
		mutableState := workflowContext.getMutableState()

		// conduct caller action
		postActions, err := action(weContext, mutableState)
		if err != nil {
			if err == ErrStaleState {
				// Handler detected that cached workflow mutable could potentially be stale
				// Reload workflow execution history
				workflowContext.getContext().clear()
				if attempt != conditionalRetryCount {
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

		if postActions.createWorkflowTask {
			// Create a transfer task to schedule a workflow task
			if !mutableState.HasPendingWorkflowTask() {
				_, err := mutableState.AddWorkflowTaskScheduledEvent(false)
				if err != nil {
					return serviceerror.NewInternal("Failed to add workflow task scheduled event.")
				}
			}
		}

		err = workflowContext.getContext().updateWorkflowExecutionAsActive(e.shard.GetTimeSource().Now())
		if err == ErrConflict {
			if attempt != conditionalRetryCount {
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
	ctx context.Context,
	namespaceID string,
	execution commonpb.WorkflowExecution,
	createWorkflowTask bool,
	action func(context workflowExecutionContext, mutableState mutableState) error,
) error {

	return e.updateWorkflowExecutionWithAction(
		ctx,
		namespaceID,
		execution,
		getUpdateWorkflowActionFunc(createWorkflowTask, action),
	)
}

func getUpdateWorkflowActionFunc(
	createWorkflowTask bool,
	action func(context workflowExecutionContext, mutableState mutableState) error,
) updateWorkflowActionFunc {

	return func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
		err := action(context, mutableState)
		if err != nil {
			return nil, err
		}
		postActions := &updateWorkflowAction{
			createWorkflowTask: createWorkflowTask,
		}
		return postActions, nil
	}
}

func (e *historyEngineImpl) failWorkflowTask(
	context workflowExecutionContext,
	scheduleID int64,
	startedID int64,
	workflowTaskFailedErr *workflowTaskFailedError,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (mutableState, error) {

	// Clear any updates we have accumulated so far
	context.clear()

	// Reload workflow execution so we can apply the workflow task failure event
	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return nil, err
	}

	if _, err = mutableState.AddWorkflowTaskFailedEvent(
		scheduleID,
		startedID,
		workflowTaskFailedErr.failedCause,
		failure.NewServerFailure(workflowTaskFailedErr.Error(), true),
		request.GetIdentity(),
		request.GetBinaryChecksum(),
		"",
		"",
		0); err != nil {
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
	request *workflowservice.StartWorkflowExecutionRequest,
	maxIDLengthLimit int,
) error {

	if len(request.GetRequestId()) == 0 {
		return serviceerror.NewInvalidArgument("Missing request ID.")
	}
	if timestamp.DurationValue(request.GetWorkflowExecutionTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowExecutionTimeoutSeconds.")
	}
	if timestamp.DurationValue(request.GetWorkflowRunTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowRunTimeoutSeconds.")
	}
	if timestamp.DurationValue(request.GetWorkflowTaskTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowTaskTimeoutSeconds.")
	}
	if request.TaskQueue == nil || request.TaskQueue.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing Taskqueue.")
	}
	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing WorkflowType.")
	}
	if len(request.GetNamespace()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}
	if len(request.GetWorkflowId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}
	if len(request.TaskQueue.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TaskQueue exceeds length limit.")
	}
	if len(request.WorkflowType.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	return common.ValidateRetryPolicy(request.RetryPolicy)
}

func (e *historyEngineImpl) overrideStartWorkflowExecutionRequest(
	namespaceEntry *cache.NamespaceCacheEntry,
	request *workflowservice.StartWorkflowExecutionRequest,
	metricsScope int,
) {
	namespace := namespaceEntry.GetInfo().Name
	executionTimeout := getWorkflowExecutionTimeout(namespace, timestamp.DurationValue(request.GetWorkflowExecutionTimeout()), e.config)
	if executionTimeout != timestamp.DurationValue(request.GetWorkflowExecutionTimeout()) {
		request.WorkflowExecutionTimeout = timestamp.DurationPtr(executionTimeout)
		e.metricsClient.Scope(
			metricsScope,
			metrics.NamespaceTag(namespace),
		).IncCounter(metrics.WorkflowExecutionTimeoutOverrideCount)
	}

	runTimeout := getWorkflowRunTimeout(namespace, timestamp.DurationValue(request.GetWorkflowRunTimeout()), executionTimeout, e.config)
	if runTimeout != timestamp.DurationValue(request.GetWorkflowRunTimeout()) {
		request.WorkflowRunTimeout = &runTimeout
		e.metricsClient.Scope(
			metricsScope,
			metrics.NamespaceTag(namespace),
		).IncCounter(metrics.WorkflowRunTimeoutOverrideCount)
	}

	taskTimeout := timestamp.DurationValue(request.GetWorkflowTaskTimeout())
	if taskTimeout == 0 {
		taskTimeout = timestamp.RoundUp(e.config.DefaultWorkflowTaskTimeout(namespace))
	}
	maxTaskTimeout := timestamp.RoundUp(e.config.MaxWorkflowTaskTimeout(namespace))
	taskTimeout = timestamp.MinDuration(taskTimeout, maxTaskTimeout)
	taskTimeout = timestamp.MinDuration(taskTimeout, runTimeout)

	if taskTimeout != timestamp.DurationValue(request.GetWorkflowTaskTimeout()) {
		request.WorkflowTaskTimeout = &taskTimeout
		e.metricsClient.Scope(
			metricsScope,
			metrics.NamespaceTag(namespace),
		).IncCounter(metrics.WorkflowTaskTimeoutOverrideCount)
	}
}

func validateNamespaceUUID(
	namespaceUUID string,
) (string, error) {

	if namespaceUUID == "" {
		return "", serviceerror.NewInvalidArgument("Missing namespace UUID.")
	} else if uuid.Parse(namespaceUUID) == nil {
		return "", serviceerror.NewInvalidArgument("Invalid namespace UUID.")
	}
	return namespaceUUID, nil
}

func (e *historyEngineImpl) getActiveNamespaceEntry(
	namespaceUUID string,
) (*cache.NamespaceCacheEntry, error) {

	return getActiveNamespaceEntryFromShard(e.shard, namespaceUUID)
}

func getActiveNamespaceEntryFromShard(
	shard ShardContext,
	namespaceUUID string,
) (*cache.NamespaceCacheEntry, error) {

	namespaceID, err := validateNamespaceUUID(namespaceUUID)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	if err = namespaceEntry.GetNamespaceNotActiveErr(); err != nil {
		return nil, err
	}
	return namespaceEntry, nil
}

func getScheduleID(
	activityID string,
	mutableState mutableState,
) (int64, error) {

	if activityID == "" {
		return 0, serviceerror.NewInvalidArgument("Neither ActivityID nor ScheduleID is provided")
	}
	activityInfo, ok := mutableState.GetActivityByActivityID(activityID)
	if !ok {
		return 0, serviceerror.NewInvalidArgument("Cannot locate Activity ScheduleID")
	}
	return activityInfo.ScheduleId, nil
}

func (e *historyEngineImpl) getStartRequest(
	namespaceID string,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) *historyservice.StartWorkflowExecutionRequest {

	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                request.GetNamespace(),
		WorkflowId:               request.GetWorkflowId(),
		WorkflowType:             request.GetWorkflowType(),
		TaskQueue:                request.GetTaskQueue(),
		Input:                    request.GetInput(),
		WorkflowExecutionTimeout: request.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:       request.GetWorkflowRunTimeout(),
		WorkflowTaskTimeout:      request.GetWorkflowTaskTimeout(),
		Identity:                 request.GetIdentity(),
		RequestId:                request.GetRequestId(),
		WorkflowIdReusePolicy:    request.GetWorkflowIdReusePolicy(),
		RetryPolicy:              request.GetRetryPolicy(),
		CronSchedule:             request.GetCronSchedule(),
		Memo:                     request.GetMemo(),
		SearchAttributes:         request.GetSearchAttributes(),
		Header:                   request.GetHeader(),
	}

	return common.CreateHistoryStartWorkflowRequest(namespaceID, req, nil, e.shard.GetTimeSource().Now())
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
	namespaceID string,
	execution commonpb.WorkflowExecution,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
) error {

	prevStartRequestID := prevExecutionInfo.GetExecutionState().CreateRequestId
	prevRunID := prevExecutionInfo.ExecutionState.RunId
	prevState := prevExecutionInfo.ExecutionState.State
	prevStatus := prevExecutionInfo.ExecutionState.Status

	return e.applyWorkflowIDReusePolicyHelper(
		prevStartRequestID,
		prevRunID,
		prevState,
		prevStatus,
		namespaceID,
		execution,
		wfIDReusePolicy,
	)

}

func (e *historyEngineImpl) applyWorkflowIDReusePolicyHelper(
	prevStartRequestID,
	prevRunID string,
	prevState enumsspb.WorkflowExecutionState,
	prevStatus enumspb.WorkflowExecutionStatus,
	namespaceID string,
	execution commonpb.WorkflowExecution,
	wfIDReusePolicy enumspb.WorkflowIdReusePolicy,
) error {

	// here we know there is some information about the prev workflow, i.e. either running right now
	// or has history check if the this workflow is finished
	switch prevState {
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		msg := "Workflow execution is already running. WorkflowId: %v, RunId: %v."
		return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		// previous workflow completed, proceed
	default:
		// persistence.WorkflowStateZombie or unknown type
		return serviceerror.NewInternal(fmt.Sprintf("Failed to process workflow, workflow has invalid state: %v.", prevState))
	}

	switch wfIDReusePolicy {
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY:
		if _, ok := FailedWorkflowStatuses[prevStatus]; !ok {
			msg := "Workflow execution already finished successfully. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: allow duplicate workflow Id if last run failed."
			return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
		}
	case enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE:
		// as long as workflow not running, so this case has no check
	case enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE:
		msg := "Workflow execution already finished. WorkflowId: %v, RunId: %v. Workflow Id reuse policy: reject duplicate workflow Id."
		return getWorkflowAlreadyStartedError(msg, prevStartRequestID, execution.GetWorkflowId(), prevRunID)
	default:
		return serviceerror.NewInternal(fmt.Sprintf("Failed to process start workflow reuse policy: %v.", wfIDReusePolicy))
	}

	return nil
}

func getWorkflowAlreadyStartedError(errMsg string, createRequestID string, workflowID string, runID string) error {
	return serviceerror.NewWorkflowExecutionAlreadyStarted(
		fmt.Sprintf(errMsg, workflowID, runID),
		createRequestID,
		runID,
	)
}

func (e *historyEngineImpl) GetReplicationMessages(
	ctx context.Context,
	pollingCluster string,
	lastReadMessageID int64,
) (*replicationspb.ReplicationMessages, error) {

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

	// Set cluster status for sync shard info
	replicationMessages.SyncShardStatus = &replicationspb.SyncShardStatus{
		StatusTime: timestamp.TimePtr(e.timeSource.Now()),
	}
	e.logger.Debug("Successfully fetched replication messages.", tag.Counter(len(replicationMessages.ReplicationTasks)))
	return replicationMessages, nil
}

func (e *historyEngineImpl) GetDLQReplicationMessages(
	ctx context.Context,
	taskInfos []*replicationspb.ReplicationTaskInfo,
) ([]*replicationspb.ReplicationTask, error) {

	scope := metrics.HistoryGetDLQReplicationMessagesScope
	sw := e.metricsClient.StartTimer(scope, metrics.GetDLQReplicationMessagesLatency)
	defer sw.Stop()

	tasks := make([]*replicationspb.ReplicationTask, len(taskInfos))
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
	ctx context.Context,
	namespaceUUID string,
	workflowID string,
	runID string,
	reapplyEvents []*historypb.HistoryEvent,
) error {

	if e.config.SkipReapplicationByNamespaceId(namespaceUUID) {
		return nil
	}

	namespaceEntry, err := e.getActiveNamespaceEntry(namespaceUUID)
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id
	// remove run id from the execution so that reapply events to the current run
	currentExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
	}

	return e.updateWorkflowExecutionWithAction(
		ctx,
		namespaceID,
		currentExecution,
		func(context workflowExecutionContext, mutableState mutableState) (*updateWorkflowAction, error) {
			// Filter out reapply event from the same cluster
			toReapplyEvents := make([]*historypb.HistoryEvent, 0, len(reapplyEvents))
			lastWriteVersion, err := mutableState.GetLastWriteVersion()
			if err != nil {
				return nil, err
			}
			for _, event := range reapplyEvents {
				if event.GetVersion() == lastWriteVersion {
					// The reapply is from the same cluster. Ignoring.
					continue
				}
				dedupResource := definition.NewEventReappliedID(runID, event.GetEventId(), event.GetVersion())
				if mutableState.IsResourceDuplicated(dedupResource) {
					// already apply the signal
					continue
				}
				toReapplyEvents = append(toReapplyEvents, event)
			}
			if len(toReapplyEvents) == 0 {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			if !mutableState.IsWorkflowExecutionRunning() {
				// need to reset target workflow (which is also the current workflow)
				// to accept events to be reapplied
				baseRunID := mutableState.GetExecutionInfo().GetRunId()
				resetRunID := uuid.New()
				baseRebuildLastEventID := mutableState.GetPreviousStartedEventID()

				// TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
				//  since cannot reapply event to a finished workflow which had no workflow tasks started
				if baseRebuildLastEventID == common.EmptyEventID {
					e.logger.Warn("cannot reapply event to a finished workflow",
						tag.WorkflowNamespaceID(namespaceID),
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
					namespaceID,
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
						e.shard.GetNamespaceCache(),
						e.shard.GetClusterMetadata(),
						context,
						mutableState,
						noopReleaseFn,
					),
					eventsReapplicationResetWorkflowReason,
					toReapplyEvents,
				); err != nil {
					return nil, err
				}
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}

			postActions := &updateWorkflowAction{
				createWorkflowTask: true,
			}
			// Do not create workflow task when the workflow is cron and the cron has not been started yet
			if mutableState.GetExecutionInfo().CronSchedule != "" && !mutableState.HasProcessedOrPendingWorkflowTask() {
				postActions.createWorkflowTask = false
			}
			reappliedEvents, err := e.eventsReapplier.reapplyEvents(
				ctx,
				mutableState,
				toReapplyEvents,
				runID,
			)
			if err != nil {
				e.logger.Error("failed to re-apply stale events", tag.Error(err))
				return nil, serviceerror.NewInternal("unable to re-apply stale events")
			}
			if len(reappliedEvents) == 0 {
				return &updateWorkflowAction{
					noop: true,
				}, nil
			}
			return postActions, nil
		})
}

func (e *historyEngineImpl) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
) (*historyservice.GetDLQMessagesResponse, error) {

	tasks, token, err := e.replicationDLQHandler.getMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageId(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.GetDLQMessagesResponse{
		Type:             request.GetType(),
		ReplicationTasks: tasks,
		NextPageToken:    token,
	}, nil
}

func (e *historyEngineImpl) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
) error {

	return e.replicationDLQHandler.purgeMessages(
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageId(),
	)
}

func (e *historyEngineImpl) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
) (*historyservice.MergeDLQMessagesResponse, error) {

	token, err := e.replicationDLQHandler.mergeMessages(
		ctx,
		request.GetSourceCluster(),
		request.GetInclusiveEndMessageId(),
		int(request.GetMaximumPageSize()),
		request.GetNextPageToken(),
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.MergeDLQMessagesResponse{
		NextPageToken: token,
	}, nil
}

func (e *historyEngineImpl) RefreshWorkflowTasks(
	ctx context.Context,
	namespaceUUID string,
	execution commonpb.WorkflowExecution,
) (retError error) {

	namespaceEntry, err := e.getActiveNamespaceEntry(namespaceUUID)
	if err != nil {
		return err
	}
	namespaceID := namespaceEntry.GetInfo().Id

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)
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
		e.shard.GetNamespaceCache(),
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
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) (workflowContext, error) {

	context, release, err := e.historyCache.getOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
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
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) (workflowContext, error) {

	if runID != "" {
		return e.loadWorkflowOnce(ctx, namespaceID, workflowID, runID)
	}

	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {

		workflowContext, err := e.loadWorkflowOnce(ctx, namespaceID, workflowID, "")
		if err != nil {
			return nil, err
		}

		if workflowContext.getMutableState().IsWorkflowExecutionRunning() {
			return workflowContext, nil
		}

		// workflow not running, need to check current record
		resp, err := e.shard.GetExecutionManager().GetCurrentExecution(
			&persistence.GetCurrentExecutionRequest{
				NamespaceID: namespaceID,
				WorkflowID:  workflowID,
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

	return nil, serviceerror.NewInternal("unable to locate current workflow execution")
}
