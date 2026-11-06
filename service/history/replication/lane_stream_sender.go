package replication

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type laneStreamSender struct {
	server                  historyservice.HistoryService_StreamWorkflowReplicationMessagesServer
	shardContext            historyi.ShardContext
	historyEngine           historyi.Engine
	taskConverter           SourceTaskConverter
	metrics                 metrics.Handler
	logger                  log.Logger
	status                  int32
	clientClusterName       string
	clientShardKey          ClusterShardKey
	serverShardKey          ClusterShardKey
	clientClusterShardCount int32
	recvSignalChan          chan struct{}
	shutdownChan            channel.ShutdownOnce
	config                  *configs.Config
	isTieredStackEnabled    bool
	readerGroup             *replicationReaderGroup
	flowController          SenderFlowController
	sendLock                sync.Mutex
	ssRateLimiter           ServerSchedulerRateLimiter

	// lanesEnabledSetting is ReplicationStreamSenderLanesEnabled when the stream started.
	lanesEnabledSetting bool
	lanePolicyFactory   LaneStreamPolicyFactory
	// lanePolicyMu serializes calls to lanePolicy, which is nil when the stream has no lane
	// policy.
	lanePolicyMu     sync.Mutex
	lanePolicy       LaneStreamPolicy
	lanePolicyClosed bool
	// laneReconcileMu keeps key reuse and its policy callbacks ordered with retirement.
	laneReconcileMu sync.Mutex
	// lanePolicyStopped is set when the stream stops. The policy is closed then, or when the
	// call that is running returns, so that stopping never waits for the policy.
	lanePolicyStopped atomic.Bool
	// lanePolicyDisabled is set when the policy or one of its limiters panics. The stream
	// then calls nothing more on the policy except Close.
	lanePolicyDisabled atomic.Bool
	// laneRegistry is nil when the stream does not use lanes.
	laneRegistry *senderLaneRegistry
	// hasPersistedLanes is whether the reader state had lanes when the stream started.
	// laneRecoveryFloor is then the persisted HIGH scope, which covers every persisted lane.
	hasPersistedLanes bool
	laneRecoveryFloor int64
	// laneRestoreIncomplete is whether some persisted lanes are not continued, so the default
	// lane re-sends their tasks from laneRecoveryFloor.
	laneRestoreIncomplete bool
	// lanesConfirmed is whether the receiver supports lanes. It is set by the first ack,
	// after which initialLaneState is closed.
	lanesConfirmed       atomic.Bool
	initialLaneStateOnce sync.Once
	initialLaneState     chan struct{}
	// laneStateMu guards the lane state that comes from the policy's answers.
	laneStateMu sync.Mutex
	// requestedLanes are the lanes in the policy's latest answer.
	requestedLanes map[string]struct{}
	laneLimiters   map[string]LaneLimiter
	// laneRefusedAt is when each lane's limiter last refused a send.
	laneRefusedAt map[string]time.Time
	laneLoopsOnce sync.Once
	// handoverInProgress is whether the shard had a namespace handover in progress at the
	// last ack. Lanes then send without rate limits.
	handoverInProgress atomic.Bool
}

func newLaneStreamSender(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext historyi.ShardContext,
	historyEngine historyi.Engine,
	ssRateLimiter ServerSchedulerRateLimiter,
	taskConverter SourceTaskConverter,
	clientClusterName string,
	clientClusterShardCount int32,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	config *configs.Config,
	lanePolicyFactory LaneStreamPolicyFactory,
) *laneStreamSender {
	logger := log.With(
		shardContext.GetLogger(),
		tag.TargetCluster(clientClusterName), // client is the target cluster (passive cluster)
		tag.TargetShardID(clientShardKey.ShardID),
		tag.ShardID(serverShardKey.ShardID), // server is the source cluster (active cluster)
		tag.Operation("replication-stream-sender"),
	)
	// Read the dynamic config once so every derived field sees the same snapshot: a
	// flip between two reads would leave the sender half in each mode until the recv
	// loop's config guard restarts the stream.
	tieredStackEnabled := config.EnableReplicationTaskTieredProcessing()
	lanesEnabledSetting := config.ReplicationStreamSenderLanesEnabled()
	s := &laneStreamSender{
		server:                  server,
		shardContext:            shardContext,
		historyEngine:           historyEngine,
		taskConverter:           taskConverter,
		metrics:                 shardContext.GetMetricsHandler(),
		logger:                  logger,
		status:                  common.DaemonStatusInitialized,
		clientClusterName:       clientClusterName,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		clientClusterShardCount: clientClusterShardCount,
		recvSignalChan:          make(chan struct{}, 1),
		shutdownChan:            channel.NewShutdownOnce(),
		config:                  config,
		isTieredStackEnabled:    tieredStackEnabled,
		readerGroup:             newReaderGroupIfEnabled(config.EnableReplicationReaderGroup, shardContext, clientShardKey, tieredStackEnabled, logger),
		flowController:          NewSenderFlowController(config, logger),
		ssRateLimiter:           ssRateLimiter,
		lanesEnabledSetting:     lanesEnabledSetting,
		lanePolicyFactory:       lanePolicyFactory,
		initialLaneState:        make(chan struct{}),
		laneLimiters:            make(map[string]LaneLimiter),
		laneRefusedAt:           make(map[string]time.Time),
	}
	// Lanes split HIGH priority traffic, so they require the tiered stack.
	if lanesEnabledSetting && tieredStackEnabled && lanePolicyFactory != nil {
		s.initLaneRegistry()
	}
	return s
}

func (s *laneStreamSender) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	getSenderEventLoop := func(priority enumsspb.TaskPriority) func() error {
		return func() error {
			return s.sendEventLoop(priority)
		}
	}

	if s.isTieredStackEnabled {
		// High Priority sender is used for live traffic
		// Low Priority sender is used for force replication closed workflow
		go WrapEventLoop(s.server.Context(), getSenderEventLoop(enumsspb.TASK_PRIORITY_HIGH), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
		go WrapEventLoop(s.server.Context(), getSenderEventLoop(enumsspb.TASK_PRIORITY_LOW), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
	} else {
		go WrapEventLoop(s.server.Context(), getSenderEventLoop(enumsspb.TASK_PRIORITY_UNSPECIFIED), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
	}

	go WrapEventLoop(s.server.Context(), s.recvEventLoop, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
	go livenessMonitor(
		s.recvSignalChan,
		s.config.ReplicationStreamSyncStatusDuration,
		s.config.ReplicationStreamSenderLivenessMultiplier,
		s.shutdownChan,
		s.Stop,
		s.logger,
	)
	s.logger.Info("StreamSender started.")
}

func (s *laneStreamSender) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	s.shutdownChan.Shutdown()
	s.closeLanePolicy()
	s.logger.Info("StreamSender stopped.")
}

// IsValid returns false once ReplicationStreamSenderLanesEnabled changes, so that the
// stream reconnects with lanes turned on or off.
func (s *laneStreamSender) IsValid() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStarted &&
		s.config.ReplicationStreamSenderLanesEnabled() == s.lanesEnabledSetting
}

func (s *laneStreamSender) Wait() {
	<-s.shutdownChan.Channel()
}

func (s *laneStreamSender) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: s.clientShardKey,
		Server: s.serverShardKey,
	}
}

func (s *laneStreamSender) recvEventLoop() (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()

	defer log.CapturePanic(s.logger, &panicErr)

	for !s.shutdownChan.IsShutdown() {
		if s.isTieredStackEnabled != s.config.EnableReplicationTaskTieredProcessing() {
			return NewStreamError("StreamSender detected tiered stack change, restart the stream", nil)
		}
		if (s.readerGroup != nil) != s.config.EnableReplicationReaderGroup() {
			return NewStreamError("StreamSender detected reader group config change, restart the stream", nil)
		}

		req, err := s.server.Recv()
		if err != nil {
			return NewStreamError("StreamSender failed to receive", err)
		}
		// Signal liveness on arrival rather than after processing, so that the liveness
		// monitor also stops a stream whose first ack never finishes processing, for example
		// because a lane policy call hangs.
		select {
		case s.recvSignalChan <- struct{}{}:
		default:
			// signal channel is full. Continue
		}
		switch attr := req.GetAttributes().(type) {
		case *historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
			if err := s.recvSyncReplicationState(attr.SyncReplicationState); err != nil {
				return fmt.Errorf("streamSender unable to handle SyncReplicationState: %w", err)
			}
			metrics.ReplicationTasksRecv.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.clientShardKey.ClusterID),
				metrics.ToClusterIDTag(s.serverShardKey.ClusterID),
				metrics.OperationTag(metrics.SyncWatermarkScope),
			)
		default:
			return fmt.Errorf("streamSender unable to handle request: %w, taskAttr: %v", err, attr)
		}

	}
	return nil
}

func (s *laneStreamSender) sendEventLoop(priority enumsspb.TaskPriority) (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()

	defer log.CapturePanic(s.logger, &panicErr)

	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)

	catchupEndExclusiveWatermark, err := s.sendCatchUp(priority)
	if err != nil {
		return fmt.Errorf("streamSender unable to catch up replication tasks: %w", err)
	}
	err = s.sendLive(
		priority,
		newTaskNotificationChan,
		catchupEndExclusiveWatermark,
	)
	if err != nil {
		return fmt.Errorf("streamSender unable to stream replication tasks: %w", err)
	}
	return nil
}

func (s *laneStreamSender) recvSyncReplicationState(
	attr *replicationspb.SyncReplicationState,
) error {
	if s.laneRegistry != nil {
		return s.recvLaneSyncReplicationState(attr)
	}
	if s.readerGroup != nil {
		readerState, err := s.readerGroup.BuildReaderState(attr)
		if err != nil {
			return err
		}
		taskID, ts, err := s.readerGroup.FailoverWatermark(attr)
		if err != nil {
			return err
		}
		if s.isTieredStackEnabled {
			s.flowController.RefreshReceiverFlowControlInfo(attr)
		}
		readerID := s.readerGroup.ReaderID()
		if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, readerState); err != nil {
			return err
		}
		return s.shardContext.UpdateRemoteReaderInfo(readerID, taskID, ts)
	}

	var readerState *persistencespb.QueueReaderState
	switch s.isTieredStackEnabled {
	case true:
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return NewStreamError("streamSender encountered unsupported SyncReplicationState", nil)
		}
		readerState = &persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				// index 0 is for overall low watermark. In tiered stack it is Min(LowWatermark-high priority, LowWatermark-low priority)
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				// index 1 is for high priority
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetHighPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				// index 2 is for low priority
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetLowPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		}
	case false:
		if attr.HighPriorityState != nil || attr.LowPriorityState != nil {
			return NewStreamError("streamSender encountered unsupported SyncReplicationState", nil)
		}
		readerState = &persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				// in single stack, index 0 is for overall low watermark
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		}
	default:
	}

	inclusiveLowWatermark := attr.GetInclusiveLowWatermark()
	inclusiveLowWatermarkTime := attr.GetInclusiveLowWatermarkTime()

	if s.isTieredStackEnabled {
		s.flowController.RefreshReceiverFlowControlInfo(attr)
	}

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)

	err := s.shardContext.UpdateReplicationQueueReaderState(
		readerID,
		readerState,
	)
	if err != nil {
		return err
	}

	if s.isTieredStackEnabled {
		// RemoteReaderInfo is used for failover. It is to determine if remote cluster has caught up on replication tasks.
		// In tiered stack, we will use high priority watermark to do failover as High Priority channel is supposed to be used for live traffic
		// and Low Priority channel is used for force replication closed workflow.
		return s.shardContext.UpdateRemoteReaderInfo(
			readerID,
			attr.HighPriorityState.InclusiveLowWatermark-1,
			attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime(),
		)
	}
	return s.shardContext.UpdateRemoteReaderInfo(
		readerID,
		inclusiveLowWatermark-1,
		inclusiveLowWatermarkTime.AsTime(),
	)
}

func (s *laneStreamSender) sendCatchUp(priority enumsspb.TaskPriority) (int64, error) {
	catchupEndExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID

	var catchupBeginInclusiveWatermark int64
	if s.laneRegistry != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
		// Whether persisted lanes continue depends on the receiver's lane support, which
		// the first ack reports.
		if err := s.sendLaneCatchUpPrime(catchupEndExclusiveWatermark); err != nil {
			return 0, err
		}
		catchupBeginInclusiveWatermark = s.laneCatchupBeginWatermark(catchupEndExclusiveWatermark)
		if s.lanesConfirmed.Load() {
			s.startLaneLoops()
		}
	} else {
		catchupBeginInclusiveWatermark = s.catchupBeginWatermark(priority, catchupEndExclusiveWatermark)
	}
	err := s.sendDefaultTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	)
	if err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

// catchupBeginWatermark returns the inclusive begin watermark for the catch-up scan:
// the persisted reader-state cursor for the given priority, falling back to the
// current end watermark when no state is persisted for this reader.
func (s *laneStreamSender) catchupBeginWatermark(priority enumsspb.TaskPriority, end int64) int64 {
	if s.readerGroup != nil {
		return s.readerGroup.CatchupBeginWatermark(end, priority)
	}
	queueState, ok := s.shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		s.logger.Debug("StreamSender queueState not found")
		return end
	}
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	readerState, ok := queueState.ReaderStates[readerID]
	if !ok {
		s.logger.Debug(fmt.Sprintf("StreamSender readerState not found, readerID %v", readerID))
		return end
	}
	return s.getSendCatchupBeginInclusiveWatermark(readerState, priority)
}

func (s *laneStreamSender) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	// priorityScopeIndex tolerates the single-stack format (1 scope -> index 0) and the
	// tiered format (3 scopes -> HIGH=1, LOW=2). When switching from single to tiered
	// stack the reader state is still in the old format, in which case using the overall
	// low watermark (scope 0) is safe as long as we always guarantee the overall low
	// watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark).
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes), s.readerGroup != nil)].Range.InclusiveMin.TaskId
}

func (s *laneStreamSender) sendLive(
	priority enumsspb.TaskPriority,
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	syncStatusTimer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer syncStatusTimer.Stop()
	sendTasks := func() error {
		endExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		err := s.sendDefaultTasks(
			priority,
			beginInclusiveWatermark,
			endExclusiveWatermark,
		)
		if err != nil {
			return err
		}
		beginInclusiveWatermark = endExclusiveWatermark
		if !syncStatusTimer.Stop() {
			select {
			case <-syncStatusTimer.C:
			default:
			}
		}
		syncStatusTimer.Reset(s.config.ReplicationStreamSendEmptyTaskDuration())
		return nil
	}

	for {
		select {
		case <-newTaskNotificationChan:
			if err := sendTasks(); err != nil {
				return err
			}
		case <-syncStatusTimer.C:
			if err := sendTasks(); err != nil {
				return err
			}
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

//nolint:revive // cyclomatic complexity matches StreamSenderImpl.sendTasks, which this copies.
func (s *laneStreamSender) sendTasks(
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
	target *laneSendTarget,
) error {
	if beginInclusiveWatermark > endExclusiveWatermark {
		err := serviceerror.NewInternalf("StreamWorkflowReplication encountered invalid task range [%v, %v)",
			beginInclusiveWatermark,
			endExclusiveWatermark,
		)
		return err
	}
	if beginInclusiveWatermark == endExclusiveWatermark {
		return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           nil,
					ExclusiveHighWatermark:     endExclusiveWatermark,
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
					Priority:                   priority,
					LaneInfo:                   target.getLaneInfo(),
				},
			},
		})
	}

	callerInfo := getReplicaitonCallerInfo(priority)
	ctx := headers.SetCallerInfo(s.server.Context(), callerInfo)
	iter, err := s.historyEngine.GetReplicationTasksIter(
		ctx,
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	if err != nil {
		return err
	}
	skipCount := 0
Loop:
	for iter.HasNext() {
		if s.shutdownChan.IsShutdown() {
			return nil
		}

		item, err := iter.Next()
		if err != nil {
			return fmt.Errorf("streamSender unable to get next replication task: %w", err)
		}

		skipCount++
		// To avoid a situation: we are skipping a lot of tasks and never send any task, receiver side will not have updated high watermark,
		// so it will not ACK back to sender, sender will not update the ACK level.
		// i.e. in tiered stack, if no low priority task in queue, we should still send watermark info to receiver to let it update ACK level.
		if skipCount > TaskMaxSkipCount {
			err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ExclusiveHighWatermark:     item.GetTaskID(),
						ExclusiveHighWatermarkTime: timestamppb.New(item.GetVisibilityTime()),
						Priority:                   priority,
						LaneInfo:                   target.getLaneInfo(),
					},
				},
			})
			if err != nil {
				return err
			}
			skipCount = 0
		}
		if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED && // case: skip priority check. When priority is unspecified, send all tasks
			priority != s.getTaskPriority(item) { // case: skip task with different priority than this loop
			continue Loop
		}
		if !s.shouldProcessTask(item) {
			continue Loop
		}
		if owner := target.skippingOwner(item); owner != "" {
			metrics.ReplicationLaneTasksSkippedByDefault.With(s.metrics).Record(
				1,
				metrics.ToClusterTag(s.clientClusterName),
				metrics.ReplicationLaneTag(owner),
			)
			continue Loop
		}
		if !target.accepts(item) {
			continue Loop
		}
		metrics.ReplicationTaskLoadLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
		)

		var attempt int64
		workflowLockPriority := locks.PriorityLow
		lowPriorityLockAttempts := 0
		operation := func() error {
			attempt++
			startTime := time.Now().UTC()
			defer func() {
				metrics.ReplicationTaskGenerationLatency.With(s.metrics).Record(
					time.Since(startTime),
					metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
					metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
					metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
					metrics.ReplicationTaskPriorityTag(priority),
				)
			}()
			task, err := s.taskConverter.Convert(item, s.clientShardKey.ClusterID, priority, workflowLockPriority)
			if err != nil {
				if workflowLockPriority == locks.PriorityLow && errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) {
					lowPriorityLockAttempts++
					if lowPriorityLockAttempts >= max(1, s.config.ReplicationTaskConverterLowPriorityLockMaxAttempts()) {
						workflowLockPriority = locks.PriorityHigh
					}
				}
				return s.recordRetry(
					item,
					enumsspb.REPLICATION_TASK_TYPE_UNSPECIFIED,
					priority,
					attempt,
					wideevents.ReplOperationTaskConversion,
					&convertError{err: fmt.Errorf("convert: %w", err)},
				)
			}
			if task == nil {
				return nil
			}
			task.Priority = priority
			if s.isTieredStackEnabled {
				if err := s.flowController.Wait(s.server.Context(), priority); err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					// continue to send task if wait operation times out.
				}
			}
			if s.config.ReplicationEnableRateLimit() && task.Priority == enumsspb.TASK_PRIORITY_LOW {
				nsName, err := s.shardContext.GetNamespaceRegistry().GetNamespaceName(
					namespace.ID(item.GetNamespaceID()),
				)
				if err != nil {
					// if there is error, then blindly send the task, better safe than sorry
					nsName = namespace.EmptyName
				}
				rlStartTime := time.Now().UTC()
				waitErr := s.ssRateLimiter.Wait(s.server.Context(), quotas.NewRequest(
					task.TaskType.String(),
					taskSchedulerToken,
					nsName.String(),
					headers.SystemPreemptableCallerInfo.CallerType,
					0,
					"",
				))
				if waitErr != nil {
					return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationRateLimit, fmt.Errorf("rate_limit: %w", waitErr))
				}
				metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)))
			}
			if target != nil && target.rateLimit != nil {
				if limitErr := target.rateLimit(s.server.Context()); limitErr != nil {
					return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationRateLimit, fmt.Errorf("lane_rate_limit: %w", limitErr))
				}
			}
			if s.config.EmitReplicationLifecycleEvents() {
				s.emitReplicationSent(task, item)
			}
			sendErr := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ReplicationTasks:           []*replicationspb.ReplicationTask{task},
						ExclusiveHighWatermark:     task.SourceTaskId + 1,
						ExclusiveHighWatermarkTime: task.VisibilityTime,
						Priority:                   priority,
						LaneInfo:                   target.getLaneInfo(),
					},
				},
			})
			if sendErr != nil {
				return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationStreamSend, fmt.Errorf("send: %w", sendErr))
			}
			skipCount = 0
			if target.getLaneInfo() != nil {
				metrics.ReplicationLaneTasksSent.With(s.metrics).Record(
					1,
					metrics.ToClusterTag(s.clientClusterName),
					metrics.ReplicationLaneTag(target.logicalKey),
				)
			}
			metrics.ReplicationTasksSend.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTag(task)),
			)
			return nil
		}

		retryPolicy := backoff.NewExponentialRetryPolicy(s.config.ReplicationStreamSenderErrorRetryWait()).
			WithBackoffCoefficient(s.config.ReplicationStreamSenderErrorRetryBackoffCoefficient()).
			WithMaximumInterval(s.config.ReplicationStreamSenderErrorRetryMaxInterval()).
			WithMaximumAttempts(s.config.ReplicationStreamSenderErrorRetryMaxAttempts()).
			WithExpirationInterval(s.config.ReplicationStreamSenderErrorRetryExpiration())

		err = backoff.ThrottleRetry(operation, retryPolicy, isRetryableError)
		metrics.ReplicationTaskSendAttempt.With(s.metrics).Record(
			attempt,
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
		)
		metrics.ReplicationTaskSendLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
		)
		if err != nil {
			metrics.ReplicationTaskSendError.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
				metrics.ReplicationTaskPriorityTag(priority),
			)
			// Retry exhaustion alone does not establish that a task is unbuildable.
			// Only explicitly identified task conversion failures may advance the watermark.
			if s.config.ReplicationStreamSenderSkipStuckTask() && isSkippable(err) {
				s.recordStuckTaskSkipped(item, attempt, priority, err)
				metrics.ReplicationTaskSendSkipped.With(s.metrics).Record(
					int64(1),
					metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
					metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
					metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
					metrics.ReplicationTaskPriorityTag(priority),
				)
				// Skip (discard) the stuck task and keep going; the trailing watermark send
				// below advances the receiver past it so the stream is not wedged.
				continue Loop
			}
			return fmt.Errorf("failed to send task: %v, cause: %w", item, err)
		}
	}
	return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           nil,
				ExclusiveHighWatermark:     endExclusiveWatermark,
				ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
				Priority:                   priority,
				LaneInfo:                   target.getLaneInfo(),
			},
		},
	})
}

func (s *laneStreamSender) sendToStream(payload *historyservice.StreamWorkflowReplicationMessagesResponse) error {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	err := s.server.Send(payload)
	if err != nil {
		return NewStreamError("Stream Sender unable to send", err)
	}
	return nil
}

func (s *laneStreamSender) shouldProcessTask(item tasks.Task) bool {
	clientShardID := common.WorkflowIDToHistoryShard(item.GetNamespaceID(), item.GetWorkflowID(), s.clientClusterShardCount)
	if clientShardID != s.clientShardKey.ShardID {
		return false
	}

	targetClusters := s.getTaskTargetCluster(item)
	if len(targetClusters) != 0 && !slices.Contains(targetClusters, s.clientClusterName) {
		return false
	}

	namespaceEntry, err := s.shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(item.GetNamespaceID()),
	)
	if err != nil {
		// if there is error, then blindly send the task, better safe than sorry
		return true
	}

	var shouldProcessTask bool
	if namespaceEntry != nil {
	FilterLoop:
		for _, targetCluster := range namespaceEntry.ClusterNames(item.GetWorkflowID()) {
			if s.clientClusterName == targetCluster {
				shouldProcessTask = s.admittedByGradualConnect(item, namespaceEntry)
				break FilterLoop
			}
		}
	}
	return shouldProcessTask
}

func (s *laneStreamSender) admittedByGradualConnect(item tasks.Task, namespaceEntry *namespace.Namespace) bool {
	if !s.config.EnableReplicationGradualConnect() {
		return true
	}

	// A shed delete can permanently resurrect history after force replication.
	if item.GetType() == enumsspb.TASK_TYPE_REPLICATION_DELETE_EXECUTION {
		return true
	}

	// Force-replication tasks follow the ramp; operators should clear the ramp before running force-replication.
	ramp := namespaceEntry.ReplicationConfig().GetClusterReplicationRamps()[s.clientClusterName]
	if ramp == nil {
		return true
	}
	percent := gradualConnectPercent(ramp, s.shardContext.GetTimeSource().Now())
	if percent >= 100 {
		return true
	}
	metricTags := []metrics.Tag{
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.TargetClusterTag(s.clientClusterName),
	}
	metrics.ReplicationGradualConnectPercent.With(s.metrics).Record(float64(percent), metricTags...)
	if dynamicconfig.RolloutAccepts([]byte(item.GetWorkflowID()), percent) {
		return true
	}
	metrics.ReplicationTasksShedByGradualConnect.With(s.metrics).Record(
		1,
		append(metricTags, metrics.OperationTag(TaskOperationTagFromTask(item.GetType())))...,
	)
	return false
}

func (s *laneStreamSender) getTaskPriority(task tasks.Task) enumsspb.TaskPriority {
	switch t := task.(type) {
	case *tasks.SyncWorkflowStateTask:
		if t.Priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
			return enumsspb.TASK_PRIORITY_LOW
		}
		return t.Priority
	case *tasks.SyncVersionedTransitionTask:
		return defaultHighTaskPriority(t.Priority)
	case *tasks.HistoryReplicationTask:
		return defaultHighTaskPriority(t.Priority)
	case *tasks.SyncActivityTask:
		return defaultHighTaskPriority(t.Priority)
	case *tasks.SyncHSMTask:
		return defaultHighTaskPriority(t.Priority)
	default:
		return enumsspb.TASK_PRIORITY_HIGH
	}
}

func (s *laneStreamSender) getTaskTargetCluster(task tasks.Task) []string {
	switch t := task.(type) {
	case *tasks.SyncWorkflowStateTask:
		return t.TargetClusters
	case *tasks.SyncVersionedTransitionTask:
		return t.TargetClusters
	case *tasks.SyncHSMTask:
		return t.TargetClusters
	case *tasks.HistoryReplicationTask:
		return t.TargetClusters
	case *tasks.SyncActivityTask:
		return t.TargetClusters
	default:
		return nil
	}
}

func (s *laneStreamSender) recordRetry(
	item tasks.Task,
	replicationTaskType enumsspb.ReplicationTaskType,
	priority enumsspb.TaskPriority,
	attempt int64,
	operation string,
	err error,
) error {
	s.shardContext.GetThrottledLogger().Warn("Replication task send retry",
		tag.TaskID(item.GetTaskID()),
		tag.WorkflowNamespaceID(item.GetNamespaceID()),
		tag.WorkflowID(item.GetWorkflowID()),
		tag.Counter(int(attempt)),
		tag.Error(err),
	)
	if s.config.EmitReplicationLifecycleEvents() {
		s.emitReplicationSenderError(item, replicationTaskType, priority, attempt, operation, "Replication task send retry", err)
	}
	return err
}

// recordStuckTaskSkipped logs (at error level, throttled) that a replication task could not
// be built after exhausting retries and is being skipped. The log identifies the workflow so
// an operator can remediate (e.g. tdbg task refresh / force replication) if the target needs
// the dropped state. Paired with the ReplicationTaskSendSkipped metric for alerting.
func (s *laneStreamSender) recordStuckTaskSkipped(
	item tasks.Task,
	attempt int64,
	priority enumsspb.TaskPriority,
	err error,
) {
	s.shardContext.GetThrottledLogger().Error("Replication task could not be built after exhausting retries, skipping task",
		tag.TaskID(item.GetTaskID()),
		tag.WorkflowNamespaceID(item.GetNamespaceID()),
		tag.WorkflowID(item.GetWorkflowID()),
		tag.WorkflowRunID(item.GetRunID()),
		tag.Counter(int(attempt)),
		tag.Error(err),
	)
	// Emit a terminal "skipped" ReplicationLifecycle wide event so the drop is traceable alongside
	// the task's sent/executing/applied events. Gated by the same config as the "sent" event.
	if s.config.EmitReplicationLifecycleEvents() {
		s.emitReplicationSkipped(item, attempt, priority, err)
	}
}

func (s *laneStreamSender) emitReplicationSenderError(
	item tasks.Task,
	replicationTaskType enumsspb.ReplicationTaskType,
	priority enumsspb.TaskPriority,
	attempt int64,
	operation string,
	message string,
	err error,
) {
	taskType, ok := stateBasedTaskTypeFromHistoryTask(item.GetType())
	if !ok {
		return
	}
	if convertedTaskType, ok := replicationLifecycleTaskType(replicationTaskType); ok {
		taskType = convertedTaskType
	}
	logger := s.shardContext.GetEventLogger()
	if logger == nil {
		return
	}
	namespaceID := item.GetNamespaceID()
	namespaceName := ""
	if name, nsErr := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(namespaceID)); nsErr == nil {
		namespaceName = name.String()
	}
	details := map[string]any{
		"attempt":        attempt,
		"priority":       priority.String(),
		"target_cluster": s.clientClusterName,
		"target_shard":   s.clientShardKey.ShardID,
	}
	wideevents.EmitReplicationError(
		logger,
		wideevents.ReplicationLifecyclePayload{
			TaskType:      taskType,
			Shard:         s.serverShardKey.ShardID,
			Namespace:     namespaceName,
			NamespaceID:   namespaceID,
			WorkflowID:    item.GetWorkflowID(),
			RunID:         item.GetRunID(),
			SourceCluster: s.shardContext.GetClusterMetadata().GetCurrentClusterName(),
			SourceShard:   s.serverShardKey.ShardID,
			SourceTaskID:  item.GetTaskID(),
		},
		operation,
		message,
		err,
		details,
	)
}

// emitReplicationSent emits a best-effort "sent" ReplicationLifecycle event for the supported
// replication task types. It never affects control flow.
func (s *laneStreamSender) emitReplicationSent(
	task *replicationspb.ReplicationTask,
	item tasks.Task,
) {
	logger := s.shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	var taskType string
	switch task.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		taskType = wideevents.ReplTaskSyncWorkflowState
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK:
		taskType = wideevents.ReplTaskSyncVersionedTransition
	case enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK:
		taskType = wideevents.ReplTaskVerifyVersionedTransition
	case enumsspb.REPLICATION_TASK_TYPE_DELETE_EXECUTION_TASK:
		// Carries no state or events, so only the identity below applies.
		taskType = wideevents.ReplTaskDeleteExecution
	default:
		return
	}

	nsID := item.GetNamespaceID()
	nsName, err := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if err != nil {
		nsName = namespace.EmptyName
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:         wideevents.ReplicationSent,
		TaskType:      taskType,
		Shard:         s.serverShardKey.ShardID,
		Namespace:     nsName.String(),
		NamespaceID:   nsID,
		WorkflowID:    item.GetWorkflowID(),
		RunID:         item.GetRunID(),
		SourceCluster: s.shardContext.GetClusterMetadata().GetCurrentClusterName(),
		SourceShard:   s.serverShardKey.ShardID,
		SourceTaskID:  item.GetTaskID(),
		TargetCluster: s.clientClusterName,
		TargetShard:   s.clientShardKey.ShardID,
		Priority:      task.GetPriority().String(),
	}
	if vt := task.GetVersionedTransition(); vt != nil {
		payload.FailoverVersion = vt.GetNamespaceFailoverVersion()
		payload.TransitionCount = vt.GetTransitionCount()
	}

	if attr := task.GetVerifyVersionedTransitionTaskAttributes(); attr != nil {
		// verify ships no history batch, so use the task's target event id as next_event_id.
		payload.NextEventID = attr.GetNextEventId()
		payload.NewRunID = attr.GetNewRunId()
	}

	if rawTaskInfo := task.GetRawTaskInfo(); rawTaskInfo != nil {
		payload.FirstEventID = rawTaskInfo.GetFirstEventId()
		payload.NextEventID = rawTaskInfo.GetNextEventId()
	} else if svtTask, ok := item.(*tasks.SyncVersionedTransitionTask); ok {
		payload.FirstEventID = svtTask.FirstEventID
		payload.NextEventID = svtTask.NextEventID
	}
	if attr := task.GetSyncVersionedTransitionTaskAttributes(); attr != nil {
		payload.IsFirstSync = attr.GetVersionedTransitionArtifact().GetIsFirstSync()
	}

	// artifact detail, extracted from the mutable state carried in the task payload: the child->parent
	// identity plus what the task actually carried. The verify task ships no mutable state, so it
	// contributes nothing here.
	populateSentArtifactInfo(&payload, task, s.shardContext.GetPayloadSerializer(), s.logger)

	wideevents.Emit(logger, payload)
}

// emitReplicationSkipped emits a best-effort terminal "skipped" ReplicationLifecycle event when the
// sender gives up building ("converting") a task and skips it. It mirrors emitReplicationSent but is
// driven by the raw source task: conversion failed, so there is no built replication task to read
// task-type/versioned-transition/parent detail from. It records the source task type, the
// source_cluster/source_shard/source_task_id join key, the target_cluster now missing the state, the
// priority tier, the exhausted attempt count, and the last convert error. It never affects control
// flow.
func (s *laneStreamSender) emitReplicationSkipped(
	item tasks.Task,
	attempt int64,
	priority enumsspb.TaskPriority,
	err error,
) {
	logger := s.shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	nsID := item.GetNamespaceID()
	nsName, nsErr := s.shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(nsID))
	if nsErr != nil {
		nsName = namespace.EmptyName
	}

	payload := wideevents.ReplicationLifecyclePayload{
		Phase:    wideevents.ReplicationSkipped,
		TaskType: replicationTaskTypeName(item),
		Shard:    s.serverShardKey.ShardID,
		// SourceCluster/SourceShard/SourceTaskID are the join key back to this cluster's replication
		// queue. TargetCluster identifies the passive this stream feeds; it is recorded because a
		// skipped task emits no downstream event there, so this is the only place the destination is
		// captured — it tells an operator which passive is now missing this task's state.
		SourceCluster: s.shardContext.GetClusterMetadata().GetCurrentClusterName(),
		SourceShard:   s.serverShardKey.ShardID,
		SourceTaskID:  item.GetTaskID(),
		TargetCluster: s.clientClusterName,
		TargetShard:   s.clientShardKey.ShardID,
		Priority:      priority.String(),
		Namespace:     nsName.String(),
		NamespaceID:   nsID,
		WorkflowID:    item.GetWorkflowID(),
		RunID:         item.GetRunID(),
		Attempt:       int32(attempt),
	}
	if err != nil {
		payload.Error = err.Error()
	}
	wideevents.Emit(logger, payload)
}
