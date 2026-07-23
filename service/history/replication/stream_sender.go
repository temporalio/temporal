//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_sender_mock.go

package replication

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
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
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	TaskMaxSkipCount = 1000
)

type (
	StreamSender interface {
		IsValid() bool
		Key() ClusterShardKeyPair
		Stop()
	}
	StreamSenderImpl struct {
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
		laneManager             *laneManager
		// isolationConfirmed is set once the receiver advertises support for grouped-cursor
		// isolation. Until then the sender keeps all LOW traffic on the default lane and
		// emits no tier-tagged messages, so an old receiver is never sent lanes it would
		// misroute.
		isolationConfirmed atomic.Bool
		flowController     SenderFlowController
		sendLock           sync.Mutex
		ssRateLimiter      ServerSchedulerRateLimiter
		tierRateLimiters   []quotas.RateLimiter // index t-1 limits throttled tier t
	}
)

func NewStreamSender(
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
) *StreamSenderImpl {
	logger := log.With(
		shardContext.GetLogger(),
		tag.TargetCluster(clientClusterName), // client is the target cluster (passive cluster)
		tag.TargetShardID(clientShardKey.ShardID),
		tag.ShardID(serverShardKey.ShardID), // server is the source cluster (active cluster)
		tag.Operation("replication-stream-sender"),
	)
	return &StreamSenderImpl{
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
		isTieredStackEnabled:    config.EnableReplicationTaskTieredProcessing(),
		readerGroup:             newReaderGroupIfEnabled(config, shardContext, clientShardKey),
		laneManager:             newLaneManagerIfEnabled(config, shardContext, clientShardKey),
		flowController:          NewSenderFlowController(config, logger),
		ssRateLimiter:           ssRateLimiter,
		tierRateLimiters:        newTierRateLimiters(config),
	}
}

func (s *StreamSenderImpl) Start() {
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
		if s.laneManager != nil {
			// One send loop per throttled tier; each drains its (rate-limited) members.
			for tier := 1; tier <= s.laneManager.TierCount(); tier++ {
				go WrapEventLoop(s.server.Context(), func() error { return s.sendTierEventLoop(tier) }, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
			}
		}
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

func (s *StreamSenderImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	s.shutdownChan.Shutdown()
	s.logger.Info("StreamSender stopped.")
}

func (s *StreamSenderImpl) IsValid() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStarted
}

func (s *StreamSenderImpl) Wait() {
	<-s.shutdownChan.Channel()
}

func (s *StreamSenderImpl) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: s.clientShardKey,
		Server: s.serverShardKey,
	}
}

func (s *StreamSenderImpl) recvEventLoop() (retErr error) {
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
		if (s.laneManager != nil) != (s.config.EnableReplicationNamespaceIsolation() && s.isTieredStackEnabled) {
			return NewStreamError("StreamSender detected namespace isolation config change, restart the stream", nil)
		}
		if s.laneManager != nil && s.laneManager.TierCount() != s.config.ReplicationStreamSenderThrottledLaneCount() {
			return NewStreamError("StreamSender detected throttled lane count change, restart the stream", nil)
		}

		req, err := s.server.Recv()
		if err != nil {
			return NewStreamError("StreamSender failed to receive", err)
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

		select {
		case s.recvSignalChan <- struct{}{}:
		default:
			// signal channel is full. Continue
		}
	}
	return nil
}

func (s *StreamSenderImpl) sendEventLoop(priority enumsspb.TaskPriority) (retErr error) {
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
	if err = s.sendLive(
		priority,
		newTaskNotificationChan,
		catchupEndExclusiveWatermark,
	); err != nil {
		return fmt.Errorf("streamSender unable to stream replication tasks: %w", err)
	}
	return nil
}

func (s *StreamSenderImpl) recvSyncReplicationState(
	attr *replicationspb.SyncReplicationState,
) error {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	if s.isTieredStackEnabled {
		s.flowController.RefreshReceiverFlowControlInfo(attr)
	}
	if attr.GetSupportsNamespaceIsolation() {
		s.isolationConfirmed.Store(true)
	}

	if s.laneManager != nil {
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return NewStreamError("streamSender: missing priority state with namespace isolation", nil)
		}
		// Reconcile HIGH-lane tier membership against the receiver's throttled set, then
		// persist 3+K scopes — tier scopes carry their NamespacePredicate + acked
		// watermark, so a reconnect reconstructs tiers instead of re-sending the whole
		// HIGH lane. Split/demote floors and the merge-back gate anchor on applied
		// (acked) watermarks so ordered HIGH events never straddle two lanes with a gap.
		// Scopes[0] = the receiver's overall min, so cleanup stays safe.
		s.laneManager.Reconcile(
			attr.GetPauseNamespaceIds(),
			attr.GetHighPriorityState().GetInclusiveLowWatermark(),
			tierAckedWatermarks(attr.GetThrottledLaneStates(), s.laneManager.TierCount()),
		)
		s.emitTierMetrics()
		if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, s.buildTieredIsolationReaderState(attr)); err != nil {
			return err
		}
		// Failover uses the HIGH lane (live traffic), same as the tiered stack.
		return s.shardContext.UpdateRemoteReaderInfo(
			readerID,
			attr.HighPriorityState.InclusiveLowWatermark-1,
			attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime(),
		)
	}

	if s.readerGroup != nil {
		readerState, err := s.readerGroup.BuildReaderState(attr)
		if err != nil {
			return err
		}
		if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, readerState); err != nil {
			return err
		}
		taskID, ts := s.readerGroup.FailoverWatermark(attr)
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
	}

	inclusiveLowWatermark := attr.GetInclusiveLowWatermark()
	inclusiveLowWatermarkTime := attr.GetInclusiveLowWatermarkTime()

	if err := s.shardContext.UpdateReplicationQueueReaderState(
		readerID,
		readerState,
	); err != nil {
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

func (s *StreamSenderImpl) sendCatchUp(priority enumsspb.TaskPriority) (int64, error) {
	catchupEndExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID

	catchupBeginInclusiveWatermark := s.catchupBeginWatermark(priority, catchupEndExclusiveWatermark)
	if err := s.sendTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
		s.defaultLaneFilter(priority),
		0, // default HIGH / LOW lane: not a throttled tier
	); err != nil {
		return 0, err
	}
	if s.laneManager != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
		s.laneManager.AdvanceDefaultCursor(catchupEndExclusiveWatermark)
	}
	return catchupEndExclusiveWatermark, nil
}

// catchupBeginWatermark returns the inclusive begin watermark for the catch-up scan.
// With isolation ON the lane manager reconstructs throttled tiers, so the default HIGH
// lane resumes normally from its own (HIGH) scope. Only when isolation is OFF but
// leftover tier scopes are persisted (isolation was disabled while a tier lagged) does
// default-HIGH resume from the overall low watermark (scope 0) to re-cover the undrained
// windows once — a rare, bounded, idempotent re-send.
func (s *StreamSenderImpl) catchupBeginWatermark(priority enumsspb.TaskPriority, end int64) int64 {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	queueState, ok := s.shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		s.logger.Debug("StreamSender queueState not found")
		return end
	}
	readerState, ok := queueState.ReaderStates[readerID]
	if !ok {
		s.logger.Debug(fmt.Sprintf("StreamSender readerState not found, readerID %v", readerID))
		return end
	}
	scopePriority := priority
	// Isolation OFF but leftover tier scopes persisted (isolation disabled while a tier
	// lagged): resume default-LOW from the overall min (scope 0) so the undrained tier
	// windows are re-covered once. With isolation ON the tiers are reconstructed, so
	// default-LOW resumes normally from its own (LOW) scope.
	if s.laneManager == nil && priority == enumsspb.TASK_PRIORITY_HIGH && len(readerState.Scopes) > 3 {
		scopePriority = enumsspb.TASK_PRIORITY_UNSPECIFIED
	}
	return s.getSendCatchupBeginInclusiveWatermark(readerState, scopePriority)
}

func (s *StreamSenderImpl) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	// priorityScopeIndex tolerates the single-stack format (1 scope -> index 0) and the
	// tiered format (3+ scopes -> HIGH=1, LOW=2). When switching from single to tiered
	// stack the reader state is still in the old format, in which case using the overall
	// low watermark (scope 0) is safe as long as we always guarantee the overall low
	// watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark).
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes))].Range.InclusiveMin.TaskId
}

func (s *StreamSenderImpl) sendLive(
	priority enumsspb.TaskPriority,
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	syncStatusTimer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer syncStatusTimer.Stop()
	sendTasks := func() error {
		endExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		if err := s.sendTasks(
			priority,
			beginInclusiveWatermark,
			endExclusiveWatermark,
			s.defaultLaneFilter(priority), // re-snapshot per batch: tier membership changes
			0,
		); err != nil {
			return err
		}
		beginInclusiveWatermark = endExclusiveWatermark
		if s.laneManager != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
			s.laneManager.AdvanceDefaultCursor(endExclusiveWatermark)
		}
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

func (s *StreamSenderImpl) sendTasks(
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
	nsFilter func(namespaceID string, taskID int64) bool,
	laneID int32,
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
					ThrottledLaneId:            laneID,
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
			if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ExclusiveHighWatermark:     item.GetTaskID(),
						ExclusiveHighWatermarkTime: timestamppb.New(item.GetVisibilityTime()),
						Priority:                   priority,
						ThrottledLaneId:            laneID,
					},
				},
			}); err != nil {
				return err
			}
			skipCount = 0
		}
		if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED && // case: skip priority check. When priority is unspecified, send all tasks
			priority != s.getTaskPriority(item) { // case: skip task with different priority than this loop
			continue Loop
		}
		if nsFilter != nil && !nsFilter(item.GetNamespaceID(), item.GetTaskID()) {
			continue Loop
		}
		if !s.shouldProcessTask(item) {
			continue Loop
		}
		metrics.ReplicationTaskLoadLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
			metrics.ReplicationStreamLaneTag(laneTagValue(laneID)),
		)

		var attempt int64
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
			task, err := s.taskConverter.Convert(item, s.clientShardKey.ClusterID, priority)
			if err != nil {
				return s.recordRetry(item, attempt, fmt.Errorf("convert: %w", err))
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
				if err := s.ssRateLimiter.Wait(s.server.Context(), quotas.NewRequest(
					task.TaskType.String(),
					taskSchedulerToken,
					nsName.String(),
					headers.SystemPreemptableCallerInfo.CallerType,
					0,
					"",
				)); err != nil {
					return s.recordRetry(item, attempt, fmt.Errorf("rate_limit: %w", err))
				}
				metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)), metrics.ReplicationStreamLaneTag(laneTagValue(laneID)))
			}
			if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ReplicationTasks:           []*replicationspb.ReplicationTask{task},
						ExclusiveHighWatermark:     task.SourceTaskId + 1,
						ExclusiveHighWatermarkTime: task.VisibilityTime,
						Priority:                   priority,
						ThrottledLaneId:            laneID,
					},
				},
			}); err != nil {
				return s.recordRetry(item, attempt, fmt.Errorf("send: %w", err))
			}
			skipCount = 0
			metrics.ReplicationTasksSend.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTag(task)),
				metrics.ReplicationTaskPriorityTag(priority),
				metrics.ReplicationStreamLaneTag(laneTagValue(laneID)),
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
			metrics.ReplicationStreamLaneTag(laneTagValue(laneID)),
		)
		metrics.ReplicationTaskSendLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
			metrics.ReplicationStreamLaneTag(laneTagValue(laneID)),
		)
		if err != nil {
			metrics.ReplicationTaskSendError.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
				metrics.ReplicationTaskPriorityTag(priority),
				metrics.ReplicationStreamLaneTag(laneTagValue(laneID)),
			)
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
				ThrottledLaneId:            laneID,
			},
		},
	})
}

func (s *StreamSenderImpl) sendToStream(payload *historyservice.StreamWorkflowReplicationMessagesResponse) error {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	err := s.server.Send(payload)
	if err != nil {
		return NewStreamError("Stream Sender unable to send", err)
	}
	return nil
}

func (s *StreamSenderImpl) shouldProcessTask(item tasks.Task) bool {
	clientShardID := common.WorkflowIDToHistoryShard(item.GetNamespaceID(), item.GetWorkflowID(), s.clientClusterShardCount)
	if clientShardID != s.clientShardKey.ShardID {
		return false
	}

	targetClusters := s.getTaskTargetCluster(item)
	if len(targetClusters) != 0 && !slices.Contains(targetClusters, s.clientClusterName) {
		return false
	}

	var shouldProcessTask bool
	namespaceEntry, err := s.shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(item.GetNamespaceID()),
	)
	if err != nil {
		// if there is error, then blindly send the task, better safe than sorry
		shouldProcessTask = true
	}

	if namespaceEntry != nil {
	FilterLoop:
		for _, targetCluster := range namespaceEntry.ClusterNames(item.GetWorkflowID()) {
			if s.clientClusterName == targetCluster {
				shouldProcessTask = true
				break FilterLoop
			}
		}
	}

	return shouldProcessTask
}

// tierLaneTagValue is the replicationStreamLane metric value for a throttled tier.
func tierLaneTagValue(tier int) string {
	return "tier-" + strconv.Itoa(tier)
}

// emitTierMetrics reports how many namespaces are isolated in each throttled tier, so
// the throttled lane count can be sized and isolation usage observed.
func (s *StreamSenderImpl) emitTierMetrics() {
	for tier := 1; tier <= s.laneManager.TierCount(); tier++ {
		metrics.ReplicationStreamSenderThrottledNamespaceCount.With(s.metrics).Record(
			float64(len(s.laneManager.TierMembers(tier))),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.ReplicationStreamLaneTag(tierLaneTagValue(tier)),
		)
	}
}

// laneTagValue is the replicationStreamLane metric value for a throttled lane id:
// "tier-N" for a throttled tier, "default" otherwise. It is orthogonal to the task's
// priority (HIGH/LOW), which is tagged separately: (priority, lane) together identify
// the physical lane — (HIGH, default), (LOW, default), or (LOW, tier-N).
func laneTagValue(laneID int32) string {
	if laneID > 0 {
		return tierLaneTagValue(int(laneID))
	}
	return "default"
}

func (s *StreamSenderImpl) getTaskPriority(task tasks.Task) enumsspb.TaskPriority {
	switch t := task.(type) {
	case *tasks.SyncWorkflowStateTask:
		if t.Priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
			return enumsspb.TASK_PRIORITY_LOW
		}
		return t.Priority
	default:
		return enumsspb.TASK_PRIORITY_HIGH
	}
}

func (s *StreamSenderImpl) getTaskTargetCluster(task tasks.Task) []string {
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

func newReaderGroupIfEnabled(
	config *configs.Config,
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
) *replicationReaderGroup {
	if !config.EnableReplicationReaderGroup() {
		return nil
	}
	return newReplicationReaderGroup(shardContext, clientShardKey, config.EnableReplicationTaskTieredProcessing())
}

// newLaneManagerIfEnabled builds the LOW-lane severity-tier manager when namespace
// isolation is enabled. Isolation only exists in the tiered stack (the LOW lane only
// exists there), so both flags are required. On a stream (re)start it reconstructs
// tier membership and cursors from the persisted reader state, so throttled
// namespaces stay isolated across reconnects (e.g. history deploys) instead of
// bursting back onto the default lane.
func newLaneManagerIfEnabled(config *configs.Config, shardContext historyi.ShardContext, clientShardKey ClusterShardKey) *laneManager {
	if !config.EnableReplicationNamespaceIsolation() || !config.EnableReplicationTaskTieredProcessing() {
		return nil
	}
	tierCount := config.ReplicationStreamSenderThrottledLaneCount()
	defaultCursor, tiers := reconstructLaneState(shardContext, clientShardKey, tierCount)
	return newLaneManagerWithState(
		tierCount,
		config.ReplicationStreamSenderTierDemotionCycles(),
		config.ReplicationStreamSenderUnthrottleCooldownCycles(),
		defaultCursor,
		tiers,
	)
}

// reconstructLaneState reads the persisted replication reader state and extracts the
// default-HIGH resume cursor and per-tier membership/cursors. Tier scope t lives at
// scope index 2+t (scope 0 = overall min, 1 = default-HIGH, 2 = LOW). Returns zero
// state when nothing is persisted (fresh shard).
func reconstructLaneState(shardContext historyi.ShardContext, clientShardKey ClusterShardKey, tierCount int) (int64, []reconstructedTier) {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(clientShardKey.ClusterID),
		clientShardKey.ShardID,
	)
	queueState, ok := shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		return 0, nil
	}
	readerState, ok := queueState.ReaderStates[readerID]
	if !ok {
		return 0, nil
	}
	return parseLaneState(readerState, tierCount)
}

// parseLaneState extracts the default-HIGH resume cursor and per-tier membership from a
// persisted reader state (scope index 2+t = tier t). Pure; the inverse of
// buildTieredIsolationReaderState.
func parseLaneState(readerState *persistencespb.QueueReaderState, tierCount int) (int64, []reconstructedTier) {
	if len(readerState.Scopes) < 3 {
		return 0, nil
	}
	defaultCursor := readerState.Scopes[1].GetRange().GetInclusiveMin().GetTaskId()
	var tiers []reconstructedTier
	for i := 3; i < len(readerState.Scopes); i++ {
		tier := i - 2
		if tier > tierCount {
			break
		}
		scope := readerState.Scopes[i]
		members := scope.GetPredicate().GetNamespaceIdPredicateAttributes().GetNamespaceIds()
		if len(members) == 0 {
			continue
		}
		tiers = append(tiers, reconstructedTier{
			tier:    tier,
			cursor:  scope.GetRange().GetInclusiveMin().GetTaskId(),
			members: members,
		})
	}
	return defaultCursor, tiers
}

// buildTieredIsolationReaderState builds the persisted reader state for the HIGH-lane
// isolation layout: scope 0 = overall min, 1 = default-HIGH (predicate excludes
// throttled namespaces), 2 = LOW (single lane, universal), 2+t = throttled HIGH tier t
// (predicate = its members, watermark from the receiver's throttled_lane_states). All K
// tier scopes are written positionally so reconstruction maps scope index 2+t back to
// tier t.
func (s *StreamSenderImpl) buildTieredIsolationReaderState(attr *replicationspb.SyncReplicationState) *persistencespb.QueueReaderState {
	universal := func() *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
			Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
		}
	}
	namespacePredicate := func(ids []string) *persistencespb.Predicate {
		return &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
			Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
				NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{NamespaceIds: ids},
			},
		}
	}
	makeScope := func(watermark int64, predicate *persistencespb.Predicate) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(watermark)),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
			},
			Predicate: predicate,
		}
	}

	throttled := s.laneManager.AllThrottledNamespaces()
	defaultHighPredicate := universal()
	if len(throttled) > 0 {
		defaultHighPredicate = &persistencespb.Predicate{
			PredicateType: enumsspb.PREDICATE_TYPE_NOT,
			Attributes: &persistencespb.Predicate_NotPredicateAttributes{
				NotPredicateAttributes: &persistencespb.NotPredicateAttributes{Predicate: namespacePredicate(throttled)},
			},
		}
	}
	scopes := []*persistencespb.QueueSliceScope{
		makeScope(attr.GetInclusiveLowWatermark(), universal()),
		makeScope(attr.GetHighPriorityState().GetInclusiveLowWatermark(), defaultHighPredicate),
		makeScope(attr.GetLowPriorityState().GetInclusiveLowWatermark(), universal()),
	}
	laneStates := attr.GetThrottledLaneStates()
	for tier := 1; tier <= s.laneManager.TierCount(); tier++ {
		watermark := attr.GetInclusiveLowWatermark()
		if tier-1 < len(laneStates) {
			watermark = laneStates[tier-1].GetInclusiveLowWatermark()
		}
		scopes = append(scopes, makeScope(watermark, namespacePredicate(s.laneManager.TierMembers(tier))))
	}
	return &persistencespb.QueueReaderState{Scopes: scopes}
}

// newTierRateLimiters builds one outgoing rate limiter per throttled tier; deeper
// tiers run slower at LowPriorityQPS * CatchupQPSRatio^tier. Returns nil when
// isolation is off.
func newTierRateLimiters(config *configs.Config) []quotas.RateLimiter {
	if !config.EnableReplicationNamespaceIsolation() || !config.EnableReplicationTaskTieredProcessing() {
		return nil
	}
	count := config.ReplicationStreamSenderThrottledLaneCount()
	limiters := make([]quotas.RateLimiter, count)
	for i := range limiters {
		depth := i + 1 // tier number (1-based)
		limiters[i] = quotas.NewDefaultOutgoingRateLimiter(func() float64 {
			ratio := math.Pow(config.ReplicationStreamSenderCatchupQPSRatio(), float64(depth))
			return float64(config.ReplicationStreamSenderLowPriorityQPS()) * ratio
		})
	}
	return limiters
}

// defaultLaneFilter returns the namespace filter for the default HIGH lane: it excludes
// namespaces currently owned by a throttled tier. Returns nil (admit all) when
// isolation is disabled or for non-HIGH priorities.
func (s *StreamSenderImpl) defaultLaneFilter(priority enumsspb.TaskPriority) func(string, int64) bool {
	// Until the receiver confirms isolation support, admit everything on the default
	// lane (no exclusion, no tier traffic) — otherwise an old receiver would never
	// receive the excluded namespaces' tasks.
	if s.laneManager == nil || priority != enumsspb.TASK_PRIORITY_HIGH || !s.isolationConfirmed.Load() {
		return nil
	}
	_, filter := s.laneManager.DefaultBatchStart()
	return filter
}

// tierAckedWatermarks extracts the per-tier acked watermarks (positional, 1-based) the
// receiver reports in throttled_lane_states, padded to tierCount. These are the applied
// points the lane manager anchors demote floors and merge-back gates on.
func tierAckedWatermarks(laneStates []*replicationspb.ReplicationState, tierCount int) []int64 {
	out := make([]int64, tierCount)
	for i := range out {
		if i < len(laneStates) {
			out[i] = laneStates[i].GetInclusiveLowWatermark()
		}
	}
	return out
}

// sendTierEventLoop runs the rate-limited send loop for one throttled HIGH tier
// (1..K). It streams only the tier's current members, each at or above its floor,
// tagging messages with the tier's throttled_lane_id. The loop runs for the life of the
// stream; an empty tier idles. The lane manager's tier cursor is advanced via
// compare-and-swap, so a concurrent Reconcile rewind is honored by re-reading.
func (s *StreamSenderImpl) sendTierEventLoop(tier int) (retErr error) {
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

	rateLimiter := s.tierRateLimiters[tier-1]
	timer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer timer.Stop()

	for {
		cursor, nsFilter := s.laneManager.TierBatchStart(tier)
		end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		// Only emit tier-tagged traffic once the receiver has confirmed it routes lanes.
		if cursor < end && s.isolationConfirmed.Load() {
			if err := rateLimiter.Wait(s.server.Context()); err != nil {
				return nil // context cancelled or shutdown
			}
			if err := s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, cursor, end, nsFilter, int32(tier)); err != nil {
				return err
			}
			// Commit progress; if Reconcile rewound the cursor meanwhile, redo from
			// the new (lower) cursor rather than skipping the rewound window.
			if !s.laneManager.AdvanceTierCursor(tier, cursor, end) {
				continue
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(s.config.ReplicationStreamSendEmptyTaskDuration())

		select {
		case <-s.shutdownChan.Channel():
			return nil
		case <-newTaskNotificationChan:
		case <-timer.C:
		}
	}
}

func (s *StreamSenderImpl) recordRetry(
	item tasks.Task,
	attempt int64,
	err error,
) error {
	s.shardContext.GetThrottledLogger().Warn("Replication task send retry",
		tag.TaskID(item.GetTaskID()),
		tag.WorkflowNamespaceID(item.GetNamespaceID()),
		tag.WorkflowID(item.GetWorkflowID()),
		tag.Counter(int(attempt)),
		tag.Error(err),
	)
	return err
}
