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
		laneController          *senderLaneController
		laneRegistry            *senderLaneRegistry
		laneInitializationError error
		// laneCapabilityKnown is closed after the first sync state has been
		// applied to the registry; lanesConfirmed is valid only after that.
		laneCapabilityKnown       chan struct{}
		laneCapabilityOnce        sync.Once
		laneCapabilityPublishOnce sync.Once
		lanesConfirmed            atomic.Bool
		laneRateLimiters          []quotas.RateLimiter
		flowController            SenderFlowController
		sendLock                  sync.Mutex
		ssRateLimiter             ServerSchedulerRateLimiter
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
	// Read the dynamic config once so every derived field sees the same snapshot: a
	// flip between two reads would leave the sender half in each mode until the recv
	// loop's config guard restarts the stream.
	tieredStackEnabled := config.EnableReplicationTaskTieredProcessing()
	readerGroup := newReaderGroupIfEnabled(config.EnableReplicationReaderGroup, shardContext, clientShardKey, tieredStackEnabled, logger)
	lanesEnabled := tieredStackEnabled && readerGroup != nil && config.EnableReplicationStreamLanes()
	var laneClassCount int
	var laneCapabilityKnown chan struct{}
	if lanesEnabled {
		laneClassCount = normalizedLaneClassCount(config.ReplicationStreamSenderLaneClassCount())
		laneCapabilityKnown = make(chan struct{})
	}
	laneRegistry, laneController, laneInitializationError := newSenderLaneComponentsIfEnabled(
		config,
		shardContext,
		clientShardKey,
		lanesEnabled,
		laneClassCount,
		logger,
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
		isTieredStackEnabled:    tieredStackEnabled,
		readerGroup:             readerGroup,
		laneController:          laneController,
		laneRegistry:            laneRegistry,
		laneInitializationError: laneInitializationError,
		laneCapabilityKnown:     laneCapabilityKnown,
		laneRateLimiters:        newLaneRateLimiters(config, lanesEnabled, laneClassCount),
		flowController:          NewSenderFlowController(config, logger),
		ssRateLimiter:           ssRateLimiter,
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
		if s.laneController != nil {
			for class := 1; class <= s.laneController.policy.ClassCount(); class++ {
				go WrapEventLoop(s.server.Context(), func() error { return s.sendLaneEventLoop(replicationLaneClass(class)) }, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, s.config)
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
	if s.laneInitializationError != nil {
		return NewStreamError("StreamSender failed to restore replication lanes", s.laneInitializationError)
	}

	for !s.shutdownChan.IsShutdown() {
		if s.isTieredStackEnabled != s.config.EnableReplicationTaskTieredProcessing() {
			return NewStreamError("StreamSender detected tiered stack change, restart the stream", nil)
		}
		if (s.readerGroup != nil) != s.config.EnableReplicationReaderGroup() {
			return NewStreamError("StreamSender detected reader group config change, restart the stream", nil)
		}
		lanesEnabled := s.isTieredStackEnabled && s.readerGroup != nil && s.config.EnableReplicationStreamLanes()
		if (s.laneController != nil) != lanesEnabled {
			return NewStreamError("StreamSender detected replication lane config change, restart the stream", nil)
		}
		if s.laneController != nil && s.laneController.policy.ClassCount() != normalizedLaneClassCount(s.config.ReplicationStreamSenderLaneClassCount()) {
			return NewStreamError("StreamSender detected replication lane class count change, restart the stream", nil)
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
	if s.laneInitializationError != nil {
		return NewStreamError("StreamSender failed to restore replication lanes", s.laneInitializationError)
	}

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
	if s.laneController != nil {
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return NewStreamError("streamSender: missing priority state with replication lanes", nil)
		}
		if err := s.observeLaneCapability(attr); err != nil {
			return err
		}
		s.flowController.RefreshReceiverFlowControlInfo(attr)
		highAcked := attr.GetHighPriorityState().GetInclusiveLowWatermark()
		if s.lanesConfirmed.Load() {
			if err := s.laneController.Reconcile(
				replicationLanePolicySignals{
					throttleHighNamespaceIDs: attr.GetThrottleHighNamespaceIds(),
					sharedHighWatermark:      highAcked,
				},
				attr.GetLaneStates(),
			); err != nil {
				return err
			}
			if err := s.sendReadyLaneRetirements(); err != nil {
				return err
			}
		} else {
			s.laneRegistry.ClearLanes()
		}
		s.publishLaneCapability()
		s.emitLaneMetrics()
		if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, s.laneRegistry.BuildReaderState(attr)); err != nil {
			return err
		}
		taskID, ts := s.laneFailoverWatermark(attr)
		return s.shardContext.UpdateRemoteReaderInfo(readerID, taskID, ts)
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
	}

	inclusiveLowWatermark := attr.GetInclusiveLowWatermark()
	inclusiveLowWatermarkTime := attr.GetInclusiveLowWatermarkTime()

	if s.isTieredStackEnabled {
		s.flowController.RefreshReceiverFlowControlInfo(attr)
	}

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
	if s.laneRegistry != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
		// Snapshot the lane resume floor before the capability wait: the recv loop
		// drops lane state once the receiver proves it does not understand lanes.
		resumeFloor, _, hasLanes := s.laneRegistry.ResumeFloor()
		// The receiver emits no sync state until both its priority trackers have
		// observed a batch, and the lane capability handshake rides on the first
		// sync state. Prime the receiver's HIGH tracker with an empty batch, or
		// the handshake and this loop wait on each other forever. The watermark
		// must not exceed the catch-up begin: the receiver drops batches whose
		// watermark does not advance.
		primeWatermark := catchupBeginInclusiveWatermark
		if hasLanes {
			primeWatermark = min(primeWatermark, resumeFloor)
		}
		if err := s.sendTasks(priority, primeWatermark, primeWatermark); err != nil {
			return 0, err
		}
		if err := s.waitForLaneCapability(); err != nil {
			return 0, err
		}
		if !s.lanesConfirmed.Load() && hasLanes {
			// The receiver does not understand lanes: re-cover their ranges on the
			// default lane from the resume floor.
			catchupBeginInclusiveWatermark = min(catchupBeginInclusiveWatermark, resumeFloor)
		}
	}
	sent, err := s.sendDefaultTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	)
	if err != nil {
		return 0, err
	}
	if !sent {
		return 0, serviceerror.NewInternal("replication default lane blocked during catch-up")
	}
	return catchupEndExclusiveWatermark, nil
}

func (s *StreamSenderImpl) observeLaneCapability(attr *replicationspb.SyncReplicationState) error {
	supported := attr.GetSupportsReplicationLanes() && attr.GetReplicationLaneProtocolVersion() >= 1
	s.laneCapabilityOnce.Do(func() {
		s.lanesConfirmed.Store(supported)
	})
	if s.lanesConfirmed.Load() != supported {
		return NewStreamError("StreamSender detected replication lane capability change", nil)
	}
	return nil
}

func (s *StreamSenderImpl) publishLaneCapability() {
	s.laneCapabilityPublishOnce.Do(func() {
		close(s.laneCapabilityKnown)
	})
}

func (s *StreamSenderImpl) waitForLaneCapability() error {
	select {
	case <-s.laneCapabilityKnown:
		return nil
	case <-s.shutdownChan.Channel():
		return context.Canceled
	case <-s.server.Context().Done():
		return s.server.Context().Err()
	}
}

// catchupBeginWatermark returns the inclusive begin watermark for the catch-up scan:
// the persisted reader-state cursor for the given priority, falling back to the
// current end watermark when no state is persisted for this reader.
func (s *StreamSenderImpl) catchupBeginWatermark(priority enumsspb.TaskPriority, end int64) int64 {
	if s.readerGroup != nil {
		watermark := s.readerGroup.CatchupBeginWatermark(end, priority)
		if s.laneRegistry == nil && priority == enumsspb.TASK_PRIORITY_HIGH {
			if queueState, ok := s.shardContext.GetQueueState(tasks.CategoryReplication); ok {
				if readerState, ok := queueState.ReaderStates[s.readerGroup.ReaderID()]; ok && len(readerState.GetLanes()) > 0 {
					return readerState.Scopes[0].Range.InclusiveMin.TaskId
				}
			}
		}
		return watermark
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

func (s *StreamSenderImpl) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	// priorityScopeIndex tolerates the single-stack format (1 scope -> index 0) and the
	// tiered format (3 scopes -> HIGH=1, LOW=2). When switching from single to tiered
	// stack the reader state is still in the old format, in which case using the overall
	// low watermark (scope 0) is safe as long as we always guarantee the overall low
	// watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark).
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes), s.readerGroup != nil)].Range.InclusiveMin.TaskId
}

func newReaderGroupIfEnabled(
	enableReplicationReaderGroup dynamicconfig.BoolPropertyFn,
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
	tieredStackEnabled bool,
	logger log.Logger,
) *replicationReaderGroup {
	if !enableReplicationReaderGroup() {
		return nil
	}
	return newReplicationReaderGroup(shardContext, clientShardKey, tieredStackEnabled, logger)
}

func newSenderLaneComponentsIfEnabled(
	config *configs.Config,
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
	enabled bool,
	classCount int,
	logger log.Logger,
) (*senderLaneRegistry, *senderLaneController, error) {
	if !enabled {
		return nil, nil, nil
	}
	defaultCursor, persisted := persistedReplicationLanes(shardContext, clientShardKey)
	registry, err := newSenderLaneRegistry(defaultCursor, persisted, classCount)
	if err != nil {
		logger.DPanic("Failed to restore replication lanes", tag.Error(err))
		registry, _ = newSenderLaneRegistry(defaultCursor, nil, classCount)
	}
	policy := newNamespaceIsolationPolicy(
		classCount,
		config.ReplicationStreamSenderLaneReclassificationCycles(),
		config.ReplicationStreamSenderLaneReleaseCycles(),
	)
	return registry, newSenderLaneController(
		registry,
		policy,
		config.ReplicationStreamSenderMaxLanes(),
		logger,
	), err
}

func persistedReplicationLanes(
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
) (int64, []*persistencespb.QueueReaderLane) {
	queueState, ok := shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		return 0, nil
	}
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(clientShardKey.ClusterID),
		clientShardKey.ShardID,
	)
	readerState, ok := queueState.ReaderStates[readerID]
	if !ok || len(readerState.Scopes) < 2 {
		return 0, nil
	}
	return readerState.Scopes[1].GetRange().GetInclusiveMin().GetTaskId(), readerState.GetLanes()
}

func newLaneRateLimiters(config *configs.Config, enabled bool, classCount int) []quotas.RateLimiter {
	if !enabled {
		return nil
	}
	limiters := make([]quotas.RateLimiter, classCount)
	for i := range limiters {
		depth := i + 1
		limiters[i] = quotas.NewDynamicRateLimiter(
			quotas.NewRateBurst(func() float64 {
				// Clamp the ratio to (0, 1]: larger values would invert the class
				// ordering, and non-positive values would freeze lanes.
				ratio := min(1, max(0.01, config.ReplicationStreamSenderLaneQPSRatio()))
				return float64(config.ReplicationStreamSenderLowPriorityQPS()) *
					math.Pow(ratio, float64(depth))
			}, func() int { return 1 }),
			time.Minute,
		)
	}
	return limiters
}

func normalizedLaneClassCount(configured int) int {
	return max(1, configured)
}

const sharedLaneTag = "default"

func laneClassTag(class replicationLaneClass) string {
	return "class-" + strconv.Itoa(int(class))
}

func (s *StreamSenderImpl) sendLaneEventLoop(class replicationLaneClass) (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()
	defer log.CapturePanic(s.logger, &panicErr)
	if s.laneInitializationError != nil {
		return NewStreamError("StreamSender failed to restore replication lanes", s.laneInitializationError)
	}

	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification(s.clientClusterName)
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)
	timer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer timer.Stop()

	for {
		if s.lanesConfirmed.Load() {
			end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
			for _, lane := range s.laneRegistry.ClassSnapshots(class) {
				if err := s.sendLane(lane, end); err != nil {
					return err
				}
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

func (s *StreamSenderImpl) sendDefaultTasks(
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
) (bool, error) {
	var filter func(tasks.Task) bool
	if s.laneRegistry != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
		if s.lanesConfirmed.Load() {
			var acquired bool
			filter, acquired = s.laneRegistry.AcquireDefault(endExclusiveWatermark)
			if !acquired {
				return false, nil
			}
			defer s.laneRegistry.ReleaseDefault()
		} else {
			s.laneRegistry.AdvanceDefaultCursor(endExclusiveWatermark)
		}
	}
	return true, s.sendTasksOnLane(
		priority,
		beginInclusiveWatermark,
		endExclusiveWatermark,
		filter,
		"",
		sharedLaneTag,
		nil,
	)
}

func (s *StreamSenderImpl) sendLane(snapshot senderLaneSnapshot, end int64) error {
	lane, ok := s.laneRegistry.Acquire(snapshot.id)
	if !ok {
		return nil
	}
	defer s.laneRegistry.Release(lane.id)
	if lane.cursor >= end {
		return s.sendTasksOnLane(
			enumsspb.TASK_PRIORITY_HIGH,
			lane.cursor,
			lane.cursor,
			nil,
			lane.id,
			laneClassTag(lane.class),
			nil,
		)
	}
	if err := s.sendTasksOnLane(
		enumsspb.TASK_PRIORITY_HIGH,
		lane.cursor,
		end,
		lane.scope.Contains,
		lane.id,
		laneClassTag(lane.class),
		s.laneRateLimiters[int(lane.class)-1],
	); err != nil {
		return err
	}
	s.laneRegistry.AdvanceLaneCursor(lane.id, end)
	return nil
}

func (s *StreamSenderImpl) sendReadyLaneRetirements() error {
	for _, lane := range s.laneRegistry.ReadyRetirements() {
		if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ExclusiveHighWatermark:     s.laneRegistry.DefaultCursor(),
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
					LaneId:                     lane.id,
					RetireLane:                 true,
				},
			},
		}); err != nil {
			return err
		}
		if !s.laneController.CompleteRetirement(lane.id) {
			return serviceerror.NewInternalf("replication lane %q changed while its retirement marker was sent", lane.id)
		}
	}
	return nil
}

func (s *StreamSenderImpl) laneFailoverWatermark(attr *replicationspb.SyncReplicationState) (int64, time.Time) {
	watermark := attr.HighPriorityState.InclusiveLowWatermark
	watermarkTime := attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime()
	for _, laneState := range attr.GetLaneStates() {
		if laneState.GetInclusiveLowWatermark() < watermark {
			watermark = laneState.GetInclusiveLowWatermark()
			watermarkTime = laneState.GetInclusiveLowWatermarkTime().AsTime()
		}
	}
	if s.lanesConfirmed.Load() {
		if floor, floorTime, ok := s.laneRegistry.ResumeFloor(); ok && floor < watermark {
			return floor - 1, floorTime
		}
	}
	return watermark - 1, watermarkTime
}

func (s *StreamSenderImpl) emitLaneMetrics() {
	counts := make(map[replicationLaneClass]int)
	for _, lane := range s.laneRegistry.Snapshots() {
		counts[lane.class]++
	}
	for class := 1; class <= s.laneController.policy.ClassCount(); class++ {
		metrics.ReplicationStreamSenderLaneCount.With(s.metrics).Record(
			float64(counts[replicationLaneClass(class)]),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.ReplicationStreamLaneTag(laneClassTag(replicationLaneClass(class))),
		)
	}
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
		sent, err := s.sendDefaultTasks(
			priority,
			beginInclusiveWatermark,
			endExclusiveWatermark,
		)
		if err != nil {
			return err
		}
		if sent {
			beginInclusiveWatermark = endExclusiveWatermark
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
) error {
	return s.sendTasksOnLane(
		priority,
		beginInclusiveWatermark,
		endExclusiveWatermark,
		nil,
		"",
		sharedLaneTag,
		nil,
	)
}

func (s *StreamSenderImpl) sendTasksOnLane(
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
	filter func(tasks.Task) bool,
	laneID string,
	laneTag string,
	laneRateLimiter quotas.RateLimiter,
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
					LaneId:                     laneID,
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
	for iter.HasNext() {
		if s.shutdownChan.IsShutdown() {
			return nil
		}

		item, err := iter.Next()
		if err != nil {
			return fmt.Errorf("streamSender unable to get next replication task: %w", err)
		}

		skipCount++
		metrics.ReplicationTasksScanned.With(s.metrics).Record(
			1,
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.ReplicationStreamLaneTag(laneTag),
		)
		if err := s.sendLaneProgressIfNeeded(item, priority, laneID, &skipCount); err != nil {
			return err
		}
		if !s.shouldSendTaskOnLane(item, priority, filter) {
			continue
		}
		metrics.ReplicationTaskLoadLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
		)

		attempt, sent, err := s.sendTaskOnLane(item, priority, laneID, laneTag, laneRateLimiter)
		if sent {
			skipCount = 0
		}
		if err != nil {
			if err := s.handleTaskSendError(item, attempt, priority, err); err != nil {
				return err
			}
		}
	}
	return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           nil,
				ExclusiveHighWatermark:     endExclusiveWatermark,
				ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
				Priority:                   priority,
				LaneId:                     laneID,
			},
		},
	})
}

func (s *StreamSenderImpl) sendLaneProgressIfNeeded(
	item tasks.Task,
	priority enumsspb.TaskPriority,
	laneID string,
	skipCount *int,
) error {
	if *skipCount <= TaskMaxSkipCount {
		return nil
	}
	if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ExclusiveHighWatermark:     item.GetTaskID(),
				ExclusiveHighWatermarkTime: timestamppb.New(item.GetVisibilityTime()),
				Priority:                   priority,
				LaneId:                     laneID,
			},
		},
	}); err != nil {
		return err
	}
	*skipCount = 0
	return nil
}

func (s *StreamSenderImpl) shouldSendTaskOnLane(
	item tasks.Task,
	priority enumsspb.TaskPriority,
	filter func(tasks.Task) bool,
) bool {
	if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED && priority != s.getTaskPriority(item) {
		return false
	}
	if filter != nil && !filter(item) {
		return false
	}
	return s.shouldProcessTask(item)
}

func (s *StreamSenderImpl) handleTaskSendError(
	item tasks.Task,
	attempt int64,
	priority enumsspb.TaskPriority,
	err error,
) error {
	// Only conversion failures are safe to skip: rate-limit, send, teardown, and
	// message-size errors must leave the task for a later stream attempt.
	if !s.config.ReplicationStreamSenderSkipStuckTask() || !isSkippable(err) {
		return fmt.Errorf("failed to send task: %v, cause: %w", item, err)
	}
	s.recordStuckTaskSkipped(item, attempt, priority, err)
	metrics.ReplicationTaskSendSkipped.With(s.metrics).Record(
		int64(1),
		metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
		metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
		metrics.ReplicationTaskPriorityTag(priority),
	)
	return nil
}

func (s *StreamSenderImpl) sendTaskOnLane(
	item tasks.Task,
	priority enumsspb.TaskPriority,
	laneID string,
	laneTag string,
	laneRateLimiter quotas.RateLimiter,
) (int64, bool, error) {
	var attempt int64
	sent := false
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
		if err := s.sendConvertedTaskOnLane(item, task, priority, attempt, laneID, laneTag, laneRateLimiter); err != nil {
			return err
		}
		sent = true
		return nil
	}

	retryPolicy := backoff.NewExponentialRetryPolicy(s.config.ReplicationStreamSenderErrorRetryWait()).
		WithBackoffCoefficient(s.config.ReplicationStreamSenderErrorRetryBackoffCoefficient()).
		WithMaximumInterval(s.config.ReplicationStreamSenderErrorRetryMaxInterval()).
		WithMaximumAttempts(s.config.ReplicationStreamSenderErrorRetryMaxAttempts()).
		WithExpirationInterval(s.config.ReplicationStreamSenderErrorRetryExpiration())

	err := backoff.ThrottleRetry(operation, retryPolicy, isRetryableError)
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
	}
	return attempt, sent, err
}

func (s *StreamSenderImpl) sendConvertedTaskOnLane(
	item tasks.Task,
	task *replicationspb.ReplicationTask,
	priority enumsspb.TaskPriority,
	attempt int64,
	laneID string,
	laneTag string,
	laneRateLimiter quotas.RateLimiter,
) error {
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
			return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationRateLimit, fmt.Errorf("rate_limit: %w", err))
		}
		metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)))
	}
	if laneRateLimiter != nil {
		rlStartTime := time.Now().UTC()
		if err := laneRateLimiter.Wait(s.server.Context()); err != nil {
			return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationRateLimit, fmt.Errorf("lane rate limit: %w", err))
		}
		metrics.ReplicationRateLimitLatency.With(s.metrics).Record(
			time.Since(rlStartTime),
			metrics.OperationTag(TaskOperationTag(task)),
			metrics.ReplicationStreamLaneTag(laneTag),
		)
	}
	if s.config.EmitReplicationLifecycleEvents() {
		s.emitReplicationSent(task, item)
	}
	if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task},
				ExclusiveHighWatermark:     task.SourceTaskId + 1,
				ExclusiveHighWatermarkTime: task.VisibilityTime,
				Priority:                   priority,
				LaneId:                     laneID,
			},
		},
	}); err != nil {
		return s.recordRetry(item, task.GetTaskType(), priority, attempt, wideevents.ReplOperationStreamSend, fmt.Errorf("send: %w", err))
	}
	metrics.ReplicationTasksSend.With(s.metrics).Record(
		int64(1),
		metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
		metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
		metrics.OperationTag(TaskOperationTag(task)),
		metrics.ReplicationStreamLaneTag(laneTag),
	)
	return nil
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

func (s *StreamSenderImpl) admittedByGradualConnect(item tasks.Task, namespaceEntry *namespace.Namespace) bool {
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

func gradualConnectPercent(ramp *persistencespb.NamespaceReplicationRamp, now time.Time) int {
	if ramp == nil || ramp.GetStartTime() == nil || ramp.GetDuration() == nil ||
		ramp.GetStartTime().CheckValid() != nil || ramp.GetDuration().CheckValid() != nil {
		return 100
	}
	duration := ramp.GetDuration().AsDuration()
	if duration <= 0 {
		return 100
	}
	elapsed := now.Sub(ramp.GetStartTime().AsTime())
	if elapsed <= 0 {
		return 0
	}
	if elapsed >= duration {
		return 100
	}
	return int(float64(elapsed) / float64(duration) * 100)
}

func (s *StreamSenderImpl) getTaskPriority(task tasks.Task) enumsspb.TaskPriority {
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

func defaultHighTaskPriority(priority enumsspb.TaskPriority) enumsspb.TaskPriority {
	if priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
		return enumsspb.TASK_PRIORITY_HIGH
	}
	return priority
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

func (s *StreamSenderImpl) recordRetry(
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
func (s *StreamSenderImpl) recordStuckTaskSkipped(
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

// convertError marks a failure to build ("convert") a replication task from its source task info.
// Such a task cannot be re-sent as-is, so — unlike a transient send or rate-limit failure — it is a
// candidate to skip when ReplicationStreamSenderSkipStuckTask is enabled. See isSkippable.
type convertError struct {
	err error
}

func (e *convertError) Error() string { return e.err.Error() }
func (e *convertError) Unwrap() error { return e.err }

// isSkippable reports whether a task that failed to send after exhausting its retry budget may be
// safely skipped (dropped, with the watermark advanced past it) instead of wedging the stream. We
// skip only when BOTH hold:
//   - the task could not be built (convertError): its source info is corrupt/unusable, so retrying
//     or reconnecting will never make it send; and
//   - the underlying error is otherwise retryable: infra/teardown errors (shard-ownership-lost,
//     stream error, context canceled) can also surface from the convert step, and those must still
//     tear the stream down so shard handoff / reconnect can proceed.
//
// Transient send and rate-limit failures are deliberately excluded (they are not convertErrors):
// dropping a task that would have succeeded on reconnect would be silent data loss.
func isSkippable(err error) bool {
	var convErr *convertError
	return errors.As(err, &convErr) && isRetryableError(err)
}
