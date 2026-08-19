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
		isolation               *isolationManager
		// isolationConfirmed is set once the receiver advertises support for
		// per-namespace lane isolation. Until then the sender keeps all HIGH traffic
		// on the shared lane and emits no lane-tagged messages, so an old receiver is
		// never sent lanes it would misroute.
		isolationConfirmed atomic.Bool
		tierRateLimiters   []quotas.RateLimiter // index t-1 limits throttled tier t
		// laneSendMu guards activeLaneSends and pendingRetires. A lane-retirement
		// marker must be the LAST message the receiver sees for a lane: emitting one
		// while a tier loop still has an in-flight (or stale-snapshot, about-to-start)
		// send for the member would let a straggler batch re-create the receiver lane
		// after the marker, freezing its watermark in the ack fold forever.
		laneSendMu      sync.Mutex
		activeLaneSends map[string]int // member namespace -> in-flight tier sends
		pendingRetires  []string       // graduated members awaiting a safe marker send
		flowController  SenderFlowController
		sendLock        sync.Mutex
		ssRateLimiter   ServerSchedulerRateLimiter
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
	// flip between two reads would leave the sender half in each mode — e.g. an
	// isolation manager whose TierCount exceeds len(tierRateLimiters), panicking the
	// tier loops — until the recv loop's config guard restarts the stream.
	tieredStackEnabled := config.EnableReplicationTaskTieredProcessing()
	isolationEnabled := tieredStackEnabled && config.EnableReplicationNamespaceIsolation()
	throttledTierCount := normalizedThrottledTierCount(config.ReplicationStreamSenderThrottledTierCount())
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
		readerGroup:             newReaderGroupIfEnabled(config, shardContext, clientShardKey, tieredStackEnabled, isolationEnabled, logger),
		isolation:               newIsolationManagerIfEnabled(config, shardContext, clientShardKey, isolationEnabled, throttledTierCount, logger),
		tierRateLimiters:        newTierRateLimiters(config, isolationEnabled, throttledTierCount),
		activeLaneSends:         make(map[string]int),
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
		if s.isolation != nil {
			// One send loop per severity tier; each drains its members' dedicated
			// lanes at the tier's (rate-limited) pace.
			for tier := 1; tier <= s.isolation.TierCount(); tier++ {
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
		if (s.isolation != nil) != (s.config.EnableReplicationNamespaceIsolation() && s.isTieredStackEnabled) {
			return NewStreamError("StreamSender detected namespace isolation config change, restart the stream", nil)
		}
		if s.isolation != nil && s.isolation.TierCount() != normalizedThrottledTierCount(s.config.ReplicationStreamSenderThrottledTierCount()) {
			return NewStreamError("StreamSender detected throttled tier count change, restart the stream", nil)
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
	if attr.GetSupportsNamespaceIsolation() {
		s.isolationConfirmed.Store(true)
	}

	if s.isolation != nil {
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return NewStreamError("streamSender: missing priority state with namespace isolation", nil)
		}
		if s.isTieredStackEnabled {
			s.flowController.RefreshReceiverFlowControlInfo(attr)
		}
		// Reconcile lane membership against the receiver's throttled set, then persist
		// one scope per member lane (namespace predicate + applied watermark), so a
		// reconnect reconstructs the lanes instead of re-sending the whole HIGH lane.
		// Split floors and the merge-back gate anchor on applied (acked) watermarks so
		// ordered HIGH events never straddle a hand-off with a gap. Scopes[0] is the
		// receiver's overall min clamped to the member lanes' resume positions —
		// replication task cleanup reads only Scopes[0], and the receiver's fold
		// cannot vouch for windows its connection has never seen.
		s.isolation.Reconcile(
			attr.GetPauseHighNamespaceIds(),
			attr.GetHighPriorityState().GetInclusiveLowWatermark(),
			memberAckedWatermarks(attr.GetIsolatedLaneStates()),
		)
		s.emitTierMetrics()
		if err := s.sendPendingRetirements(); err != nil {
			return err
		}
		if err := s.shardContext.UpdateReplicationQueueReaderState(readerID, s.isolation.BuildReaderState(attr)); err != nil {
			return err
		}
		taskID, ts := s.isolationFailoverWatermark(attr)
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
	if s.isolation != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
		if !s.isolationConfirmed.Load() {
			// Restored member lanes exist but the receiver has not (or will never —
			// e.g. a version rollback) confirmed isolation support, so the tier
			// loops may never run. Their unsent windows must ride the shared lane
			// instead: start catch-up no higher than the lowest member resume
			// position (the shared filter admits members while unconfirmed). If the
			// receiver confirms later, tier lanes re-cover the same window —
			// duplicates, applied idempotently, never a gap.
			if memberFloor := s.isolation.MemberResumeFloor(); memberFloor > 0 {
				catchupBeginInclusiveWatermark = min(catchupBeginInclusiveWatermark, memberFloor)
			}
		}
		// Advance the default cursor BEFORE sending: the merge-back gate compares tier
		// acked watermarks against it, so advancing first guarantees a namespace can
		// never graduate while a batch that excludes it is still in flight (which would
		// leave its tasks in that batch's window unsent on every lane).
		s.isolation.AdvanceDefaultCursor(catchupEndExclusiveWatermark)
	}
	if err := s.sendTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
		s.defaultLaneFilter(priority),
		"", // shared HIGH / LOW lane, not an isolated member lane
		sharedLaneTag,
		nil,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

// catchupBeginWatermark returns the inclusive begin watermark for the catch-up scan:
// the persisted reader-state cursor for the given priority, falling back to the
// current end watermark when no state is persisted for this reader.
func (s *StreamSenderImpl) catchupBeginWatermark(priority enumsspb.TaskPriority, end int64) int64 {
	if s.readerGroup != nil {
		return s.readerGroup.CatchupBeginWatermark(end, priority)
	}
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
	// Isolation OFF but leftover tier scopes persisted (isolation was disabled while a
	// tier lagged): resume default-HIGH from the overall min (scope 0) so the undrained
	// tier windows are re-covered once — a rare, bounded, idempotent re-send. With
	// isolation ON the tiers are reconstructed instead, so default-HIGH resumes
	// normally from its own scope.
	if s.isolation == nil && priority == enumsspb.TASK_PRIORITY_HIGH && len(readerState.Scopes) > 3 {
		scopePriority = enumsspb.TASK_PRIORITY_UNSPECIFIED
	}
	return s.getSendCatchupBeginInclusiveWatermark(readerState, scopePriority)
}

func (s *StreamSenderImpl) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	// priorityScopeIndex tolerates the single-stack format (1 scope -> index 0) and the
	// tiered format (3 scopes -> HIGH=1, LOW=2). When switching from single to tiered
	// stack the reader state is still in the old format, in which case using the overall
	// low watermark (scope 0) is safe as long as we always guarantee the overall low
	// watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark).
	// Extended states (>3 scopes) only exist when isolation wrote member-lane scopes,
	// so the isolation manager's presence is what allows reading priority scopes out of
	// them: without it HIGH has already been mapped to the overall min by the orphaned
	// scope guard in catchupBeginWatermark.
	return readerState.Scopes[priorityScopeIndex(priority, len(readerState.Scopes), s.isolation != nil)].Range.InclusiveMin.TaskId
}

func newReaderGroupIfEnabled(
	config *configs.Config,
	shardContext historyi.ShardContext,
	clientShardKey ClusterShardKey,
	tieredStackEnabled bool,
	isolationEnabled bool,
	logger log.Logger,
) *replicationReaderGroup {
	if !config.EnableReplicationReaderGroup() {
		return nil
	}
	return newReplicationReaderGroup(shardContext, clientShardKey, tieredStackEnabled, isolationEnabled, logger)
}

// newIsolationManagerIfEnabled builds the namespace isolation manager when enabled.
// Isolation only exists in the tiered stack, so both flags are required. On a stream
// (re)start it reconstructs member lanes from the persisted reader state, so
// throttled namespaces stay isolated across reconnects (e.g. history deploys)
// instead of bursting back onto the shared lane.
func newIsolationManagerIfEnabled(config *configs.Config, shardContext historyi.ShardContext, clientShardKey ClusterShardKey, enabled bool, tierCount int, logger log.Logger) *isolationManager {
	if !enabled {
		return nil
	}
	defaultCursor, restored, err := reconstructIsolationState(shardContext, clientShardKey)
	if err != nil {
		// Fall back to zero state: throttled namespaces burst back onto the shared
		// lane (the reconnect backstop) rather than blocking stream startup.
		logger.Error("Failed to parse isolation state, starting with no restored lanes", tag.Error(err))
	}
	return newIsolationManagerWithState(
		tierCount,
		config.ReplicationStreamSenderTierDemotionCycles(),
		config.ReplicationStreamSenderUnthrottleCooldownCycles(),
		config.ReplicationStreamSenderMaxIsolatedNamespaces(),
		defaultCursor,
		restored,
	)
}

// reconstructIsolationState reads the persisted replication reader state and
// extracts the shared-HIGH resume cursor and the member lanes. Returns zero state
// when nothing is persisted (fresh shard).
func reconstructIsolationState(shardContext historyi.ShardContext, clientShardKey ClusterShardKey) (int64, []restoredMember, error) {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(clientShardKey.ClusterID),
		clientShardKey.ShardID,
	)
	queueState, ok := shardContext.GetQueueState(tasks.CategoryReplication)
	if !ok {
		return 0, nil, nil
	}
	readerState, ok := queueState.ReaderStates[readerID]
	if !ok {
		return 0, nil, nil
	}
	return parseIsolationState(readerState)
}

// newTierRateLimiters builds one outgoing rate limiter per severity tier; deeper
// tiers run slower at LowPriorityQPS * ThrottledTierQPSRatio^tier. A tier's budget is
// shared by its members' lanes. Returns nil when isolation is off.
func newTierRateLimiters(config *configs.Config, enabled bool, tierCount int) []quotas.RateLimiter {
	if !enabled {
		return nil
	}
	// Clamped exactly like newIsolationManager's tierCount, so len(tierRateLimiters)
	// always equals TierCount().
	count := normalizedThrottledTierCount(tierCount)
	limiters := make([]quotas.RateLimiter, count)
	for i := range limiters {
		depth := i + 1 // tier number (1-based)
		rateFn := func() float64 {
			ratio := math.Pow(config.ReplicationStreamSenderThrottledTierQPSRatio(), float64(depth))
			return float64(config.ReplicationStreamSenderLowPriorityQPS()) * ratio
		}
		// Burst 1: a throttled lane is a strict trickle. The default burst (one
		// second of rate) would let an idle lane catch up at line speed, momentarily
		// competing with healthy namespaces for receiver apply capacity — the exact
		// resource isolation protects.
		limiters[i] = quotas.NewDynamicRateLimiter(
			quotas.NewRateBurst(rateFn, func() int { return 1 }),
			time.Minute,
		)
	}
	return limiters
}

func normalizedThrottledTierCount(configured int) int {
	return max(1, configured)
}

// defaultLaneFilter returns the task filter for the shared HIGH lane: it excludes
// namespaces currently owning an isolated lane. Returns nil (admit all) when
// isolation is disabled or for non-HIGH priorities.
func (s *StreamSenderImpl) defaultLaneFilter(priority enumsspb.TaskPriority) func(task tasks.Task) bool {
	// Until the receiver confirms isolation support, admit everything on the shared
	// lane (no exclusion, no lane traffic) — otherwise an old receiver would never
	// receive the excluded namespaces' tasks.
	if s.isolation == nil || priority != enumsspb.TASK_PRIORITY_HIGH || !s.isolationConfirmed.Load() {
		return nil
	}
	return s.isolation.DefaultFilter()
}

// memberAckedWatermarks extracts each isolated lane's applied watermark from the
// receiver's per-lane report.
func memberAckedWatermarks(laneStates map[string]*replicationspb.ReplicationState) map[string]int64 {
	if len(laneStates) == 0 {
		return nil
	}
	out := make(map[string]int64, len(laneStates))
	for ns, state := range laneStates {
		out[ns] = state.GetInclusiveLowWatermark()
	}
	return out
}

// sharedLaneTag is the replicationStreamLane metric value for the shared priority
// lanes; isolated member lanes are tagged by their severity tier ("tier-N"), keeping
// metric cardinality bounded by the tier count rather than the namespace count.
const sharedLaneTag = "default"

// tierLaneTagValue is the replicationStreamLane metric value for a severity tier.
func tierLaneTagValue(tier int) string {
	return "tier-" + strconv.Itoa(tier)
}

// emitTierMetrics reports how many namespaces are isolated in each severity tier, so
// isolation usage can be observed and the cap sized.
func (s *StreamSenderImpl) emitTierMetrics() {
	for tier := 1; tier <= s.isolation.TierCount(); tier++ {
		metrics.ReplicationStreamSenderThrottledNamespaceCount.With(s.metrics).Record(
			float64(s.isolation.TierMemberCount(tier)),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.ReplicationStreamLaneTag(tierLaneTagValue(tier)),
		)
	}
}

// sendTierEventLoop runs the send loop for one severity tier (1..K): each wake-up it
// walks the tier's members and streams each member's dedicated lane from that lane's
// own cursor, pacing every task through the tier's shared rate limiter. Members never
// interact — a member joining, demoting, or graduating moves no other member's
// cursor, and no cursor ever rewinds. An idle member lane sends a watermark-only
// keepalive so its receiver-side watermark keeps advancing and never pins the overall
// ack minimum. The loop runs for the life of the stream; a tier with no members idles.
// sendTierMember streams one member's lane for one tier-loop wake-up: a
// watermark-only keepalive when the lane is idle, otherwise the lane's tasks in
// [cursor, end) at the tier's pace. All lane traffic runs under a send lease
// (beginMemberLaneSend) so retirement markers can be ordered strictly after the
// lane's final batch.
func (s *StreamSenderImpl) sendTierMember(tier int, member memberSnapshot, end int64) error {
	if !s.beginMemberLaneSend(member.namespaceID, member.generation) {
		// The member graduated (or re-split into a new incarnation) after this
		// tier-loop snapshot was taken: this incarnation must send nothing more.
		return nil
	}
	defer s.endMemberLaneSend(member.namespaceID)
	if member.cursor >= end {
		// Idle (or briefly ahead of the read watermark): watermark-only keepalive at
		// the lane's own cursor.
		return s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, member.cursor, member.cursor, nil, member.namespaceID, tierLaneTagValue(tier), nil)
	}
	if err := s.sendTasks(enumsspb.TASK_PRIORITY_HIGH, member.cursor, end, member.scope.Contains, member.namespaceID, tierLaneTagValue(tier), s.tierRateLimiters[tier-1]); err != nil {
		return err
	}
	s.isolation.AdvanceMemberCursor(member.namespaceID, member.generation, end)
	return nil
}

// beginMemberLaneSend takes a send lease on a member's lane, verifying the snapshot
// is still current. The double-check after registering closes the race with marker
// emission: any lane send that proceeds either registered before the marker path
// checked activity (so the marker was deferred), or re-verified membership after —
// and a member is removed from the manager before its marker becomes pending, so a
// send that passes the re-check strictly precedes its marker.
func (s *StreamSenderImpl) beginMemberLaneSend(namespaceID string, generation int64) bool {
	if !s.isolation.IsMemberGeneration(namespaceID, generation) {
		return false
	}
	s.laneSendMu.Lock()
	s.activeLaneSends[namespaceID]++
	s.laneSendMu.Unlock()
	if !s.isolation.IsMemberGeneration(namespaceID, generation) {
		s.endMemberLaneSend(namespaceID)
		return false
	}
	return true
}

func (s *StreamSenderImpl) endMemberLaneSend(namespaceID string) {
	s.laneSendMu.Lock()
	if s.activeLaneSends[namespaceID]--; s.activeLaneSends[namespaceID] <= 0 {
		delete(s.activeLaneSends, namespaceID)
	}
	s.laneSendMu.Unlock()
}

// sendPendingRetirements emits lane-retirement markers for graduated members whose
// lanes are quiescent. A marker for a member with an in-flight tier send stays
// pending until a later ack (the marker must be the lane's last message); a marker
// for a namespace that re-split in the meantime is dropped (it would retire the NEW
// lane). Called on the recv loop, once per ack.
func (s *StreamSenderImpl) sendPendingRetirements() error {
	s.laneSendMu.Lock()
	s.pendingRetires = append(s.pendingRetires, s.isolation.PopRetired()...)
	var toSend []string
	remaining := s.pendingRetires[:0]
	for _, ns := range s.pendingRetires {
		switch {
		case s.isolation.IsMember(ns):
			// Re-split before the marker went out: the retirement is obsolete.
		case s.activeLaneSends[ns] > 0:
			remaining = append(remaining, ns)
		default:
			toSend = append(toSend, ns)
		}
	}
	s.pendingRetires = remaining
	s.laneSendMu.Unlock()

	// The marker is the lane's final message, letting the receiver drop the lane's
	// tracking once drained.
	for _, ns := range toSend {
		if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ExclusiveHighWatermark:     s.isolation.DefaultCursor(),
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
					IsolatedNamespaceId:        ns,
					RetireIsolatedLane:         true,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

// isolationFailoverWatermark returns the watermark for UpdateRemoteReaderInfo with
// isolation active. The HIGH lane alone no longer bounds all live traffic — an
// isolated lane can lag hours behind at deep-tier pacing — so failover readiness
// must fold every isolated lane's applied watermark into the bound. Once the
// receiver has confirmed isolation support it also folds in the member lanes' resume
// floor: receiver lane trackers are per-connection and lazily created on lane
// traffic, so a restored lane is invisible in the receiver's report until its first
// post-reconnect batch lands, and the floor is the only bound for its unsent window
// until then. Before confirmation the shared lane honestly covers those windows
// (catch-up clamps to the floor and admits members), so HIGH alone is correct.
func (s *StreamSenderImpl) isolationFailoverWatermark(attr *replicationspb.SyncReplicationState) (int64, time.Time) {
	watermark := attr.HighPriorityState.InclusiveLowWatermark
	watermarkTime := attr.HighPriorityState.InclusiveLowWatermarkTime.AsTime()
	for _, laneState := range attr.GetIsolatedLaneStates() {
		if laneState.GetInclusiveLowWatermark() < watermark {
			watermark = laneState.GetInclusiveLowWatermark()
			watermarkTime = laneState.GetInclusiveLowWatermarkTime().AsTime()
		}
	}
	if s.isolationConfirmed.Load() {
		if floor := s.isolation.MemberResumeFloor(); floor > 0 && floor < watermark {
			// The floor has no receiver-reported visibility time; pair the
			// conservative task ID with an unknown (zero) timestamp rather than a
			// fresher lane's time.
			return floor - 1, time.Time{}
		}
	}
	return watermark - 1, watermarkTime
}

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

	timer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer timer.Stop()

	for {
		// Only emit lane-tagged traffic once the receiver has confirmed it routes lanes.
		if s.isolationConfirmed.Load() {
			end := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
			for _, member := range s.isolation.TierMemberSnapshots(tier) {
				if err := s.sendTierMember(tier, member, end); err != nil {
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

func (s *StreamSenderImpl) sendLive(
	priority enumsspb.TaskPriority,
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	syncStatusTimer := time.NewTimer(s.config.ReplicationStreamSendEmptyTaskDuration())
	defer syncStatusTimer.Stop()
	sendTasks := func() error {
		endExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
		if s.isolation != nil && priority == enumsspb.TASK_PRIORITY_HIGH {
			// Advance BEFORE sending (and before snapshotting the filter): see sendCatchUp.
			s.isolation.AdvanceDefaultCursor(endExclusiveWatermark)
		}
		if err := s.sendTasks(
			priority,
			beginInclusiveWatermark,
			endExclusiveWatermark,
			s.defaultLaneFilter(priority), // re-snapshot per batch: membership changes
			"",
			sharedLaneTag,
			nil,
		); err != nil {
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

func (s *StreamSenderImpl) sendTasks(
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
	nsFilter func(task tasks.Task) bool,
	isolatedNamespaceID string,
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
					IsolatedNamespaceId:        isolatedNamespaceID,
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
	// Rows examined pre-filter, batched per call: with replication_tasks_send this
	// gives the lane's scan amplification (rows examined per task sent).
	scannedCount := int64(0)
	defer func() {
		if scannedCount > 0 {
			metrics.ReplicationTasksScanned.With(s.metrics).Record(
				scannedCount,
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.ReplicationTaskPriorityTag(priority),
				metrics.ReplicationStreamLaneTag(laneTag),
			)
		}
	}()
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

		scannedCount++
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
						IsolatedNamespaceId:        isolatedNamespaceID,
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
		if nsFilter != nil && !nsFilter(item) { // case: task owned by a different lane
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
			metrics.ReplicationStreamLaneTag(laneTag),
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
				metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)))
			}
			if laneRateLimiter != nil {
				// Per-task pacing for a throttled tier: this is what actually slows an
				// isolated namespace down (a per-batch wait would let unbounded batches
				// through at full speed).
				rlStartTime := time.Now().UTC()
				if err := laneRateLimiter.Wait(s.server.Context()); err != nil {
					return s.recordRetry(item, attempt, fmt.Errorf("tier_rate_limit: %w", err))
				}
				metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)), metrics.ReplicationStreamLaneTag(laneTag))
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
						IsolatedNamespaceId:        isolatedNamespaceID,
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
				metrics.ReplicationStreamLaneTag(laneTag),
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
			metrics.ReplicationStreamLaneTag(laneTag),
		)
		metrics.ReplicationTaskSendLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(priority),
			metrics.ReplicationStreamLaneTag(laneTag),
		)
		if err != nil {
			metrics.ReplicationTaskSendError.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
				metrics.ReplicationTaskPriorityTag(priority),
				metrics.ReplicationStreamLaneTag(laneTag),
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
				IsolatedNamespaceId:        isolatedNamespaceID,
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
