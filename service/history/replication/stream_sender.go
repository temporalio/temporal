//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_sender_mock.go

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
		flowController          SenderFlowController
		sendLock                sync.Mutex
		ssRateLimiter           ServerSchedulerRateLimiter
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
		readerGroup:             newReaderGroupIfEnabled(config.EnableReplicationReaderGroup, shardContext, clientShardKey, tieredStackEnabled, logger),
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
		if !s.shouldProcessTask(item) {
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
				// Wrap as convertError so isSkippable can tell "the task could not be built"
				// (its source info is corrupt/unusable) apart from transient send/rate-limit
				// failures, which must not be skipped.
				return s.recordRetry(item, attempt, &convertError{err: fmt.Errorf("convert: %w", err)})
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
			// Only skip a task that could not be *built* after exhausting retries (isSkippable):
			// its source info is corrupt/unusable, so retrying or reconnecting will never make it
			// send. Transient send/rate-limit failures are NOT skipped (dropping a task that would
			// have succeeded on reconnect is silent data loss), and infra/teardown errors
			// (shard-ownership-lost, stream error, context canceled) must still tear the stream
			// down so shard handoff / reconnect can proceed. Deterministic non-retryable failures
			// such as an oversized gRPC message are also intentionally NOT handled here: they
			// surface from the send path as a (non-retryable) StreamError, are left to the
			// transport-layer message-size fix, and remain observable via the throttled skip log
			// and the ReplicationTaskSendSkipped metric.
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
