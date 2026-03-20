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
		flowController          SenderFlowController
		sendLock                sync.Mutex
		ssRateLimiter           ServerSchedulerRateLimiter
		// throttledNamespaceIDs stores a map[string]struct{} of namespace IDs that the
		// receiver has reported as throttled; updated on each SyncReplicationState ACK.
		throttledNamespaceIDs atomic.Value
		// throttledCatchupStart is the task ID of the first THROTTLED task skipped during a
		// flow control pause. Non-zero means a catchup is pending. Only accessed by the HIGH
		// sender goroutine so no synchronization is needed.
		throttledCatchupStart      int64
		// throttledCatchupNamespaces is the snapshot of throttled namespace IDs taken when
		// throttledCatchupStart was first set. The catchup must only re-send tasks from these
		// namespaces — tasks for other namespaces were sent normally as HIGH and must not be
		// duplicated. The atomic.Value stores an immutable map so the snapshot is safe to hold.
		throttledCatchupNamespaces map[string]struct{}
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

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	if s.isTieredStackEnabled {
		s.flowController.RefreshReceiverFlowControlInfo(attr)
		throttled := make(map[string]struct{}, len(attr.GetThrottledNamespaceIds()))
		for _, id := range attr.GetThrottledNamespaceIds() {
			throttled[id] = struct{}{}
		}
		s.throttledNamespaceIDs.Store(throttled)
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
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)

	catchupEndExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID

	var catchupBeginInclusiveWatermark int64
	queueState, ok := s.shardContext.GetQueueState(
		tasks.CategoryReplication,
	)
	if !ok {
		s.logger.Debug("StreamSender queueState not found")
		catchupBeginInclusiveWatermark = catchupEndExclusiveWatermark
	} else {
		readerState, ok := queueState.ReaderStates[readerID]
		if !ok {
			s.logger.Debug(fmt.Sprintf("StreamSender readerState not found, readerID %v", readerID))
			catchupBeginInclusiveWatermark = catchupEndExclusiveWatermark
		} else {
			catchupBeginInclusiveWatermark = s.getSendCatchupBeginInclusiveWatermark(readerState, priority)
		}
	}
	if err := s.sendTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

func (s *StreamSenderImpl) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	getReaderScopesIndex := func(priority enumsspb.TaskPriority) int {
		switch priority {
		case enumsspb.TASK_PRIORITY_HIGH:
			/*
				this is to handle the case when switch from single stack to tiered stack, the reader state is still in old format.
				In this case, it is safe to use the overall low watermark as the beginInclusiveWatermark, as long as we always guarantee
				the overall low watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark)
			*/
			if len(readerState.Scopes) != 3 {
				return 0
			}
			return 1
		case enumsspb.TASK_PRIORITY_LOW:
			if len(readerState.Scopes) != 3 {
				return 0
			}
			return 2
		case enumsspb.TASK_PRIORITY_UNSPECIFIED:
			return 0
		default:
			return 0
		}
	}
	return readerState.Scopes[getReaderScopesIndex(priority)].Range.InclusiveMin.TaskId
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
		// If THROTTLED tasks were skipped during a pause, run a catchup once the lane
		// resumes so those tasks are delivered before the HIGH reader advances further.
		if priority == enumsspb.TASK_PRIORITY_HIGH &&
			s.throttledCatchupStart > 0 &&
			!s.flowController.IsPaused(enumsspb.TASK_PRIORITY_THROTTLED) {
			if err := s.sendThrottledCatchup(s.throttledCatchupStart, beginInclusiveWatermark); err != nil {
				return err
			}
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
		// In tiered-stack mode, resolve the effective priority for this task.
		// The HIGH loop handles both HIGH and THROTTLED tasks (THROTTLED tasks originate from
		// the HIGH task reader). The LOW loop handles only always-LOW tasks.
		// In single-stack mode (UNSPECIFIED), priority is passed through unchanged.
		taskPriority := priority
		if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED {
			taskPriority = s.getEffectiveTaskPriority(item)
			// HIGH loop: skip only always-LOW tasks (e.g. SyncWorkflowStateTask); THROTTLED tasks
			// originate from the HIGH reader and are intentionally sent within this loop at reduced
			// priority, so they are NOT skipped here.
			if priority == enumsspb.TASK_PRIORITY_HIGH && taskPriority == enumsspb.TASK_PRIORITY_LOW {
				continue Loop
			}
			// LOW loop: skip anything that isn't always-LOW (including THROTTLED, which the HIGH
			// loop handles so the LOW reader doesn't double-send it).
			if priority == enumsspb.TASK_PRIORITY_LOW && taskPriority != enumsspb.TASK_PRIORITY_LOW {
				continue Loop
			}
		}
		if !s.shouldProcessTask(item) {
			continue Loop
		}
		// Non-blocking THROTTLED skip: if this task has been demoted to THROTTLED and the
		// THROTTLED flow control lane is currently paused, skip it so the HIGH goroutine
		// is not blocked. On the first skip, send an anchor beacon so the receiver's
		// THROTTLED tracker holds the synthetic HIGH watermark back to this task ID —
		// preventing the sender from reading past it on reconnect. A catchup pass will
		// re-send the skipped tasks once the lane resumes.
		if s.isTieredStackEnabled &&
			taskPriority == enumsspb.TASK_PRIORITY_THROTTLED &&
			s.flowController.IsPaused(enumsspb.TASK_PRIORITY_THROTTLED) {
			if s.throttledCatchupStart == 0 {
				s.throttledCatchupStart = item.GetTaskID()
				// Snapshot the throttled namespace set so the catchup re-sends only
				// tasks that were actually skipped. Tasks for other namespaces were
				// already sent as HIGH and must not be duplicated.
				m, _ := s.throttledNamespaceIDs.Load().(map[string]struct{})
				s.throttledCatchupNamespaces = m
				if err := s.sendThrottledAnchor(item.GetTaskID(), item.GetVisibilityTime()); err != nil {
					return err
				}
			}
			continue Loop
		}
		metrics.ReplicationTaskLoadLatency.With(s.metrics).Record(
			time.Since(item.GetVisibilityTime()),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTagFromTask(item.GetType())),
			metrics.ReplicationTaskPriorityTag(taskPriority),
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
					metrics.ReplicationTaskPriorityTag(taskPriority),
				)
			}()
			task, err := s.taskConverter.Convert(item, s.clientShardKey.ClusterID, taskPriority)
			if err != nil {
				return err
			}
			if task == nil {
				return nil
			}
			task.Priority = taskPriority
			if s.isTieredStackEnabled {
				// THROTTLED tasks use the THROTTLED flow control lane so backpressure
				// from throttled-namespace backlogs does not affect the HIGH lane.
				waitPriority := priority
				if taskPriority == enumsspb.TASK_PRIORITY_THROTTLED {
					waitPriority = enumsspb.TASK_PRIORITY_THROTTLED
				}
				if err := s.flowController.Wait(s.server.Context(), waitPriority); err != nil {
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
					return err
				}
				metrics.ReplicationRateLimitLatency.With(s.metrics).Record(time.Since(rlStartTime), metrics.OperationTag(TaskOperationTag(task)))
			}
			if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ReplicationTasks:           []*replicationspb.ReplicationTask{task},
						ExclusiveHighWatermark:     task.SourceTaskId + 1,
						ExclusiveHighWatermarkTime: task.VisibilityTime,
						Priority:                   taskPriority,
					},
				},
			}); err != nil {
				return err
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

// sendThrottledAnchor sends an empty THROTTLED-priority batch with ExclusiveHighWatermark=taskID
// to the receiver. This anchors the receiver's THROTTLED tracker so the synthetic HIGH watermark
// (min of HIGH and THROTTLED trackers) does not advance past unacknowledged THROTTLED tasks.
func (s *StreamSenderImpl) sendThrottledAnchor(taskID int64, visibilityTime time.Time) error {
	return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ExclusiveHighWatermark:     taskID,
				ExclusiveHighWatermarkTime: timestamppb.New(visibilityTime),
				Priority:                   enumsspb.TASK_PRIORITY_THROTTLED,
			},
		},
	})
}

// sendThrottledCatchup re-reads tasks in [start, end) and sends originally-HIGH tasks
// as THROTTLED priority to fill the gap left by skipping them during a THROTTLED pause.
// It uses non-blocking pause checks so a re-pause mid-catchup updates throttledCatchupStart
// and returns immediately without blocking the HIGH sender goroutine. On full completion it
// clears throttledCatchupStart.
func (s *StreamSenderImpl) sendThrottledCatchup(start, end int64) error {
	if start >= end {
		s.throttledCatchupStart = 0
		s.throttledCatchupNamespaces = nil
		return nil
	}
	s.logger.Info("starting THROTTLED catchup", tag.NewInt64("start", start), tag.NewInt64("end", end))

	callerInfo := getReplicaitonCallerInfo(enumsspb.TASK_PRIORITY_THROTTLED)
	ctx := headers.SetCallerInfo(s.server.Context(), callerInfo)
	iter, err := s.historyEngine.GetReplicationTasksIter(ctx, string(s.clientShardKey.ClusterID), start, end)
	if err != nil {
		return err
	}

	for iter.HasNext() {
		if s.shutdownChan.IsShutdown() {
			return nil
		}
		item, err := iter.Next()
		if err != nil {
			return fmt.Errorf("sendThrottledCatchup unable to get next replication task: %w", err)
		}
		// Only re-send tasks that (a) are originally HIGH priority and (b) belong to a
		// namespace that was throttled when the skip occurred. Tasks for other namespaces
		// were sent normally as HIGH during the pause and must not be duplicated.
		// Always-LOW tasks (e.g. SyncWorkflowStateTask) are served by the LOW reader.
		if s.getTaskPriority(item) != enumsspb.TASK_PRIORITY_HIGH {
			continue
		}
		if _, wasThrottled := s.throttledCatchupNamespaces[item.GetNamespaceID()]; !wasThrottled {
			continue
		}
		if !s.shouldProcessTask(item) {
			continue
		}
		// Non-blocking re-pause check: if THROTTLED paused again, update the catchup start
		// and send a new anchor beacon so the synthetic HIGH watermark stays correct,
		// then return so the HIGH goroutine is not blocked.
		if s.flowController.IsPaused(enumsspb.TASK_PRIORITY_THROTTLED) {
			if s.throttledCatchupStart != item.GetTaskID() {
				s.throttledCatchupStart = item.GetTaskID()
				if err := s.sendThrottledAnchor(item.GetTaskID(), item.GetVisibilityTime()); err != nil {
					return err
				}
			}
			s.logger.Info("THROTTLED catchup interrupted by re-pause", tag.NewInt64("taskID", item.GetTaskID()))
			return nil
		}

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
					metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_THROTTLED),
				)
			}()
			task, err := s.taskConverter.Convert(item, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_THROTTLED)
			if err != nil {
				return err
			}
			if task == nil {
				return nil
			}
			task.Priority = enumsspb.TASK_PRIORITY_THROTTLED
			if err := s.flowController.Wait(s.server.Context(), enumsspb.TASK_PRIORITY_THROTTLED); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
			}
			return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ReplicationTasks:           []*replicationspb.ReplicationTask{task},
						ExclusiveHighWatermark:     task.SourceTaskId + 1,
						ExclusiveHighWatermarkTime: task.VisibilityTime,
						Priority:                   enumsspb.TASK_PRIORITY_THROTTLED,
					},
				},
			})
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
			metrics.ReplicationTaskPriorityTag(enumsspb.TASK_PRIORITY_THROTTLED),
		)
		if err != nil {
			return fmt.Errorf("sendThrottledCatchup failed to send task: %v, cause: %w", item, err)
		}
	}

	s.logger.Info("THROTTLED catchup complete", tag.NewInt64("start", start), tag.NewInt64("end", end))
	s.throttledCatchupStart = 0
	s.throttledCatchupNamespaces = nil
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

// getEffectiveTaskPriority returns the task priority to use for routing decisions.
// In tiered-stack mode, HIGH-priority tasks whose namespace is currently reported as
// throttled by the receiver are sent with THROTTLED priority. The receiver continues
// to run THROTTLED tasks through the namespace rate limiter so the namespace can
// recover and be promoted back to HIGH priority.
func (s *StreamSenderImpl) getEffectiveTaskPriority(task tasks.Task) enumsspb.TaskPriority {
	p := s.getTaskPriority(task)
	if p == enumsspb.TASK_PRIORITY_HIGH && s.isTieredStackEnabled {
		if m, ok := s.throttledNamespaceIDs.Load().(map[string]struct{}); ok {
			if _, throttled := m[task.GetNamespaceID()]; throttled {
				return enumsspb.TASK_PRIORITY_THROTTLED
			}
		}
	}
	return p
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
