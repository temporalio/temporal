//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_receiver_mock.go

package replication

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	ctasks "go.temporal.io/server/common/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ReceiverModeUnset       ReceiverMode = 0
	ReceiverModeSingleStack ReceiverMode = 1 // default mode. It only uses High Priority Task Tracker for processing tasks.
	ReceiverModeTieredStack ReceiverMode = 2
)

type (
	ReceiverMode   int32
	StreamReceiver interface {
		IsValid() bool
		Key() ClusterShardKeyPair
		Stop()
	}
	StreamReceiverImpl struct {
		ProcessToolBox

		status                  int32
		clientShardKey          ClusterShardKey
		serverShardKey          ClusterShardKey
		highPriorityTaskTracker ExecutableTaskTracker
		lowPriorityTaskTracker  ExecutableTaskTracker
		shutdownChan            channel.ShutdownOnce
		logger                  log.Logger
		stream                  Stream
		taskConverter           ExecutableTaskConverter
		receiverMode            ReceiverMode
		flowController          ReceiverFlowController
		recvSignalChan          chan struct{}
	}
)

func NewClusterShardKey(
	ClusterID int32,
	ClusterShardID int32,
) ClusterShardKey {
	return ClusterShardKey{
		ClusterID: ClusterID,
		ShardID:   ClusterShardID,
	}
}

func NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) *StreamReceiverImpl {
	logger := log.With(
		processToolBox.Logger,
		tag.SourceCluster(processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))),
		tag.SourceShardID(serverShardKey.ShardID),
		tag.ShardID(clientShardKey.ShardID), // client is the local cluster (target cluster, passive cluster)
		tag.Operation("replication-stream-receiver"),
	)
	highPriorityTaskTracker := NewExecutableTaskTracker(logger, processToolBox.MetricsHandler)
	lowPriorityTaskTracker := NewExecutableTaskTracker(logger, processToolBox.MetricsHandler)
	taskTrackerMap := make(map[enumsspb.TaskPriority]FlowControlSignalProvider)
	taskTrackerMap[enumsspb.TASK_PRIORITY_HIGH] = func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount: highPriorityTaskTracker.Size(),
		}
	}
	taskTrackerMap[enumsspb.TASK_PRIORITY_LOW] = func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount: lowPriorityTaskTracker.Size(),
		}
	}
	return &StreamReceiverImpl{
		ProcessToolBox: processToolBox,

		status:                  common.DaemonStatusInitialized,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		highPriorityTaskTracker: highPriorityTaskTracker,
		lowPriorityTaskTracker:  lowPriorityTaskTracker,
		shutdownChan:            channel.NewShutdownOnce(),
		logger:                  logger,
		stream: newStream(
			processToolBox,
			clientShardKey,
			serverShardKey,
		),
		taskConverter:  taskConverter,
		receiverMode:   ReceiverModeUnset,
		flowController: NewReceiverFlowControl(taskTrackerMap, processToolBox.Config),
		recvSignalChan: make(chan struct{}, 1),
	}
}

// Start starts the processor
func (r *StreamReceiverImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go WrapEventLoop(context.Background(), r.sendEventLoop, r.Stop, r.logger, r.MetricsHandler, r.clientShardKey, r.serverShardKey, r.Config)
	go WrapEventLoop(context.Background(), r.recvEventLoop, r.Stop, r.logger, r.MetricsHandler, r.clientShardKey, r.serverShardKey, r.Config)
	go livenessMonitor(
		r.recvSignalChan,
		r.Config.ReplicationStreamSendEmptyTaskDuration,
		r.Config.ReplicationStreamReceiverLivenessMultiplier,
		r.shutdownChan,
		r.Stop,
		r.logger,
	)
	r.logger.Info("StreamReceiver started.")
}

// Stop stops the processor
func (r *StreamReceiverImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.shutdownChan.Shutdown()
	r.stream.Close()
	r.highPriorityTaskTracker.Cancel()
	r.lowPriorityTaskTracker.Cancel()

	r.logger.Info("StreamReceiver shutting down.")
}

func (r *StreamReceiverImpl) IsValid() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStarted
}

func (r *StreamReceiverImpl) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: r.clientShardKey,
		Server: r.serverShardKey,
	}
}

func (r *StreamReceiverImpl) sendEventLoop() error {
	var panicErr error
	defer func() {
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(r.MetricsHandler).Record(1)
		}
	}()
	defer log.CapturePanic(r.logger, &panicErr)

	timer := time.NewTicker(r.Config.ReplicationStreamSyncStatusDuration())
	defer timer.Stop()

	var inclusiveLowWatermark int64

	for {
		select {
		case <-timer.C:
			watermark, err := r.ackMessage(r.stream)
			if err != nil {
				return err
			}
			if watermark != inclusiveLowWatermark {
				inclusiveLowWatermark = watermark
				r.logger.Debug(fmt.Sprintf("StreamReceiver acked inclusiveLowWatermark %d", inclusiveLowWatermark))
			}
		case <-r.shutdownChan.Channel():
			return nil
		}
	}
}

func (r *StreamReceiverImpl) recvEventLoop() error {
	var panicErr error
	defer func() {
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(r.MetricsHandler).Record(1)
		}
	}()
	defer log.CapturePanic(r.logger, &panicErr)

	err := r.processMessages(r.stream)
	if err == nil {
		return nil
	}
	return err
}

// ackMessage returns the inclusive low watermark if present.
func (r *StreamReceiverImpl) ackMessage(
	stream Stream,
) (int64, error) {
	highPriorityWaterMarkInfo := r.highPriorityTaskTracker.LowWatermark()
	lowPriorityWaterMarkInfo := r.lowPriorityTaskTracker.LowWatermark()
	size := r.highPriorityTaskTracker.Size() + r.lowPriorityTaskTracker.Size()

	var highPriorityWatermark, lowPriorityWatermark *replicationspb.ReplicationState
	inclusiveLowWaterMark := int64(-1)
	var inclusiveLowWaterMarkTime time.Time

	receiverMode := ReceiverMode(atomic.LoadInt32((*int32)(&r.receiverMode)))
	switch receiverMode {
	case ReceiverModeUnset:
		return 0, nil
	case ReceiverModeTieredStack:
		if highPriorityWaterMarkInfo == nil || lowPriorityWaterMarkInfo == nil {
			// This is to prevent the case: high priority task tracker received a batch of tasks, but low priority task tracker did not get any task yet.
			// i.e. HighPriorityTracker received watermark 10 and LowPriorityTracker did not receive any tasks yet.
			// we should avoid ack with {high: 10, low: nil}. If we do, sender will not able to correctly interpret the overall low watermark of the queue,
			// because it is possible that low priority tracker might receive a batch of tasks with watermark 5 later.
			// It is also true for the opposite case.
			r.logger.Warn("Tiered stack mode. Have to wait for both high and low priority tracker received at least one batch of tasks before acking.")
			return 0, nil
		}
		highPriorityFlowControlCommand := r.flowController.GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH)
		if highPriorityFlowControlCommand == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
			r.logger.Warn(fmt.Sprintf("pausing High Priority Tasks, current size: %v, lowWatermark: %v", r.highPriorityTaskTracker.Size(), highPriorityWaterMarkInfo.Watermark))
		}
		highPriorityWatermark = &replicationspb.ReplicationState{
			InclusiveLowWatermark:     highPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(highPriorityWaterMarkInfo.Timestamp),
			FlowControlCommand:        highPriorityFlowControlCommand,
		}
		lowPriorityFlowControlCommand := r.flowController.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
		if lowPriorityFlowControlCommand == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
			r.logger.Warn(fmt.Sprintf("pausing Low Priority Tasks, current size: %v, lowWatermark: %v", r.lowPriorityTaskTracker.Size(), lowPriorityWaterMarkInfo.Watermark))
		}
		lowPriorityWatermark = &replicationspb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(lowPriorityWaterMarkInfo.Timestamp),
			FlowControlCommand:        lowPriorityFlowControlCommand,
		}
		if highPriorityWaterMarkInfo.Watermark <= lowPriorityWaterMarkInfo.Watermark {
			inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
		} else {
			inclusiveLowWaterMark = lowPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = lowPriorityWaterMarkInfo.Timestamp
		}
	case ReceiverModeSingleStack:
		if highPriorityWaterMarkInfo == nil { // This should not happen, more for a safety check
			return 0, NewStreamError("Single stack mode. High priority tracker does not have low watermark info", serviceerror.NewInternal("Invalid tracker state"))
		}
		if lowPriorityWaterMarkInfo != nil {
			return 0, NewStreamError("Single stack mode. Should not receive low priority task", serviceerror.NewInternal("Invalid tracker state"))
		}
		inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
		inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
	}
	if inclusiveLowWaterMark == -1 {
		return 0, NewStreamError("InclusiveLowWaterMark is not set", serviceerror.NewInternal("Invalid inclusive low watermark"))
	}

	if err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     inclusiveLowWaterMark,
				InclusiveLowWatermarkTime: timestamppb.New(inclusiveLowWaterMarkTime),
				HighPriorityState:         highPriorityWatermark,
				LowPriorityState:          lowPriorityWatermark,
			},
		},
	}); err != nil {
		return 0, NewStreamError("stream_receiver failed to send", err)
	}
	metrics.ReplicationTasksRecvBacklog.With(r.MetricsHandler).Record(
		int64(size),
		metrics.FromClusterIDTag(r.serverShardKey.ClusterID),
		metrics.ToClusterIDTag(r.clientShardKey.ClusterID),
	)
	metrics.ReplicationTasksSend.With(r.MetricsHandler).Record(
		int64(1),
		metrics.FromClusterIDTag(r.clientShardKey.ClusterID),
		metrics.ToClusterIDTag(r.serverShardKey.ClusterID),
		metrics.OperationTag(metrics.SyncWatermarkScope),
	)
	return inclusiveLowWaterMark, nil
}

func (r *StreamReceiverImpl) processMessages(
	stream Stream,
) error {
	allClusterInfo := r.ClusterMetadata.GetAllClusterInfo()
	clusterName, _, err := ClusterIDToClusterNameShardCount(allClusterInfo, r.serverShardKey.ClusterID)
	if err != nil {
		return err
	}

	streamRespChen, err := stream.Recv()
	if err != nil {
		return NewStreamError("stream_receiver failed to recv", err)
	}
	for streamResp := range streamRespChen {
		select {
		case r.recvSignalChan <- struct{}{}:
		default:
			// signal channel is full. Continue
		}
		if streamResp.Err != nil {
			return streamResp.Err
		}
		if err := r.validateAndSetReceiverMode(streamResp.Resp.GetMessages().Priority); err != nil {
			// sender mode changed, exit loop and let stream reconnect to retry
			return NewStreamError("ReplicationTask wrong receiver mode", err)
		}

		if err = ValidateTasksHaveSamePriority(streamResp.Resp.GetMessages().Priority, streamResp.Resp.GetMessages().ReplicationTasks...); err != nil {
			// This should not happen because source side is sending task 1 by 1. Validate here just in case.
			return NewStreamError("ReplicationTask priority check failed", err)
		}
		convertedTasks := r.taskConverter.Convert(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			streamResp.Resp.GetMessages().ReplicationTasks...,
		)
		exclusiveHighWatermark := streamResp.Resp.GetMessages().ExclusiveHighWatermark
		exclusiveHighWatermarkTime := timestamp.TimeValue(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime)
		taskTracker, err := r.getTaskTracker(streamResp.Resp.GetMessages().Priority)
		if err != nil {
			// Todo: Change to write Tasks to DLQ. As resend task will not help here
			return NewStreamError("ReplicationTask wrong priority", err)
		}
		for _, task := range taskTracker.TrackTasks(WatermarkInfo{
			Watermark: exclusiveHighWatermark,
			Timestamp: exclusiveHighWatermarkTime,
		}, convertedTasks...) {
			scheduler, err := r.getTaskScheduler(streamResp.Resp.GetMessages().Priority, task)
			if err != nil {
				return err
			}
			scheduler.Submit(task)
		}
	}
	return nil
}

func (r *StreamReceiverImpl) getTaskTracker(priority enumsspb.TaskPriority) (ExecutableTaskTracker, error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED, enumsspb.TASK_PRIORITY_HIGH:
		return r.highPriorityTaskTracker, nil
	case enumsspb.TASK_PRIORITY_LOW:
		return r.lowPriorityTaskTracker, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("Unknown task priority: %v", priority)
	}
}

func (r *StreamReceiverImpl) getTaskScheduler(priority enumsspb.TaskPriority, task TrackableExecutableTask) (ctasks.Scheduler[TrackableExecutableTask], error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED:
		switch task.(type) {
		case *ExecutableWorkflowStateTask:
			// this is an optimization for workflow state task. The low priority task scheduler is grouping task by workflow ID.
			// When multiple runs of task come in, we can serialize them in the low priority task scheduler. As long as we use a single tracker,
			// the task ACK is guaranteed to be in order.(i.e. no task will be lost)
			return r.ProcessToolBox.LowPriorityTaskScheduler, nil
		}
		return r.ProcessToolBox.HighPriorityTaskScheduler, nil
	case enumsspb.TASK_PRIORITY_HIGH:
		return r.ProcessToolBox.HighPriorityTaskScheduler, nil
	case enumsspb.TASK_PRIORITY_LOW:
		return r.ProcessToolBox.LowPriorityTaskScheduler, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("Unknown task priority: %v", priority)
	}
}

// Receiver mode can only be set once for the lifetime of the receiver. Receiver mode is set when receiver receive the first task.
// If the first task is prioritized, receiver mode will be set to ReceiverModeTieredStack. If the first task is not prioritized, receiver mode will be set to ReceiverModeSingleStack.
// Receiver mode cannot be changed once it is set. If we enabled sender side to send tasks with different priority, we need to change the receiver mode by reconnecting the stream.
func (r *StreamReceiverImpl) validateAndSetReceiverMode(priority enumsspb.TaskPriority) error {
	receiverMode := ReceiverMode(atomic.LoadInt32((*int32)(&r.receiverMode)))
	switch receiverMode {
	case ReceiverModeUnset:
		r.setReceiverMode(priority)
		return nil
	case ReceiverModeSingleStack:
		if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("ReceiverModeSingleStack cannot process prioritized task")
		}
	case ReceiverModeTieredStack:
		if priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("ReceiverModeTieredStack cannot process non-prioritized task")
		}
	}
	return nil
}

func (r *StreamReceiverImpl) setReceiverMode(priority enumsspb.TaskPriority) {
	if r.receiverMode != ReceiverModeUnset {
		return
	}
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeSingleStack))
	case enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeTieredStack))
	}
}

func ValidateTasksHaveSamePriority(messageBatchPriority enumsspb.TaskPriority, tasks ...*replicationspb.ReplicationTask) error {
	if len(tasks) == 0 || messageBatchPriority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
		return nil
	}
	for _, task := range tasks {
		if task.Priority != messageBatchPriority {
			return serviceerror.NewInvalidArgumentf("Task priority does not match batch priority: %v, %v", task.Priority, messageBatchPriority)
		}
	}
	return nil
}

func newStream(
	processToolBox ProcessToolBox,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) Stream {
	var clientProvider BiDirectionStreamClientProvider[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse] = &streamClientProvider{
		processToolBox: processToolBox,
		clientShardKey: clientShardKey,
		serverShardKey: serverShardKey,
	}
	return NewBiDirectionStream(
		clientProvider,
		processToolBox.MetricsHandler,
		log.With(processToolBox.Logger, tag.ShardID(clientShardKey.ShardID)),
	)
}

type streamClientProvider struct {
	processToolBox ProcessToolBox
	clientShardKey ClusterShardKey
	serverShardKey ClusterShardKey
}

var _ BiDirectionStreamClientProvider[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse] = (*streamClientProvider)(nil)

func (p *streamClientProvider) Get(
	ctx context.Context,
) (BiDirectionStreamClient[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	return NewStreamBiDirectionStreamClientProvider(
		p.processToolBox.ClusterMetadata,
		p.processToolBox.ClientBean,
	).Get(ctx, p.clientShardKey, p.serverShardKey)
}
