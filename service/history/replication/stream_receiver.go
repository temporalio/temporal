//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination stream_receiver_mock.go

package replication

import (
	"context"
	"fmt"
	"maps"
	"sync"
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
	"go.temporal.io/server/common/testing/testhooks"
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
		// memberLanes holds one lane per isolated namespace, created lazily as the
		// sender tags messages with isolated_namespace_id. Each lane is its own
		// monotonic stream for the life of the stream connection; a retired lane
		// (the sender merged the namespace back) is dropped once its pending work
		// drains. Tasks are still scheduled at the message's priority.
		memberLaneMu      sync.Mutex
		memberLanes       map[string]*memberLane
		memberLanesClosed bool // set by Stop; late-created lanes are pre-cancelled
		shutdownChan      channel.ShutdownOnce
		logger            log.Logger
		stream            Stream
		taskConverter     ExecutableTaskConverter
		receiverMode      ReceiverMode
		flowController    ReceiverFlowController
		recvSignalChan    chan struct{}

		slowSubmissionMu         sync.RWMutex
		slowSubmissionTimestamps map[enumsspb.TaskPriority]time.Time
	}

	executeInterceptedTask struct {
		TrackableExecutableTask
		execute func() error
	}

	// memberLane is the receiver-side state of one isolated namespace's lane.
	memberLane struct {
		tracker ExecutableTaskTracker
		// retiring is set when the sender marks the lane retired; the lane is
		// dropped once its tracker drains.
		retiring bool
		// tracking counts batches resolved to this lane whose TrackTasks call has
		// not finished yet; the ack loop must not drop the lane while one is in
		// flight.
		tracking int
	}
)

func (t executeInterceptedTask) Execute() error {
	return t.execute()
}

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
	receiver := &StreamReceiverImpl{
		ProcessToolBox: processToolBox,

		status:                  common.DaemonStatusInitialized,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		highPriorityTaskTracker: highPriorityTaskTracker,
		lowPriorityTaskTracker:  lowPriorityTaskTracker,
		memberLanes:             make(map[string]*memberLane),
		shutdownChan:            channel.NewShutdownOnce(),
		logger:                  logger,
		stream: newStream(
			processToolBox,
			clientShardKey,
			serverShardKey,
		),
		taskConverter:            taskConverter,
		receiverMode:             ReceiverModeUnset,
		slowSubmissionTimestamps: make(map[enumsspb.TaskPriority]time.Time),
		recvSignalChan:           make(chan struct{}, 1),
	}
	taskTrackerMap := make(map[enumsspb.TaskPriority]FlowControlSignalProvider)
	taskTrackerMap[enumsspb.TASK_PRIORITY_HIGH] = func() *FlowControlSignal {
		return &FlowControlSignal{
			// Include the isolated per-namespace lane trackers: isolation splits the
			// HIGH lane, so flow control protects against total HIGH backlog (default
			// lane + member lanes).
			taskTrackingCount:  receiver.highFamilyTrackingCount(),
			lastSlowSubmission: receiver.getLastSlowSubmissionTimestamp(enumsspb.TASK_PRIORITY_HIGH),
		}
	}
	taskTrackerMap[enumsspb.TASK_PRIORITY_LOW] = func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  lowPriorityTaskTracker.Size(),
			lastSlowSubmission: receiver.getLastSlowSubmissionTimestamp(enumsspb.TASK_PRIORITY_LOW),
		}
	}
	receiver.flowController = NewReceiverFlowControl(taskTrackerMap, processToolBox.Config)
	if receiver.NamespaceThrottler == nil {
		receiver.NamespaceThrottler = NoopNamespaceThrottler{}
	}
	return receiver
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
	go maxLifetimeMonitor(
		r.Config.ReplicationStreamMaxLifetime,
		r.Config.ReplicationStreamMaxLifetimeJitter,
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
	r.memberLaneMu.Lock()
	r.memberLanesClosed = true
	for _, lane := range r.memberLanes {
		lane.tracker.Cancel()
	}
	r.memberLaneMu.Unlock()

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
	size := r.highFamilyTrackingCount() + r.lowPriorityTaskTracker.Size()

	var highPriorityWatermark, lowPriorityWatermark *replicationspb.ReplicationState
	var isolatedLaneStates map[string]*replicationspb.ReplicationState
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
		highPriorityFlowControlInfo := r.flowController.GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH)
		if highPriorityFlowControlInfo.Command == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
			r.logger.Warn(fmt.Sprintf("pausing High Priority Tasks: %s", highPriorityFlowControlInfo.Cause))
		}
		highPriorityWatermark = &replicationspb.ReplicationState{
			InclusiveLowWatermark:     highPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(highPriorityWaterMarkInfo.Timestamp),
			FlowControlCommand:        highPriorityFlowControlInfo.Command,
		}
		lowPriorityFlowControlInfo := r.flowController.GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW)
		if lowPriorityFlowControlInfo.Command == enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE {
			r.logger.Warn(fmt.Sprintf("pausing Low Priority Tasks: %s", lowPriorityFlowControlInfo.Cause))
		}
		lowPriorityWatermark = &replicationspb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(lowPriorityWaterMarkInfo.Timestamp),
			FlowControlCommand:        lowPriorityFlowControlInfo.Command,
		}
		if highPriorityWaterMarkInfo.Watermark <= lowPriorityWaterMarkInfo.Watermark {
			inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
		} else {
			inclusiveLowWaterMark = lowPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = lowPriorityWaterMarkInfo.Timestamp
		}
		// Fold isolated member lanes into the overall min so Scopes[0] on the sender
		// (and thus cleanup) accounts for a lagging lane: the overall min must be the
		// lowest point in flight across every lane. Report each lane's watermark keyed
		// by namespace so the sender can anchor per-member state on applied points.
		memberWms := r.memberLaneWatermarks()
		if len(memberWms) > 0 {
			isolatedLaneStates = make(map[string]*replicationspb.ReplicationState, len(memberWms))
		}
		for ns, wm := range memberWms {
			if wm.Watermark < inclusiveLowWaterMark {
				inclusiveLowWaterMark = wm.Watermark
				inclusiveLowWaterMarkTime = wm.Timestamp
			}
			isolatedLaneStates[ns] = &replicationspb.ReplicationState{
				InclusiveLowWatermark:     wm.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(wm.Timestamp),
			}
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

	var throttleHighNamespaceIDs []string
	if receiverMode == ReceiverModeTieredStack {
		throttleHighNamespaceIDs = r.NamespaceThrottler.ThrottledNamespaceIDs(r.clientShardKey.ShardID)
	}
	if err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     inclusiveLowWaterMark,
				InclusiveLowWatermarkTime: timestamppb.New(inclusiveLowWaterMarkTime),
				HighPriorityState:         highPriorityWatermark,
				LowPriorityState:          lowPriorityWatermark,
				ThrottleHighNamespaceIds:  throttleHighNamespaceIDs,
				IsolatedLaneStates:        isolatedLaneStates,
				// Advertise that this receiver routes isolated_namespace_id-tagged
				// messages, so the sender may safely emit lane traffic to it.
				// Isolation only applies to the tiered stack.
				SupportsNamespaceIsolation: receiverMode == ReceiverModeTieredStack,
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

		messages := streamResp.Resp.GetMessages()
		priority := messages.Priority

		if err := r.validateAndSetReceiverMode(priority); err != nil {
			// sender mode changed, exit loop and let stream reconnect to retry
			return NewStreamError("ReplicationTask wrong receiver mode", err)
		}

		if err = ValidateTasksHaveSamePriority(priority, messages.ReplicationTasks...); err != nil {
			// This should not happen because source side is sending task 1 by 1. Validate here just in case.
			return NewStreamError("ReplicationTask priority check failed", err)
		}

		convertedTasks := r.taskConverter.Convert(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			messages.ReplicationTasks...,
		)
		exclusiveHighWatermark := messages.ExclusiveHighWatermark
		exclusiveHighWatermarkTime := timestamp.TimeValue(messages.ExclusiveHighWatermarkTime)
		// Route to the per-lane tracker: an isolated namespace's lane gets its own
		// monotonic tracker; unset routes by priority (HIGH / default-LOW).
		taskTracker, err := r.getTaskTrackerForLane(priority, messages.GetIsolatedNamespaceId(), messages.GetRetireIsolatedLane())
		if err != nil {
			// Todo: Change to write Tasks to DLQ. As resend task will not help here
			return NewStreamError("ReplicationTask wrong priority", err)
		}

		if priority == enumsspb.TASK_PRIORITY_HIGH {
			// Isolation acts on HIGH (live) traffic: feed the throttler every HIGH task,
			// including isolated-lane tasks, so an isolated namespace stays observable
			// and is released once its HIGH rate drops. Load is scoped to the local shard.
			for _, task := range convertedTasks {
				if nsID := task.ReplicationTask().GetRawTaskInfo().GetNamespaceId(); nsID != "" {
					r.NamespaceThrottler.RecordTask(r.clientShardKey.ShardID, nsID)
				}
			}
		}

		submissionThreshold := r.Config.ReplicationReceiverSlowSubmissionLatencyThreshold()

		trackedTasks := taskTracker.TrackTasks(WatermarkInfo{
			Watermark: exclusiveHighWatermark,
			Timestamp: exclusiveHighWatermarkTime,
		}, convertedTasks...)
		if isolatedNamespaceID := messages.GetIsolatedNamespaceId(); isolatedNamespaceID != "" {
			// Finish strictly after TrackTasks: the ack loop may only delete a
			// retiring lane once no batch sits between lane resolution and tracking,
			// or the batch would be tracked on an orphaned lane and dropped from the
			// ack fold.
			r.finishLaneBatch(isolatedNamespaceID, messages.GetRetireIsolatedLane())
		}
		for _, task := range trackedTasks {
			schedulerPriority, err := r.getTaskSchedulerPriority(priority, task)
			if err != nil {
				return err
			}
			scheduler, err := r.getTaskScheduler(schedulerPriority)
			if err != nil {
				return err
			}

			// Tests use this hook to pause replication tasks before they are applied.
			if hook, ok := testhooks.Get(r.TestHooks, testhooks.HistoryReplicationTaskInterceptor, testhooks.GlobalScope); ok {
				originalTask := task
				task = executeInterceptedTask{
					TrackableExecutableTask: originalTask,
					execute: func() error {
						return hook(originalTask.ReplicationTask(), originalTask.Execute)
					},
				}
			}

			start := time.Now()
			scheduler.Submit(task)
			end := time.Now()
			if end.Sub(start) > submissionThreshold {
				r.recordSlowSubmission(schedulerPriority, end)
			}
		}
	}
	return nil
}

func (r *StreamReceiverImpl) getLastSlowSubmissionTimestamp(priority enumsspb.TaskPriority) time.Time {
	r.slowSubmissionMu.RLock()
	defer r.slowSubmissionMu.RUnlock()
	if ts, ok := r.slowSubmissionTimestamps[priority]; ok {
		return ts
	}
	return time.Time{}
}

func (r *StreamReceiverImpl) recordSlowSubmission(priority enumsspb.TaskPriority, ts time.Time) {
	r.slowSubmissionMu.Lock()
	defer r.slowSubmissionMu.Unlock()
	r.slowSubmissionTimestamps[priority] = ts
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

// getTaskTrackerForLane returns the tracker for a message's lane: an isolated
// namespace's lane gets its own lazily-created tracker so each lane stays a single
// monotonic stream; an empty namespace routes by priority (HIGH / default-LOW).
// Every lane resolution must be paired with finishLaneBatch after the batch's
// TrackTasks call, so the ack loop never deletes the lane mid-batch.
func (r *StreamReceiverImpl) getTaskTrackerForLane(priority enumsspb.TaskPriority, isolatedNamespaceID string, retire bool) (ExecutableTaskTracker, error) {
	if isolatedNamespaceID == "" {
		return r.getTaskTracker(priority)
	}
	if priority != enumsspb.TASK_PRIORITY_HIGH {
		// Isolation splits the HIGH lane only. Lane-tagged traffic at any other
		// priority (including single-stack UNSPECIFIED, whose ack path would never
		// fold a member lane) is a sender bug — fail the stream rather than
		// silently mis-acking.
		return nil, serviceerror.NewInternalf("isolated lane traffic must be HIGH priority, got %v", priority)
	}
	r.memberLaneMu.Lock()
	defer r.memberLaneMu.Unlock()
	lane, ok := r.memberLanes[isolatedNamespaceID]
	if !ok {
		lane = &memberLane{tracker: NewExecutableTaskTracker(r.logger, r.MetricsHandler)}
		if r.memberLanesClosed {
			// Lost the race with Stop(): pre-cancel so tracked tasks are
			// cancelled rather than executed after shutdown.
			lane.tracker.Cancel()
		}
		r.memberLanes[isolatedNamespaceID] = lane
	}
	if !retire && lane.retiring {
		// Non-retire traffic on a retired lane means the sender re-isolated the
		// namespace before the lane drained: the lane is live again. Without this,
		// the stale retiring flag would delete an active lane at the first
		// transient drain, dropping its watermark from the ack fold.
		lane.retiring = false
	}
	lane.tracking++
	return lane.tracker, nil
}

// finishLaneBatch completes a batch that getTaskTrackerForLane resolved to a lane,
// called after the batch's TrackTasks: it releases the batch's hold on the lane and
// applies the message's retire flag. A retiring lane is only dropped once it is
// drained and no batch is mid-track — so a retired lane can never pin the overall
// ack minimum, and a batch can never be orphaned onto a deleted lane.
func (r *StreamReceiverImpl) finishLaneBatch(isolatedNamespaceID string, retire bool) {
	r.memberLaneMu.Lock()
	defer r.memberLaneMu.Unlock()
	lane, ok := r.memberLanes[isolatedNamespaceID]
	if !ok {
		return
	}
	lane.tracking--
	if retire {
		lane.retiring = true
	}
}

// highFamilyTrackingCount is the total tracked-task count across the default HIGH lane
// and all isolated member lanes (used for HIGH flow control — isolation splits the
// HIGH lane).
func (r *StreamReceiverImpl) highFamilyTrackingCount() int {
	count := r.highPriorityTaskTracker.Size()
	r.memberLaneMu.Lock()
	for _, lane := range r.memberLanes {
		count += lane.tracker.Size()
	}
	r.memberLaneMu.Unlock()
	return count
}

// memberLaneWatermarks snapshots the low watermark of every isolated member lane that
// has received at least one batch, keyed by namespace id. Retired lanes are dropped
// once drained.
func (r *StreamReceiverImpl) memberLaneWatermarks() map[string]WatermarkInfo {
	r.memberLaneMu.Lock()
	lanes := make(map[string]*memberLane, len(r.memberLanes))
	maps.Copy(lanes, r.memberLanes)
	r.memberLaneMu.Unlock()

	out := make(map[string]WatermarkInfo, len(lanes))
	for ns, lane := range lanes {
		wm := lane.tracker.LowWatermark()

		r.memberLaneMu.Lock()
		currentLane, ok := r.memberLanes[ns]
		if !ok || currentLane != lane {
			r.memberLaneMu.Unlock()
			continue
		}
		// Drop only lanes that are retiring, drained, have tracked at least one
		// batch (wm != nil), and have no batch between lane resolution and
		// tracking: deleting a lane mid-batch would orphan that batch outside the
		// ack fold.
		if lane.retiring && lane.tracking == 0 && lane.tracker.Size() == 0 && wm != nil {
			delete(r.memberLanes, ns)
			r.memberLaneMu.Unlock()
			continue
		}
		if wm != nil {
			out[ns] = *wm
		}
		r.memberLaneMu.Unlock()
	}
	return out
}

func (r *StreamReceiverImpl) getTaskSchedulerPriority(priority enumsspb.TaskPriority, task TrackableExecutableTask) (enumsspb.TaskPriority, error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED:
		switch task.(type) {
		case *ExecutableWorkflowStateTask:
			// This is an optimization for workflow state task. The low priority task scheduler is grouping task by workflow ID.
			// When multiple runs of task come in, we can serialize them in the low priority task scheduler. As long as we use a single tracker,
			// the task ACK is guaranteed to be in order.(i.e. no task will be lost)
			return enumsspb.TASK_PRIORITY_LOW, nil
		}
		return enumsspb.TASK_PRIORITY_HIGH, nil
	case enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW:
		return priority, nil
	default:
		return 0, serviceerror.NewInvalidArgumentf("Unknown task priority: %v", priority)
	}
}

func (r *StreamReceiverImpl) getTaskScheduler(priority enumsspb.TaskPriority) (ctasks.Scheduler[TrackableExecutableTask], error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_HIGH:
		return r.HighPriorityTaskScheduler, nil
	case enumsspb.TASK_PRIORITY_LOW:
		return r.LowPriorityTaskScheduler, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("Unknown task scheduler priority: %v", priority)
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
