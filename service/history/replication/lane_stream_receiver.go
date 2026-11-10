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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// replicationLaneProtocolVersion is the version of the replication lane protocol
// implemented by the lane stream sender and receiver.
const replicationLaneProtocolVersion = 1

type laneStreamReceiver struct {
	ProcessToolBox

	status                  int32
	clientShardKey          ClusterShardKey
	serverShardKey          ClusterShardKey
	highPriorityTaskTracker ExecutableTaskTracker
	lowPriorityTaskTracker  ExecutableTaskTracker
	// lanesEnabled is whether this receiver accepts replication lanes, and maxLanes how many
	// it tracks at once. Both are fixed for the life of the stream.
	lanesEnabled bool
	maxLanes     int
	laneFeedback LaneFeedbackProvider
	// memberLanes holds one tracker per replication lane, keyed by the lane ID in
	// WorkflowReplicationMessages.lane_info and created lazily. Each lane is its own
	// monotonic stream for the life of the stream connection. A retired lane is
	// dropped once its pending work drains. Tasks are still scheduled at the
	// message's priority.
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

func newLaneStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	laneFeedback LaneFeedbackProvider,
) *laneStreamReceiver {
	logger := log.With(
		processToolBox.Logger,
		tag.SourceCluster(processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))),
		tag.SourceShardID(serverShardKey.ShardID),
		tag.ShardID(clientShardKey.ShardID), // client is the local cluster (target cluster, passive cluster)
		tag.Operation("replication-stream-receiver"),
	)
	highPriorityTaskTracker := NewExecutableTaskTracker(logger, processToolBox.MetricsHandler)
	lowPriorityTaskTracker := NewExecutableTaskTracker(logger, processToolBox.MetricsHandler)
	receiver := &laneStreamReceiver{
		ProcessToolBox: processToolBox,

		status:                  common.DaemonStatusInitialized,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		highPriorityTaskTracker: highPriorityTaskTracker,
		lowPriorityTaskTracker:  lowPriorityTaskTracker,
		lanesEnabled:            processToolBox.Config.ReplicationStreamReceiverLanesEnabled(),
		maxLanes:                processToolBox.Config.ReplicationStreamReceiverMaxLanes(),
		laneFeedback:            laneFeedback,
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
	taskTrackerMap[enumsspb.TASK_PRIORITY_HIGH] = receiver.highPriorityFlowControlSignal
	taskTrackerMap[enumsspb.TASK_PRIORITY_LOW] = func() *FlowControlSignal {
		return &FlowControlSignal{
			taskTrackingCount:  lowPriorityTaskTracker.Size(),
			lastSlowSubmission: receiver.getLastSlowSubmissionTimestamp(enumsspb.TASK_PRIORITY_LOW),
		}
	}
	receiver.flowController = NewReceiverFlowControl(taskTrackerMap, processToolBox.Config)
	return receiver
}

// Start starts the processor
func (r *laneStreamReceiver) Start() {
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
func (r *laneStreamReceiver) Stop() {
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

// IsValid returns false once ReplicationStreamReceiverLanesEnabled changes, so that the stream
// reconnects and advertises the new lane support to its sender.
func (r *laneStreamReceiver) IsValid() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStarted &&
		r.Config.ReplicationStreamReceiverLanesEnabled() == r.lanesEnabled &&
		r.Config.ReplicationStreamReceiverMaxLanes() == r.maxLanes
}

func (r *laneStreamReceiver) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: r.clientShardKey,
		Server: r.serverShardKey,
	}
}

func (r *laneStreamReceiver) feedbackStream() LaneFeedbackStream {
	return LaneFeedbackStream{Source: r.serverShardKey, Target: r.clientShardKey}
}

func (r *laneStreamReceiver) sendEventLoop() error {
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

func (r *laneStreamReceiver) recvEventLoop() error {
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
func (r *laneStreamReceiver) ackMessage(
	stream Stream,
) (int64, error) {
	highPriorityWaterMarkInfo := r.highPriorityTaskTracker.LowWatermark()
	lowPriorityWaterMarkInfo := r.lowPriorityTaskTracker.LowWatermark()
	size := r.highFamilyTrackingCount() + r.lowPriorityTaskTracker.Size()

	var highPriorityWatermark, lowPriorityWatermark *replicationspb.ReplicationState
	var laneStates map[string]*replicationspb.ReplicationState
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
		// Fold lanes into the overall min so Scopes[0] on the sender (and thus
		// cleanup) accounts for a lagging lane: the overall min must be the lowest
		// point in flight across every lane. Report each lane's watermark keyed by
		// lane ID so the sender can track each lane's progress.
		memberWms := r.memberLaneWatermarks()
		if len(memberWms) > 0 {
			laneStates = make(map[string]*replicationspb.ReplicationState, len(memberWms))
		}
		for laneID, wm := range memberWms {
			if wm.Watermark < inclusiveLowWaterMark {
				inclusiveLowWaterMark = wm.Watermark
				inclusiveLowWaterMarkTime = wm.Timestamp
			}
			laneStates[laneID] = &replicationspb.ReplicationState{
				InclusiveLowWatermark:     wm.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(wm.Timestamp),
				FlowControlCommand:        r.laneFlowControlCommand(laneID),
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
	default:
	}
	if inclusiveLowWaterMark == -1 {
		return 0, NewStreamError("InclusiveLowWaterMark is not set", serviceerror.NewInternal("Invalid inclusive low watermark"))
	}

	var throttleHighNamespaceIDs []string
	var laneFeedback *anypb.Any
	var maxLanes int32
	laneProtocolVersion := r.laneProtocolVersion(receiverMode)
	if receiverMode == ReceiverModeTieredStack {
		throttleHighNamespaceIDs = r.laneFeedback.ThrottledNamespaceIDs(r.feedbackStream())
	}
	if laneProtocolVersion > 0 {
		maxLanes = int32(r.maxLanes)
		if maxBytes := r.Config.ReplicationStreamReceiverLaneFeedbackMaxBytes(); maxBytes > 0 {
			laneFeedback = r.laneFeedback.Feedback(r.feedbackStream(), maxBytes)
			if size := proto.Size(laneFeedback); size > maxBytes {
				metrics.ReplicationLaneFeedbackDropped.With(r.MetricsHandler).Record(
					1,
					metrics.FromClusterIDTag(r.serverShardKey.ClusterID),
					metrics.ToClusterIDTag(r.clientShardKey.ClusterID),
				)
				r.logger.Warn("Dropping replication lane feedback larger than the configured limit.", tag.NewInt("size", size), tag.NewInt("limit", maxBytes))
				laneFeedback = nil
			}
		}
	}
	err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     inclusiveLowWaterMark,
				InclusiveLowWatermarkTime: timestamppb.New(inclusiveLowWaterMarkTime),
				HighPriorityState:         highPriorityWatermark,
				LowPriorityState:          lowPriorityWatermark,
				ThrottleHighNamespaceIds:  throttleHighNamespaceIDs,
				LaneStates:                laneStates,
				LaneProtocolVersion:       laneProtocolVersion,
				LaneFeedback:              laneFeedback,
				MaxLanes:                  maxLanes,
			},
		},
	})
	if err != nil {
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

//nolint:revive // cognitive complexity matches StreamReceiverImpl.processMessages, which this copies.
func (r *laneStreamReceiver) processMessages(
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
		laneID, err := r.messageLaneID(messages)
		if err != nil {
			return NewStreamError("ReplicationTask lane check failed", err)
		}
		// Route to the per-lane tracker: a lane gets its own monotonic tracker; no lane
		// routes by priority (HIGH / default-LOW).
		taskTracker, err := r.getTaskTrackerForLane(priority, laneID, messages.GetLaneInfo().GetRetire())
		if err != nil {
			// Todo: Change to write Tasks to DLQ. As resend task will not help here
			return NewStreamError("ReplicationTask wrong priority", err)
		}

		if priority == enumsspb.TASK_PRIORITY_HIGH {
			// Feed the provider every HIGH task, including lane tasks, so an isolated
			// namespace stays observable and can be released once its rate drops.
			for _, task := range convertedTasks {
				if nsID := task.ReplicationTask().GetRawTaskInfo().GetNamespaceId(); nsID != "" {
					r.laneFeedback.RecordTask(r.feedbackStream(), nsID)
				}
			}
		}

		submissionThreshold := r.Config.ReplicationReceiverSlowSubmissionLatencyThreshold()

		trackedTasks := taskTracker.TrackTasks(WatermarkInfo{
			Watermark: exclusiveHighWatermark,
			Timestamp: exclusiveHighWatermarkTime,
		}, convertedTasks...)
		if laneID != "" {
			// Finish strictly after TrackTasks: the ack loop may only delete a
			// retiring lane once no batch sits between lane resolution and tracking,
			// or the batch would be tracked on an orphaned lane and dropped from the
			// ack fold.
			r.finishLaneBatch(laneID, messages.GetLaneInfo().GetRetire())
		}
		for _, task := range trackedTasks {
			if r.PayloadObserver != nil {
				observePayload(r.PayloadObserver, task.ReplicationTask(), clusterName, r.serverShardKey.ShardID, PayloadReceived, time.Now())
			}
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

func (r *laneStreamReceiver) getLastSlowSubmissionTimestamp(priority enumsspb.TaskPriority) time.Time {
	r.slowSubmissionMu.RLock()
	defer r.slowSubmissionMu.RUnlock()
	if ts, ok := r.slowSubmissionTimestamps[priority]; ok {
		return ts
	}
	return time.Time{}
}

func (r *laneStreamReceiver) recordSlowSubmission(priority enumsspb.TaskPriority, ts time.Time) {
	r.slowSubmissionMu.Lock()
	defer r.slowSubmissionMu.Unlock()
	r.slowSubmissionTimestamps[priority] = ts
}

func (r *laneStreamReceiver) getTaskTracker(priority enumsspb.TaskPriority) (ExecutableTaskTracker, error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED, enumsspb.TASK_PRIORITY_HIGH:
		return r.highPriorityTaskTracker, nil
	case enumsspb.TASK_PRIORITY_LOW:
		return r.lowPriorityTaskTracker, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("Unknown task priority: %v", priority)
	}
}

// getTaskTrackerForLane returns the tracker for a message's lane: a lane gets its own
// lazily-created tracker so each lane stays a single monotonic stream; an empty lane
// ID routes by priority (HIGH / default-LOW).
// Every lane resolution must be paired with finishLaneBatch after the batch's
// TrackTasks call, so the ack loop never deletes the lane mid-batch.
func (r *laneStreamReceiver) getTaskTrackerForLane(priority enumsspb.TaskPriority, laneID string, retire bool) (ExecutableTaskTracker, error) {
	if laneID == "" {
		return r.getTaskTracker(priority)
	}
	if priority != enumsspb.TASK_PRIORITY_HIGH {
		// Lanes split the HIGH lane only. Lane-tagged traffic at any other
		// priority (including single-stack UNSPECIFIED, whose ack path would never
		// fold a member lane) is a sender bug — fail the stream rather than
		// silently mis-acking.
		return nil, serviceerror.NewInternalf("lane traffic must be HIGH priority, got %v", priority)
	}
	r.memberLaneMu.Lock()
	defer r.memberLaneMu.Unlock()
	lane, ok := r.memberLanes[laneID]
	if !ok {
		if open := r.openLaneCount(); open >= r.maxLanes {
			return nil, serviceerror.NewInvalidArgumentf("sender opened a replication lane beyond max_lanes %d with %d lanes open", r.maxLanes, open)
		}
		lane = &memberLane{tracker: NewExecutableTaskTracker(r.logger, r.MetricsHandler)}
		if r.memberLanesClosed {
			// Lost the race with Stop(): pre-cancel so tracked tasks are
			// cancelled rather than executed after shutdown.
			lane.tracker.Cancel()
		}
		r.memberLanes[laneID] = lane
	}
	if !retire && lane.retiring {
		// Non-retire traffic on a retired lane means the sender reused the lane
		// before it drained: the lane is live again. Without this,
		// the stale retiring flag would delete an active lane at the first
		// transient drain, dropping its watermark from the ack fold.
		lane.retiring = false
	}
	lane.tracking++
	return lane.tracker, nil
}

// openLaneCount returns how many lanes have not been retired. A retired lane still drains, but
// no longer counts toward max_lanes: the sender sends its retirement marker before it opens
// another lane. The caller holds memberLaneMu.
func (r *laneStreamReceiver) openLaneCount() int {
	open := 0
	for _, lane := range r.memberLanes {
		if !lane.retiring {
			open++
		}
	}
	return open
}

// finishLaneBatch completes a batch that getTaskTrackerForLane resolved to a lane,
// called after the batch's TrackTasks: it releases the batch's hold on the lane and
// applies the message's retire flag. A retiring lane is only dropped once it is
// drained and no batch is mid-track — so a retired lane can never pin the overall
// ack minimum, and a batch can never be orphaned onto a deleted lane.
func (r *laneStreamReceiver) finishLaneBatch(laneID string, retire bool) {
	r.memberLaneMu.Lock()
	defer r.memberLaneMu.Unlock()
	lane, ok := r.memberLanes[laneID]
	if !ok {
		return
	}
	lane.tracking--
	if retire {
		lane.retiring = true
	}
}

// highFamilyTrackingCount is the total tracked-task count across the default HIGH lane
// and all lanes (used for HIGH flow control — lanes split the HIGH lane).
// highPriorityFlowControlSignal decides the pause of the default HIGH priority lane. It counts
// only the default lane's tasks, so a lane that applies slowly pauses itself rather than the
// default lane. Past ReplicationReceiverMaxTotalOutstandingTaskCount across the default lane and
// every lane, it pauses HIGH anyway, and with it every lane.
func (r *laneStreamReceiver) highPriorityFlowControlSignal() *FlowControlSignal {
	count := r.highPriorityTaskTracker.Size()
	if total := r.highFamilyTrackingCount(); total > r.Config.ReplicationReceiverMaxTotalOutstandingTaskCount() {
		count = max(count, total, r.Config.ReplicationReceiverMaxOutstandingTaskCount()+1)
	}
	return &FlowControlSignal{
		taskTrackingCount:  count,
		lastSlowSubmission: r.getLastSlowSubmissionTimestamp(enumsspb.TASK_PRIORITY_HIGH),
	}
}

// laneFlowControlCommand pauses a lane whose outstanding tasks pass
// ReplicationReceiverLaneMaxOutstandingTaskCount, and resumes it otherwise.
func (r *laneStreamReceiver) laneFlowControlCommand(laneID string) enumsspb.ReplicationFlowControlCommand {
	r.memberLaneMu.Lock()
	lane, ok := r.memberLanes[laneID]
	outstanding := 0
	if ok {
		outstanding = lane.tracker.Size()
	}
	r.memberLaneMu.Unlock()
	if limit := r.Config.ReplicationReceiverLaneMaxOutstandingTaskCount(); outstanding > limit {
		r.logger.Warn("Pausing a replication lane.", tag.NewStringTag("lane", laneID), tag.Counter(outstanding), tag.NewInt("limit", limit))
		return enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE
	}
	return enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME
}

func (r *laneStreamReceiver) highFamilyTrackingCount() int {
	count := r.highPriorityTaskTracker.Size()
	r.memberLaneMu.Lock()
	for _, lane := range r.memberLanes {
		count += lane.tracker.Size()
	}
	r.memberLaneMu.Unlock()
	return count
}

// memberLaneWatermarks snapshots the low watermark of every lane that has received at
// least one batch, keyed by lane ID. Retired lanes are dropped once drained.
func (r *laneStreamReceiver) memberLaneWatermarks() map[string]WatermarkInfo {
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

func (r *laneStreamReceiver) getTaskSchedulerPriority(priority enumsspb.TaskPriority, task TrackableExecutableTask) (enumsspb.TaskPriority, error) {
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

func (r *laneStreamReceiver) getTaskScheduler(priority enumsspb.TaskPriority) (ctasks.Scheduler[TrackableExecutableTask], error) {
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
func (r *laneStreamReceiver) validateAndSetReceiverMode(priority enumsspb.TaskPriority) error {
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
	default:
	}
	return nil
}

func (r *laneStreamReceiver) setReceiverMode(priority enumsspb.TaskPriority) {
	if r.receiverMode != ReceiverModeUnset {
		return
	}
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeSingleStack))
	case enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeTieredStack))
	default:
	}
}

// laneProtocolVersion is the replication lane protocol version this receiver advertises.
// Lanes split HIGH priority traffic, so they require the tiered stack.
func (r *laneStreamReceiver) laneProtocolVersion(receiverMode ReceiverMode) int32 {
	if !r.lanesEnabled || receiverMode != ReceiverModeTieredStack {
		return 0
	}
	return replicationLaneProtocolVersion
}

// messageLaneID returns the lane ID of a batch, or an empty string for a batch on a
// default lane. It rejects lane metadata this receiver has not advertised support for.
func (r *laneStreamReceiver) messageLaneID(messages *replicationspb.WorkflowReplicationMessages) (string, error) {
	if messages.GetIsolatedNamespaceId() != "" || messages.GetRetireIsolatedLane() {
		return "", serviceerror.NewInvalidArgument("namespace isolation lanes are not supported by this receiver")
	}
	laneInfo := messages.GetLaneInfo()
	if laneInfo == nil {
		return "", nil
	}
	if !r.lanesEnabled {
		return "", serviceerror.NewInvalidArgument("replication lanes are not enabled on this receiver")
	}
	if laneInfo.GetLaneId() == "" {
		return "", serviceerror.NewInvalidArgument("replication lane ID is empty")
	}
	return laneInfo.GetLaneId(), nil
}
