package replication

import (
	"maps"
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type receiverLaneRegistry struct {
	mu                  sync.Mutex
	highPriorityTracker ExecutableTaskTracker
	lowPriorityTracker  ExecutableTaskTracker
	lanes               map[string]*receiverLane
	closed              bool
	logger              log.Logger
	metricsHandler      metrics.Handler
}

// receiverLane intentionally contains no logical key or policy state. The receiver
// only preserves ordering and progress for the sender's stream-local lane ID.

type receiverLane struct {
	tracker  ExecutableTaskTracker
	priority enumsspb.TaskPriority
	retiring bool
}

func newReceiverLaneRegistry(logger log.Logger, metricsHandler metrics.Handler) *receiverLaneRegistry {
	return &receiverLaneRegistry{
		highPriorityTracker: NewExecutableTaskTracker(logger, metricsHandler),
		lowPriorityTracker:  NewExecutableTaskTracker(logger, metricsHandler),
		lanes:               make(map[string]*receiverLane),
		logger:              logger,
		metricsHandler:      metricsHandler,
	}
}

func (r *receiverLaneRegistry) TrackBatch(
	priority enumsspb.TaskPriority,
	laneInfo *replicationspb.ReplicationLaneInfo,
	watermark WatermarkInfo,
	tasks ...TrackableExecutableTask,
) ([]TrackableExecutableTask, error) {
	if laneInfo == nil {
		tracker, err := r.defaultTracker(priority)
		if err != nil {
			return nil, err
		}
		return tracker.TrackTasks(watermark, tasks...), nil
	}
	laneID := laneInfo.GetLaneId()
	if laneID == "" {
		return nil, serviceerror.NewInternal("empty replication lane ID")
	}
	if priority != enumsspb.TASK_PRIORITY_HIGH && priority != enumsspb.TASK_PRIORITY_LOW {
		return nil, serviceerror.NewInternalf("invalid replication lane priority: %v", priority)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.lanes[laneID]
	if !ok {
		lane = &receiverLane{
			tracker:  NewExecutableTaskTracker(r.logger, r.metricsHandler),
			priority: priority,
		}
		if r.closed {
			lane.tracker.Cancel()
		}
		r.lanes[laneID] = lane
	} else if lane.priority != priority {
		return nil, serviceerror.NewInternalf(
			"replication lane %q changed priority from %v to %v",
			laneID,
			lane.priority,
			priority,
		)
	} else if lane.retiring && !laneInfo.GetRetireLane() {
		return nil, serviceerror.NewInternalf("replication lane %q received traffic after retirement", laneID)
	}
	trackedTasks := lane.tracker.TrackTasks(watermark, tasks...)
	if laneInfo.GetRetireLane() {
		lane.retiring = true
	}
	return trackedTasks, nil
}

func (r *receiverLaneRegistry) defaultTracker(priority enumsspb.TaskPriority) (ExecutableTaskTracker, error) {
	switch priority {
	case enumsspb.TASK_PRIORITY_UNSPECIFIED, enumsspb.TASK_PRIORITY_HIGH:
		return r.highPriorityTracker, nil
	case enumsspb.TASK_PRIORITY_LOW:
		return r.lowPriorityTracker, nil
	default:
		return nil, serviceerror.NewInvalidArgumentf("Unknown task priority: %v", priority)
	}
}

func (r *receiverLaneRegistry) DefaultWatermarks() (highPriority, lowPriority *WatermarkInfo) {
	return r.highPriorityTracker.LowWatermark(), r.lowPriorityTracker.LowWatermark()
}

func (r *receiverLaneRegistry) TrackingCount(priority enumsspb.TaskPriority) int {
	tracker, err := r.defaultTracker(priority)
	if err != nil || priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
		r.logger.DPanic("Replication lane tracking count requested for invalid priority")
		return 0
	}
	r.mu.Lock()
	lanes := make([]*receiverLane, 0, len(r.lanes))
	for _, lane := range r.lanes {
		if lane.priority == priority {
			lanes = append(lanes, lane)
		}
	}
	r.mu.Unlock()

	count := tracker.Size()
	for _, lane := range lanes {
		count += lane.tracker.Size()
	}
	return count
}

func (r *receiverLaneRegistry) Watermarks() map[string]WatermarkInfo {
	r.mu.Lock()
	lanes := make(map[string]*receiverLane, len(r.lanes))
	maps.Copy(lanes, r.lanes)
	r.mu.Unlock()

	out := make(map[string]WatermarkInfo, len(lanes))
	for laneID, lane := range lanes {
		watermark := lane.tracker.LowWatermark()

		r.mu.Lock()
		current, ok := r.lanes[laneID]
		if !ok || current != lane {
			r.mu.Unlock()
			continue
		}
		if lane.retiring && lane.tracker.Size() == 0 && watermark != nil {
			delete(r.lanes, laneID)
			r.mu.Unlock()
			continue
		}
		if watermark != nil {
			out[laneID] = *watermark
		}
		r.mu.Unlock()
	}
	return out
}

func (r *receiverLaneRegistry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.highPriorityTracker.Cancel()
	r.lowPriorityTracker.Cancel()
	for _, lane := range r.lanes {
		lane.tracker.Cancel()
	}
}
