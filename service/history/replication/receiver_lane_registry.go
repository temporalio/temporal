package replication

import (
	"maps"
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type receiverLaneRegistry struct {
	mu             sync.Mutex
	lanes          map[string]*receiverLane
	closed         bool
	logger         log.Logger
	metricsHandler metrics.Handler
}

// receiverLane intentionally contains no logical key or policy state. The receiver
// only preserves ordering and progress for the sender's stream-local lane ID.

type receiverLane struct {
	tracker                    ExecutableTaskTracker
	priority                   enumsspb.TaskPriority
	retiring                   bool
	batchRegistrationsInFlight int
}

func newReceiverLaneRegistry(logger log.Logger, metricsHandler metrics.Handler) *receiverLaneRegistry {
	return &receiverLaneRegistry{
		lanes:          make(map[string]*receiverLane),
		logger:         logger,
		metricsHandler: metricsHandler,
	}
}

func (r *receiverLaneRegistry) Resolve(
	laneID string,
	priority enumsspb.TaskPriority,
	retire bool,
) (ExecutableTaskTracker, error) {
	if laneID == "" {
		return nil, serviceerror.NewInternal("empty replication lane ID")
	}
	if priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
		return nil, serviceerror.NewInternal("replication lanes require tiered processing")
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
	} else if lane.retiring && !retire {
		return nil, serviceerror.NewInternalf("replication lane %q received traffic after retirement", laneID)
	}
	lane.batchRegistrationsInFlight++
	return lane.tracker, nil
}

func (r *receiverLaneRegistry) FinishBatchRegistration(laneID string, retire bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane, ok := r.lanes[laneID]
	if !ok || lane.batchRegistrationsInFlight == 0 {
		r.logger.DPanic("Replication lane batch finished without a matching resolution")
		return
	}
	lane.batchRegistrationsInFlight--
	if retire {
		lane.retiring = true
	}
}

func (r *receiverLaneRegistry) TrackingCount(priority enumsspb.TaskPriority) int {
	r.mu.Lock()
	lanes := make([]*receiverLane, 0, len(r.lanes))
	for _, lane := range r.lanes {
		if lane.priority == priority {
			lanes = append(lanes, lane)
		}
	}
	r.mu.Unlock()

	count := 0
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
		if lane.retiring && lane.batchRegistrationsInFlight == 0 && lane.tracker.Size() == 0 && watermark != nil {
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
	for _, lane := range r.lanes {
		lane.tracker.Cancel()
	}
}
