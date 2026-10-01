package matching

import (
	"sync"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
)

// shareVerdict says where one worker's poller density sits relative to its fleet's.
type shareVerdict int

const (
	// shareUnknown means the worker can't be placed -- it didn't report, or has no peers to
	// compare against -- and must therefore never be denied pollers.
	shareUnknown shareVerdict = iota
	shareOver
	shareUnder
	shareFair
)

// pollerShareTracker holds the most recent poller-pool report from each worker polling one
// physical queue. The quantity it equalizes is poller density -- pollers per execution slot
// -- not poller count, so a 32-slot worker is expected to hold more pollers than a 2-slot
// one rather than the same number.
type pollerShareTracker struct {
	ttl        time.Duration
	timeSource clock.TimeSource

	mu      sync.Mutex
	reports map[string]pollerShareReport
}

type pollerShareReport struct {
	target   int64
	capacity int64
	reported time.Time
}

func newPollerShareTracker(ttl time.Duration, timeSource clock.TimeSource) *pollerShareTracker {
	return &pollerShareTracker{
		ttl:        ttl,
		timeSource: timeSource,
		reports:    make(map[string]pollerShareReport),
	}
}

// record stores what a worker reported about its own poller pool on this poll. A worker
// reporting nothing, or an unknown capacity, stays untracked: invisible both as a subject of
// fairness and as a contributor to the fleet mean.
func (t *pollerShareTracker) record(workerInstanceKey string, info *taskqueuepb.PollerScalingInfo) {
	if t == nil || workerInstanceKey == "" || info.GetPollerTarget() <= 0 || info.GetMaxSlots() <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reports[workerInstanceKey] = pollerShareReport{
		target:   int64(info.GetPollerTarget()),
		capacity: int64(info.GetMaxSlots()),
		reported: t.timeSource.Now(),
	}
}

// forget drops a worker that has shut down. Without this a departed worker keeps inflating
// the fleet mean for a full TTL, which reads as every survivor being under-share and
// suppresses the scale-downs that should follow the departure.
func (t *pollerShareTracker) forget(workerInstanceKey string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.reports, workerInstanceKey)
}

// classify compares one worker's poller density against the fleet's. band is a multiplier
// giving the width of the dead zone on either side of the mean, which keeps workers from
// flapping between verdicts and bounds the spread the fleet settles at to roughly band^2.
func (t *pollerShareTracker) classify(workerInstanceKey string, band float64) shareVerdict {
	if t == nil || workerInstanceKey == "" || band <= 1 {
		return shareUnknown
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.timeSource.Now()
	var self pollerShareReport
	var totalTarget, totalCapacity int64
	live := 0
	for key, report := range t.reports {
		// Covers workers that stopped polling without shutting down cleanly -- crashes and
		// killed pods, where forget never runs. Swept here rather than on a timer because
		// this is the only reader that cares.
		if now.Sub(report.reported) > t.ttl {
			delete(t.reports, key)
			continue
		}
		totalTarget += report.target
		totalCapacity += report.capacity
		live++
		if key == workerInstanceKey {
			self = report
		}
	}

	// A lone reporter is trivially at its own mean; it takes a peer before the comparison
	// means anything.
	if live < 2 || self.capacity == 0 {
		return shareUnknown
	}

	own := float64(self.target) / float64(self.capacity)
	fleet := float64(totalTarget) / float64(totalCapacity)
	switch {
	case own > fleet*band:
		return shareOver
	case own*band < fleet:
		return shareUnder
	default:
		return shareFair
	}
}
