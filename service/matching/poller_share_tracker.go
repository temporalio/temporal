package matching

import (
	"sync"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
)

// shareVerdict says where one worker's poller count sits relative to its fleet's mean.
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
// physical queue, and compares a worker's poller count against the fleet's mean.
//
// It exists because scaling suggestions ride on dispatched tasks: under FIFO matching a
// worker's share of dispatched tasks equals its share of pollers, so growth is proportional
// to current size in both directions. The equilibrium is degenerate -- any imbalance between
// identical workers is preserved rather than corrected.
type pollerShareTracker struct {
	ttl        time.Duration
	timeSource clock.TimeSource

	mu      sync.Mutex
	reports map[string]pollerShareReport
}

type pollerShareReport struct {
	target   int64
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
// reporting nothing stays untracked: invisible both as a subject of fairness and as a
// contributor to the fleet mean.
//
// Keyed on worker instance rather than poller identity because a fleet commonly shares one
// configured identity, and fairness has to tell its members apart.
func (t *pollerShareTracker) record(workerInstanceKey string, info *taskqueuepb.PollerScalingInfo) {
	if t == nil || workerInstanceKey == "" || info.GetPollerTarget() <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reports[workerInstanceKey] = pollerShareReport{
		target:   int64(info.GetPollerTarget()),
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

// classify compares one worker's poller count against the fleet mean. band is a multiplier
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
	var totalTarget int64
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
		live++
		if key == workerInstanceKey {
			self = report
		}
	}

	// A lone reporter is trivially at its own mean; it takes a peer before the comparison
	// means anything.
	if live < 2 || self.target == 0 {
		return shareUnknown
	}

	own := float64(self.target)
	fleet := float64(totalTarget) / float64(live)
	switch {
	case own > fleet*band:
		return shareOver
	case own*band < fleet:
		return shareUnder
	default:
		return shareFair
	}
}
