package matching

import (
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
)

const pollerShareInitMaxSize = 1000

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
// physical queue, and compares a worker's poller count against the fleet's mean. It exists
// because scaling suggestions ride on dispatched tasks, so growth is proportional to current
// size and imbalance between identical workers persists rather than correcting.
//
// Entries expire on the same TTL as pollerHistory, which covers workers that stopped polling
// without shutting down cleanly.
type pollerShareTracker struct {
	reports cache.Cache // workerInstanceKey -> target (int64)
}

func newPollerShareTracker(ttl time.Duration, timeSource clock.TimeSource) *pollerShareTracker {
	return &pollerShareTracker{
		reports: cache.New(pollerShareInitMaxSize, &cache.Options{TTL: ttl, TimeSource: timeSource}),
	}
}

// record stores what a worker reported about its own poller pool on this poll. A worker
// reporting nothing stays untracked: invisible both as a subject of fairness and as a
// contributor to the fleet mean.
//
// Keyed on worker instance rather than poller identity because a fleet commonly shares one
// configured identity, and fairness has to tell its members apart.
func (t *pollerShareTracker) record(workerInstanceKey string, info *taskqueuepb.PollerPoolInfo) {
	if t == nil || workerInstanceKey == "" || info.GetTargetPollers() <= 0 {
		return
	}
	t.reports.Put(workerInstanceKey, int64(info.GetTargetPollers()))
}

// forget drops a worker that has shut down. Without this a departed worker keeps inflating
// the fleet mean for a full TTL, which reads as every survivor being under-share and
// suppresses the scale-downs that should follow the departure.
func (t *pollerShareTracker) forget(workerInstanceKey string) {
	if t == nil {
		return
	}
	t.reports.Delete(workerInstanceKey)
}

// classify compares one worker's poller count against the fleet mean. band is a multiplier
// giving the width of the dead zone on either side of the mean, which keeps workers from
// flapping between verdicts and bounds the spread the fleet settles at to roughly band^2.
func (t *pollerShareTracker) classify(workerInstanceKey string, band float64) shareVerdict {
	if t == nil || workerInstanceKey == "" || band <= 1 {
		return shareUnknown
	}

	var own, total int64
	live := 0
	ite := t.reports.Iterator()
	defer ite.Close()
	for ite.HasNext() {
		entry := ite.Next()
		target := entry.Value().(int64)
		total += target
		live++
		if entry.Key().(string) == workerInstanceKey {
			own = target
		}
	}

	// A lone reporter is trivially at its own mean; it takes a peer before the comparison
	// means anything.
	if live < 2 || own == 0 {
		return shareUnknown
	}

	fleet := float64(total) / float64(live)
	switch {
	case float64(own) > fleet*band:
		return shareOver
	case float64(own)*band < fleet:
		return shareUnder
	default:
		return shareFair
	}
}
