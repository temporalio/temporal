package matching

import (
	"sync"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/clock"
)

// pollerShareTracker holds the poller pool state each worker reports on its polls, so a
// physical queue can tell whether the worker it is about to answer already holds more
// than its share of the fleet's pollers.
//
// Why this exists: poller-scaling suggestions ride on dispatched tasks, and task dispatch
// is proportional to poller count (the matcher hands tasks to waiting pollers in FIFO
// order). So a worker's rate of receiving suggestions is proportional to how many pollers
// it already has, which makes growth multiplicative and preserves any imbalance between
// otherwise identical workers -- there is no term anywhere that compares one worker to
// another. This is the missing term.
//
// The counts are self-reported rather than measured server-side on purpose. Matching can
// count in-flight polls per worker, but that undercounts a worker whose pollers are busy
// dispatching rather than parked in a poll -- which inverts the signal, since the busiest
// worker then looks the most starved. Measured on a 3-worker fleet, reported targets were
// within 20% of each other while in-flight poll counts differed by 5.7x.
type pollerShareTracker struct {
	timeSource clock.TimeSource
	ttl        time.Duration

	mu      sync.Mutex
	workers map[string]reportedPollerState
}

type reportedPollerState struct {
	pollerTarget int32
	maxSlots     int32
	lastReported time.Time
}

func newPollerShareTracker(timeSource clock.TimeSource, ttl time.Duration) *pollerShareTracker {
	return &pollerShareTracker{
		timeSource: timeSource,
		ttl:        ttl,
		workers:    make(map[string]reportedPollerState),
	}
}

// record stores what a worker reported on this poll. A zero or negative target is treated
// as "not reported": older SDKs do not send the field at all, and a worker that is not
// autoscaling has no meaningful target to compare.
func (t *pollerShareTracker) record(key string, info *taskqueuepb.PollerScalingInfo) {
	if key == "" || info.GetPollerTarget() <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workers[key] = reportedPollerState{
		pollerTarget: info.GetPollerTarget(),
		maxSlots:     info.GetMaxSlots(),
		lastReported: t.timeSource.Now(),
	}
}

func (t *pollerShareTracker) forget(key string) {
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.workers, key)
}

// overShare reports whether the given worker holds more than `factor` times its fair
// share of the fleet's pollers, where fair is weighted by each worker's slot capacity so
// a bigger worker is allowed proportionally more pollers.
//
// Comparison is on poller *density* -- pollers per slot -- against the fleet-wide density
// rather than against a mean of per-worker ratios, which one small-capacity worker can
// drag badly. At equal density n_i/cap_i is the same for every worker, i.e. n_i is
// proportional to cap_i, which is the intended allocation. When every worker reports the
// same capacity this reduces exactly to n_i > factor * mean(n), so the unweighted case
// falls out rather than needing its own code path.
//
// Returns false whenever the comparison would be meaningless: the worker never reported,
// fewer than two workers are known, or capacities are missing. Failing open matters --
// suppressing a worker on bad data starves the one that may most need pollers.
func (t *pollerShareTracker) overShare(key string, factor float64) (over bool, mine, fair float64) {
	if key == "" || factor <= 0 {
		return false, 0, 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := t.timeSource.Now().Add(-t.ttl)
	me, ok := t.workers[key]
	if !ok || me.lastReported.Before(cutoff) {
		return false, 0, 0
	}

	// Only workers that reported recently take part. Workers on older SDKs are invisible
	// here, so fairness is enforced among reporters and the rest are simply unmanaged --
	// which is why the caller should gate this on reporting coverage. Counting them as
	// zero instead would understate the fleet and over-suppress everyone who does report.
	var totalPollers, totalSlots float64
	live := 0
	weighted := true
	for k, w := range t.workers {
		if w.lastReported.Before(cutoff) {
			delete(t.workers, k)
			continue
		}
		live++
		totalPollers += float64(w.pollerTarget)
		if w.maxSlots <= 0 {
			// MaxSlots is documented as possibly 0 when a slot supplier has no
			// well-defined limit. One such worker makes capacity weighting undefined for
			// the whole fleet, so fall back to unweighted for everyone.
			weighted = false
		}
		totalSlots += float64(w.maxSlots)
	}
	if live < 2 {
		return false, 0, 0
	}

	if weighted && totalSlots > 0 && me.maxSlots > 0 {
		mine = float64(me.pollerTarget) / float64(me.maxSlots)
		fair = totalPollers / totalSlots
	} else {
		mine = float64(me.pollerTarget)
		fair = totalPollers / float64(live)
	}
	if fair <= 0 {
		return false, mine, fair
	}
	return mine > factor*fair, mine, fair
}

// reportingWorkers is the number of workers that reported within the TTL. Used to decide
// whether coverage is high enough for share-based fairness to be safe to apply.
func (t *pollerShareTracker) reportingWorkers() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := t.timeSource.Now().Add(-t.ttl)
	n := 0
	for _, w := range t.workers {
		if !w.lastReported.Before(cutoff) {
			n++
		}
	}
	return n
}
