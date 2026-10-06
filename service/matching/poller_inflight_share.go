package matching

import "sync"

// shareVerdict says where one worker sits relative to its peers on this queue.
type shareVerdict int

const (
	// shareUnknown means fairness must not be applied: the worker didn't identify itself,
	// or there is no peer to compare it against. Always fail open -- a worker we can't
	// place must never be denied pollers because of it.
	shareUnknown shareVerdict = iota
	shareOver
	shareUnder
	shareFair
)

// inflightPollTracker counts how many polls each worker currently has parked on one
// physical queue.
//
// Under FIFO matching a task goes to whichever poller is waiting, so a worker's share of
// in-flight polls *is* its share of dispatched tasks. That makes this the quantity that
// directly drives the rich-get-richer effect: a worker that gets ahead holds more polls,
// wins more races, receives more scaling suggestions, and grows further.
//
// Counting here rather than on the engine's workerPollerTracker is deliberate. This is
// already scoped to the physical queue the decision is made on, so there is no host-wide
// filtering to do, and a forwarded poll is counted once by the child queue and once by the
// root -- which is correct, since each genuinely has a poll parked on it.
//
// Note what this cannot see: a poller blocked waiting for an execution slot never issues a
// poll, so a slot-bound worker looks smaller than its configured pool. For workers of equal
// capacity that bias is common-mode and cancels out; for unequal capacity it does not, and
// correcting it would require workers to report their slot count.
type inflightPollTracker struct {
	mu     sync.Mutex
	counts map[string]int // workerInstanceKey -> polls currently parked on this queue
}

func newInflightPollTracker() *inflightPollTracker {
	return &inflightPollTracker{counts: make(map[string]int)}
}

// add registers (n=+1) or releases (n=-1) one parked poll. Entries are deleted at zero so
// a departed worker stops counting immediately, without needing a TTL or an eviction path.
func (t *inflightPollTracker) add(workerInstanceKey string, n int) {
	if t == nil || workerInstanceKey == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.counts[workerInstanceKey] + n
	if c <= 0 {
		delete(t.counts, workerInstanceKey)
		return
	}
	t.counts[workerInstanceKey] = c
}

// classify compares one worker's parked-poll count against the mean across workers on this
// queue. band is the width of the dead zone on either side of the mean, as a multiplier,
// which keeps workers from flapping between verdicts.
//
// The caller is mid-dispatch, so its own count still includes the poll being matched while
// peers show only parked polls. That poll is about to end, so it is discounted -- without
// it an evenly balanced fleet reads as over-share (3 pollers each becomes 4 vs a mean of
// 3.33, which trips a 1.2 band exactly).
func (t *inflightPollTracker) classify(workerInstanceKey string, band float64) shareVerdict {
	if t == nil || workerInstanceKey == "" || band <= 1 {
		return shareUnknown
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	self, ok := t.counts[workerInstanceKey]
	// One worker is trivially at its own mean; it takes a peer before the comparison means
	// anything.
	if !ok || len(t.counts) < 2 {
		return shareUnknown
	}

	total := 0
	for _, c := range t.counts {
		total += c
	}
	own := float64(self - 1) // discount the poll being matched right now
	mean := float64(total-1) / float64(len(t.counts))
	if own <= 0 || mean <= 0 {
		return shareUnknown
	}

	switch {
	case own > mean*band:
		return shareOver
	case own*band < mean:
		return shareUnder
	default:
		return shareFair
	}
}
