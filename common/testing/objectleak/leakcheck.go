package objectleak

import (
	"errors"
	"runtime"
	runtimedebug "runtime/debug"
	"time"
)

const (
	defaultGCSettleTimeout = 10 * time.Second
	checkGCMinWait         = 2 * time.Second
	checkGCBurst           = 3
	checkGCPause           = 20 * time.Millisecond
	checkGCQuiet           = 500 * time.Millisecond
)

// ObjectLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectLeakCheck struct {
	objects         []trackedObject
	roots           int
	expected        patterns
	pruneTypes      patterns
	gcSettleTimeout time.Duration
}

type Option func(*ObjectLeakCheck) error

// WithExpected marks retained objects whose reflected path or type name matches
// pattern as expected. A trailing '*' matches any suffix.
func WithExpected(pattern string) Option {
	return func(t *ObjectLeakCheck) error {
		t.expected = append(t.expected, newPattern(pattern))
		return nil
	}
}

// WithPruneType prevents Track from descending into values whose reflected type
// matches pattern. Named types match their package-qualified name. A trailing
// '*' matches any suffix.
func WithPruneType(pattern string) Option {
	return func(t *ObjectLeakCheck) error {
		t.pruneTypes = append(t.pruneTypes, newPattern(pattern))
		return nil
	}
}

// WithGCSettleTimeout sets the maximum time Check spends forcing GC and waiting
// for retained-object counts to settle.
func WithGCSettleTimeout(timeout time.Duration) Option {
	return func(t *ObjectLeakCheck) error {
		if timeout <= 0 {
			return errors.New("GC settle timeout must be positive")
		}
		t.gcSettleTimeout = timeout
		return nil
	}
}

// NewObjectLeakCheck creates an object leak checker.
func NewObjectLeakCheck(opts ...Option) (ObjectLeakCheck, error) {
	t := ObjectLeakCheck{
		gcSettleTimeout: defaultGCSettleTimeout,
	}
	for _, opt := range opts {
		if err := opt(&t); err != nil {
			return ObjectLeakCheck{}, err
		}
	}
	return t, nil
}

// Track walks all values reachable from root and tracks pointer objects it finds.
func (t *ObjectLeakCheck) Track(root any) {
	walker := newObjectWalker(t.pruneTypes)
	walker.track(root)
	t.roots++
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by expected patterns, expected patterns that no
// longer match any tracked object, or prune rules that did not match during
// tracking.
func (t *ObjectLeakCheck) Check() (string, error) {
	start := time.Now()
	minWaitDeadline := start.Add(checkGCMinWait)
	deadline := start.Add(t.gcSettleTimeout)
	settledDeadline := minWaitDeadline

	var lastTotals [3]int
	var haveLastTotals bool
	var report report
	var err error
	for {
		// AddCleanup callbacks run after GC proves tracked objects are
		// unreachable. Run a small burst and yield so callbacks can mark tracked
		// objects before the next report snapshot.
		for range checkGCBurst {
			//nolint:revive // This checker intentionally forces GC to drive AddCleanup callbacks.
			runtime.GC()
			runtime.Gosched()
		}
		runtimedebug.FreeOSMemory()
		runtime.Gosched()

		report = newReport(t.objects, t.roots, t.expected, t.pruneTypes)
		err = report.failures()
		now := time.Now()

		// Wait for the report totals to stop changing instead of returning on
		// the first passing report. This lets delayed cleanup callbacks remove
		// both unexpected objects and now-stale expected patterns before we decide.
		if totals := report.totals(); !haveLastTotals || totals != lastTotals {
			lastTotals = totals
			haveLastTotals = true
			settledDeadline = now.Add(checkGCQuiet)
			if minWaitDeadline.After(settledDeadline) {
				settledDeadline = minWaitDeadline
			}
		}

		// The minimum wait gives cleanup callbacks several GC cycles to run. The
		// quiet window then handles normal cleanup latency; the timeout bounds a
		// genuinely stuck object graph so the test can still report diagnostics.
		if now.After(settledDeadline) || now.After(deadline) {
			return report.string(), err
		}
		time.Sleep(checkGCPause)
	}
}
