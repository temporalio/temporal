package leakcheck

import (
	"fmt"
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
	excludes        exclusions
	gcSettleTimeout time.Duration
}

type Option func(*ObjectLeakCheck) error

// WithExclude skips retained-object failures whose reflected path or type name
// matches pattern. A trailing '*' matches any suffix.
func WithExclude(pattern string) Option {
	return func(t *ObjectLeakCheck) error {
		exclusion, err := newExclusion(pattern)
		if err != nil {
			return err
		}
		t.excludes = append(t.excludes, exclusion)
		return nil
	}
}

// WithGCSettleTimeout sets the maximum time Check spends forcing GC and waiting
// for retained-object counts to settle.
func WithGCSettleTimeout(timeout time.Duration) Option {
	return func(t *ObjectLeakCheck) error {
		if timeout <= 0 {
			return fmt.Errorf("GC settle timeout must be positive")
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
	walker := newObjectWalker()
	walker.track(root)
	t.roots++
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
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
			runtime.GC()
			runtime.Gosched()
		}
		runtimedebug.FreeOSMemory()
		runtime.Gosched()

		report = newReport(t.objects, t.roots, t.excludes)
		err = report.failures()
		now := time.Now()

		// Wait for the report totals to stop changing instead of returning on
		// the first passing report. This lets delayed cleanup callbacks remove
		// both unexpected objects and now-stale exclusions before we decide.
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
