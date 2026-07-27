package objectleak

import (
	"errors"
	"iter"
	"runtime"
	runtimedebug "runtime/debug"
	"slices"
	"sync/atomic"
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

// Baseline contains the retained objects captured by IgnoreCurrent.
type Baseline struct {
	objects       []trackedObject
	collectedByID map[objectIdentity]*atomic.Bool
}

type objectIdentity struct {
	addr     uintptr
	typeName string
}

func (b Baseline) contains(obj trackedObject) bool {
	collected, ok := b.collectedByID[obj.identity()]
	return ok && !collected.Load()
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

// WithGCSettleTimeout sets the maximum time Check and IgnoreCurrent spend
// forcing GC and waiting for retained-object counts to settle.
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

// IgnoreCurrent waits for currently tracked objects to settle, returns the
// retained objects as a baseline, and resets tracked roots.
func (t *ObjectLeakCheck) IgnoreCurrent() Baseline {
	settleGC(t.gcSettleTimeout, func() int {
		return len(slices.Collect(retainedObjects(t.objects)))
	})

	baseline := Baseline{
		collectedByID: make(map[objectIdentity]*atomic.Bool),
	}
	for obj := range retainedObjects(t.objects) {
		identity := obj.identity()
		if canonicalCollected, ok := baseline.collectedByID[identity]; ok {
			obj.cleanup.Stop()
			obj.collected = canonicalCollected
		} else {
			baseline.collectedByID[identity] = obj.collected
		}
		baseline.objects = append(baseline.objects, obj)
	}
	t.objects = nil
	t.roots = 0
	return baseline
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
func (t *ObjectLeakCheck) Check(baseline Baseline) (string, error) {
	settleGC(t.gcSettleTimeout, func() int {
		return len(slices.Collect(retainedObjects(t.objects))) +
			len(slices.Collect(retainedObjects(baseline.objects)))
	})
	report := newReport(t.objects, baseline, t.roots, t.expected, t.pruneTypes)
	return report.string(), report.failures()
}

func retainedObjects(objects []trackedObject) iter.Seq[trackedObject] {
	return func(yield func(trackedObject) bool) {
		for _, obj := range objects {
			if !obj.collected.Load() && !yield(obj) {
				return
			}
		}
	}
}

func settleGC(timeout time.Duration, snapshot func() int) {
	start := time.Now()
	minWaitDeadline := start.Add(checkGCMinWait)
	deadline := start.Add(timeout)
	settledDeadline := minWaitDeadline

	var lastSnapshot int
	var haveLastSnapshot bool
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

		now := time.Now()

		// Wait for the snapshot to stop changing instead of returning on the
		// first passing state. This lets delayed cleanup callbacks finish before
		// we decide.
		if currentSnapshot := snapshot(); !haveLastSnapshot || currentSnapshot != lastSnapshot {
			lastSnapshot = currentSnapshot
			haveLastSnapshot = true
			settledDeadline = now.Add(checkGCQuiet)
			if minWaitDeadline.After(settledDeadline) {
				settledDeadline = minWaitDeadline
			}
		}

		// The minimum wait gives cleanup callbacks several GC cycles to run. The
		// quiet window then handles normal cleanup latency; the timeout bounds a
		// genuinely stuck object graph so the test can still report diagnostics.
		if now.After(settledDeadline) || now.After(deadline) {
			return
		}
		time.Sleep(checkGCPause)
	}
}
