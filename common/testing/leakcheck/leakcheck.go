package leakcheck

import (
	"runtime"
	runtimedebug "runtime/debug"
	"time"
)

// ObjectGraphLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectGraphLeakCheck struct {
	objects  []trackedObject
	excludes []exclusion
}

type Option func(*ObjectGraphLeakCheck)

// WithExclude skips retained-object failures whose reflected path or type name
// matches pattern. A trailing '*' matches any suffix.
func WithExclude(pattern string) Option {
	return func(t *ObjectGraphLeakCheck) {
		t.excludes = append(t.excludes, newExclusion(pattern))
	}
}

// NewObjectGraphLeakCheck creates an object graph leak checker. Add roots with
// Track after the code under test has finished creating the objects that should
// be released.
func NewObjectGraphLeakCheck(opts ...Option) ObjectGraphLeakCheck {
	t := ObjectGraphLeakCheck{}
	for _, opt := range opts {
		opt(&t)
	}
	return t
}

// Track snapshots all pointer objects reachable from root. rootPath is the
// stable path used for exclusion matching and report grouping.
func (t *ObjectGraphLeakCheck) Track(rootPath string, root any) {
	walker := newGraphWalker()
	walker.track(rootPath, root)
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
func (t *ObjectGraphLeakCheck) Check() (string, error) {
	for range 200 {
		runtime.GC()
		runtimedebug.FreeOSMemory()
		time.Sleep(20 * time.Millisecond)
	}
	report := newObjectGraphReport(t.objects, t.excludes)
	return report.String(), report.failures()
}
