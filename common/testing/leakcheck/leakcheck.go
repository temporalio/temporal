package leakcheck

import (
	"runtime"
	runtimedebug "runtime/debug"
	"time"
)

// ObjectLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectLeakCheck struct {
	objects  []trackedObject
	excludes exclusions
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

// NewObjectLeakCheck creates an object leak checker. Add roots with
// Track after the code under test has finished creating the objects that should
// be released.
func NewObjectLeakCheck(opts ...Option) (ObjectLeakCheck, error) {
	t := ObjectLeakCheck{}
	for _, opt := range opts {
		if err := opt(&t); err != nil {
			return ObjectLeakCheck{}, err
		}
	}
	return t, nil
}

// Track snapshots all pointer objects reachable from root. rootPath is the
// stable path used for exclusion matching and report grouping.
func (t *ObjectLeakCheck) Track(rootPath string, root any) {
	walker := newObjectWalker()
	walker.track(rootPath, root)
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
func (t *ObjectLeakCheck) Check() (string, error) {
	for range 200 {
		runtime.GC()
		runtimedebug.FreeOSMemory()
		time.Sleep(20 * time.Millisecond)
	}
	report := newObjectLeakReport(t.objects, t.excludes)
	return report.String(), report.failures()
}
