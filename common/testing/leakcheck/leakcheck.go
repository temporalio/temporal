package leakcheck

import (
	"runtime"
	runtimedebug "runtime/debug"
	"time"
)

const (
	checkGCDeadline = 10 * time.Second
	checkGCPause    = 20 * time.Millisecond
)

// ObjectLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectLeakCheck struct {
	objects  []trackedObject
	roots    int
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
	t.roots++
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
func (t *ObjectLeakCheck) Check() (string, error) {
	deadline := time.Now().Add(checkGCDeadline)
	var report report
	var err error
	for {
		runtime.GC()
		runtimedebug.FreeOSMemory()
		report = newReport(t.objects, t.roots, t.excludes)
		err = report.failures()
		if err == nil || time.Now().After(deadline) {
			return report.string(), err
		}
		time.Sleep(checkGCPause)
	}
}
