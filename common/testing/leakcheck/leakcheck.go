package leakcheck

import (
	"fmt"
	"reflect"
	"runtime"
	runtimedebug "runtime/debug"
	"sync/atomic"
	"time"
	"unsafe"
)

// ObjectGraphLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectGraphLeakCheck struct {
	objects  []trackedObject
	excludes []exclusion
}

type trackedObject struct {
	path      string
	typeName  string
	collected *atomic.Bool
	cleanup   runtime.Cleanup
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
	walker.walk(reflect.ValueOf(root), rootPath)
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
func (t *ObjectGraphLeakCheck) Check() (string, error) {
	settleLeakCheck()
	report := newObjectGraphReport(t.objects, t.excludes)
	return report.String(), report.failures()
}

// settleLeakCheck gives pending cleanups, runtime timers, and GC enough time to
// release short-lived references before leak probes sample object reachability.
func settleLeakCheck() {
	for range 200 {
		runtime.GC()
		runtimedebug.FreeOSMemory()
		time.Sleep(20 * time.Millisecond)
	}
}

type graphWalker struct {
	objects []trackedObject
	seen    map[uintptr]struct{}
}

func newGraphWalker() graphWalker {
	return graphWalker{
		seen: make(map[uintptr]struct{}),
	}
}

func (w *graphWalker) walk(v reflect.Value, path string) {
	if !v.IsValid() {
		return
	}
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		addr := v.Pointer()
		if _, ok := w.seen[addr]; ok {
			return
		}
		w.seen[addr] = struct{}{}
		if obj, ok := trackPointerObject(addr, path, v.Type().String()); ok {
			w.objects = append(w.objects, obj)
		}
		w.walk(v.Elem(), path)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			w.walk(v.Field(i), path+"."+field.Name)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i), fmt.Sprintf("%s[%d]", path, i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for i := 0; iter.Next(); i++ {
			w.walk(iter.Key(), fmt.Sprintf("%s[key%d]", path, i))
			w.walk(iter.Value(), fmt.Sprintf("%s[%d]", path, i))
		}
	}
}

func trackPointerObject(addr uintptr, path string, typeName string) (trackedObject, bool) {
	collected := &atomic.Bool{}
	var cleanup runtime.Cleanup
	ok := true
	func() {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		cleanup = runtime.AddCleanup((*byte)(unsafe.Pointer(addr)), func(collected *atomic.Bool) {
			collected.Store(true)
		}, collected)
	}()
	if !ok || cleanup == (runtime.Cleanup{}) {
		return trackedObject{}, false
	}
	return trackedObject{
		path:      path,
		typeName:  typeName,
		collected: collected,
		cleanup:   cleanup,
	}, true
}
