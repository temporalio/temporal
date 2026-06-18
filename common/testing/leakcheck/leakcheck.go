package leakcheck

import (
	"fmt"
	"reflect"
	"runtime"
	runtimedebug "runtime/debug"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

// ObjectGraphLeakCheck tracks objects reachable from roots and reports objects
// that remain reachable after GC.
type ObjectGraphLeakCheck struct {
	objects    []trackedObject
	excludes   []string
	rootCounts map[string]int
}

type trackedObject struct {
	matchPath   string
	displayPath string
	typeName    string
	collected   *atomic.Bool
	cleanup     runtime.Cleanup
}

type Option func(*ObjectGraphLeakCheck)

// WithExclude skips retained-object failures whose reflected path or type name
// matches pattern. A trailing '*' matches any suffix.
func WithExclude(pattern string) Option {
	return func(t *ObjectGraphLeakCheck) {
		t.excludes = append(t.excludes, pattern)
	}
}

// NewObjectGraphLeakCheck creates an object graph leak checker. Add roots with
// Track after the code under test has finished creating the objects that should
// be released.
func NewObjectGraphLeakCheck(opts ...Option) ObjectGraphLeakCheck {
	t := ObjectGraphLeakCheck{
		rootCounts: make(map[string]int),
	}
	for _, opt := range opts {
		opt(&t)
	}
	return t
}

// Track snapshots all pointer objects reachable from root. rootPath is the
// stable path used for exclusion matching; reports add a per-root index so
// repeated roots are distinguishable.
func (t *ObjectGraphLeakCheck) Track(rootPath string, root any) {
	if t.rootCounts == nil {
		t.rootCounts = make(map[string]int)
	}
	rootIndex := t.rootCounts[rootPath]
	t.rootCounts[rootPath]++
	displayRootPath := fmt.Sprintf("%s[%d]", rootPath, rootIndex)

	walker := newGraphWalker()
	walker.walk(reflect.ValueOf(root), rootPath, displayRootPath)
	t.objects = append(t.objects, walker.objects...)
}

// Check settles GC, then returns a full retained-object report and an error for
// retained objects not covered by exclusions or exclusions that no longer match
// any tracked object.
func (t *ObjectGraphLeakCheck) Check() (string, error) {
	settleLeakCheck()
	var report objectGraphReport
	matchedExcludes := make(map[string]bool, len(t.excludes))
	for _, obj := range t.objects {
		excludedBy := t.matchingExcludes(obj)
		for _, pattern := range excludedBy {
			matchedExcludes[pattern] = true
		}
		if obj.collected.Load() {
			continue
		}
		report.retainedObjects = append(report.retainedObjects, retainedObject{
			matchPath:   obj.matchPath,
			displayPath: obj.displayPath,
			typeName:    obj.typeName,
			excludedBy:  excludedBy,
		})
	}
	for _, pattern := range t.excludes {
		if !matchedExcludes[pattern] {
			report.unmatchedExcludes = append(report.unmatchedExcludes, pattern)
		}
	}
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

func (t *ObjectGraphLeakCheck) matchingExcludes(obj trackedObject) []string {
	var matches []string
	matchesPattern := func(pattern string, value string) bool {
		if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
			return strings.HasPrefix(value, prefix)
		}
		return value == pattern
	}
	for _, pattern := range t.excludes {
		if matchesPattern(pattern, obj.matchPath) || matchesPattern(pattern, obj.typeName) {
			matches = append(matches, pattern)
		}
	}
	return matches
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

func (w *graphWalker) walk(v reflect.Value, matchPath string, displayPath string) {
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
		if obj, ok := trackPointerObject(addr, matchPath, displayPath, v.Type().String()); ok {
			w.objects = append(w.objects, obj)
		}
		w.walk(v.Elem(), matchPath, displayPath)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			w.walk(v.Field(i), matchPath+"."+field.Name, displayPath+"."+field.Name)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i), fmt.Sprintf("%s[%d]", matchPath, i), fmt.Sprintf("%s[%d]", displayPath, i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for i := 0; iter.Next(); i++ {
			w.walk(iter.Key(), fmt.Sprintf("%s[key%d]", matchPath, i), fmt.Sprintf("%s[key%d]", displayPath, i))
			w.walk(iter.Value(), fmt.Sprintf("%s[%d]", matchPath, i), fmt.Sprintf("%s[%d]", displayPath, i))
		}
	}
}

func trackPointerObject(addr uintptr, matchPath string, displayPath string, typeName string) (trackedObject, bool) {
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
		matchPath:   matchPath,
		displayPath: displayPath,
		typeName:    typeName,
		collected:   collected,
		cleanup:     cleanup,
	}, true
}
