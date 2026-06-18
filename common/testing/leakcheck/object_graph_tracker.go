package leakcheck

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"
)

// ObjectGraphLeakCheck tracks objects reachable from a root and reports objects
// that remain reachable after GC.
type ObjectGraphLeakCheck struct {
	objects  []trackedObject
	excludes []string
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
		t.excludes = append(t.excludes, pattern)
	}
}

func NewObjectGraphLeakCheck(rootPath string, root any, opts ...Option) ObjectGraphLeakCheck {
	t := ObjectGraphLeakCheck{}
	for _, opt := range opts {
		opt(&t)
	}
	walker := graphWalker{
		seen: make(map[uintptr]struct{}),
	}
	walker.walk(reflect.ValueOf(root), rootPath)
	t.objects = walker.objects
	return t
}

func (t ObjectGraphLeakCheck) Failures() error {
	return t.report().failures()
}

func (t ObjectGraphLeakCheck) Report() string {
	return t.report().String()
}

func (t ObjectGraphLeakCheck) report() objectGraphReport {
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
			path:       obj.path,
			typeName:   obj.typeName,
			excludedBy: excludedBy,
		})
	}
	for _, pattern := range t.excludes {
		if !matchedExcludes[pattern] {
			report.unmatchedExcludes = append(report.unmatchedExcludes, pattern)
		}
	}
	return report
}

type objectGraphReport struct {
	retainedObjects   []retainedObject
	unmatchedExcludes []string
}

type retainedObject struct {
	path       string
	typeName   string
	excludedBy []string
}

func (r objectGraphReport) failures() error {
	var failures []error
	for _, obj := range r.retainedObjects {
		if len(obj.excludedBy) > 0 {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s)", obj.path, obj.typeName))
	}
	for _, pattern := range r.unmatchedExcludes {
		failures = append(failures, fmt.Errorf("object graph exclusion %q did not match any object", pattern))
	}
	return errors.Join(failures...)
}

func (r objectGraphReport) String() string {
	var lines []string
	for _, obj := range r.retainedObjects {
		line := fmt.Sprintf("retained graph object %s (%s)", obj.path, obj.typeName)
		if len(obj.excludedBy) > 0 {
			line += fmt.Sprintf(" [excluded by %s]", strings.Join(obj.excludedBy, ", "))
		}
		lines = append(lines, line)
	}
	for _, pattern := range r.unmatchedExcludes {
		lines = append(lines, fmt.Sprintf("object graph exclusion %q did not match any object", pattern))
	}
	return strings.Join(lines, "\n")
}

func (t ObjectGraphLeakCheck) matchingExcludes(obj trackedObject) []string {
	var matches []string
	matchesPattern := func(pattern string, value string) bool {
		if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
			return strings.HasPrefix(value, prefix)
		}
		return value == pattern
	}
	for _, pattern := range t.excludes {
		if matchesPattern(pattern, obj.path) || matchesPattern(pattern, obj.typeName) {
			matches = append(matches, pattern)
		}
	}
	return matches
}

type graphWalker struct {
	objects []trackedObject
	seen    map[uintptr]struct{}
}

func (w *graphWalker) walk(v reflect.Value, objPath string) {
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
		if obj, ok := trackPointerObject(addr, objPath, v.Type().String()); ok {
			w.objects = append(w.objects, obj)
		}
		w.walk(v.Elem(), objPath)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			w.walk(v.Field(i), objPath+"."+field.Name)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i), fmt.Sprintf("%s[%d]", objPath, i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for i := 0; iter.Next(); i++ {
			w.walk(iter.Key(), fmt.Sprintf("%s[key%d]", objPath, i))
			w.walk(iter.Value(), fmt.Sprintf("%s[%d]", objPath, i))
		}
	}
}

func trackPointerObject(addr uintptr, objPath string, typeName string) (trackedObject, bool) {
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
		path:      objPath,
		typeName:  typeName,
		collected: collected,
		cleanup:   cleanup,
	}, true
}
