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

// ObjectGraphTracker tracks objects reachable from a root and reports objects
// that remain reachable after GC.
type ObjectGraphTracker struct {
	objects  []trackedObject
	excludes []string
}

type trackedObject struct {
	path      string
	typeName  string
	collected *atomic.Bool
	cleanup   runtime.Cleanup
}

type Option func(*ObjectGraphTracker)

// WithExclude skips retained-object failures whose reflected path or type name
// matches pattern. A trailing '*' matches any suffix.
func WithExclude(pattern string) Option {
	return func(t *ObjectGraphTracker) {
		t.excludes = append(t.excludes, pattern)
	}
}

func NewObjectGraphTracker(rootPath string, root any, opts ...Option) ObjectGraphTracker {
	t := ObjectGraphTracker{}
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

func (t ObjectGraphTracker) Failures() error {
	var failures []error
	for _, obj := range t.objects {
		if obj.collected.Load() || t.excluded(obj) {
			continue
		}
		failures = append(failures, fmt.Errorf("retained graph object %s (%s)", obj.path, obj.typeName))
	}
	return errors.Join(failures...)
}

func (t ObjectGraphTracker) excluded(obj trackedObject) bool {
	for _, pattern := range t.excludes {
		if matchesPattern(pattern, obj.path) || matchesPattern(pattern, obj.typeName) {
			return true
		}
	}
	return false
}

func matchesPattern(pattern string, value string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(value, prefix)
	}
	return value == pattern
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
		cleanup = runtime.AddCleanup((*byte)(unsafe.Pointer(addr)), markCollected, collected)
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

func markCollected(collected *atomic.Bool) {
	collected.Store(true)
}
