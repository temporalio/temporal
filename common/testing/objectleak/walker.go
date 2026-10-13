package objectleak

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// The runtime may batch pointer-free allocations smaller than 16 bytes into a
// shared block. A cleanup attached to one of those allocations may not run
// while any allocation in the block is reachable, so those objects cannot be
// tracked individually.
const runtimeTinyAllocationSize = 16

type trackedObject struct {
	addr      uintptr
	path      path
	typeName  string
	collected *atomic.Bool
	cleanup   runtime.Cleanup
}

type objectWalker struct {
	objects    []trackedObject
	seen       map[uintptr]struct{}
	pruneTypes patterns
}

func newObjectWalker(pruneTypes patterns) objectWalker {
	return objectWalker{
		seen:       make(map[uintptr]struct{}),
		pruneTypes: pruneTypes,
	}
}

func (w *objectWalker) track(root any) {
	w.walk(reflect.ValueOf(root), nil)
}

func (w *objectWalker) walk(v reflect.Value, path path) {
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
		ptr := v.UnsafePointer()
		addr := uintptr(ptr)
		if _, ok := w.seen[addr]; ok {
			return
		}
		w.seen[addr] = struct{}{}
		if isTrackableObject(v.Type().Elem()) {
			if obj, ok := trackPointerObject(ptr, addr, path, v.Type().String()); ok {
				w.objects = append(w.objects, obj)
			}
		}
		if w.shouldPrune(v.Type()) {
			return
		}
		w.walk(v.Elem(), path)
	case reflect.Struct:
		if w.shouldPrune(v.Type()) {
			return
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			w.walk(v.Field(i), path.field(field.Name))
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i), path.index(i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for i := 0; iter.Next(); i++ {
			w.walk(iter.Key(), path.mapKey(i))
			w.walk(iter.Value(), path.index(i))
		}
	default:
		return
	}
}

func isTrackableObject(t reflect.Type) bool {
	return t.Size() >= runtimeTinyAllocationSize || typeContainsPointers(t)
}

func typeContainsPointers(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Array:
		return t.Len() > 0 && typeContainsPointers(t.Elem())
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer,
		reflect.Slice, reflect.String, reflect.UnsafePointer:
		return true
	case reflect.Struct:
		for field := range t.Fields() {
			if typeContainsPointers(field.Type) {
				return true
			}
		}
	default:
		return false
	}
	return false
}

func (w *objectWalker) shouldPrune(t reflect.Type) bool {
	return w.pruneTypes.matchType(t)
}

func trackPointerObject(ptr unsafe.Pointer, addr uintptr, path path, typeName string) (trackedObject, bool) {
	collected := &atomic.Bool{}
	var cleanup runtime.Cleanup
	ok := true
	func() {
		// Some reflected pointers are not valid heap objects for AddCleanup.
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		//nolint:govet // The checker must attach cleanup to reflected heap addresses.
		cleanup = runtime.AddCleanup((*byte)(ptr), func(collected *atomic.Bool) {
			collected.Store(true)
		}, collected)
	}()
	if !ok || cleanup == (runtime.Cleanup{}) {
		return trackedObject{}, false
	}
	return trackedObject{
		addr:      addr,
		path:      path,
		typeName:  typeName,
		collected: collected,
		cleanup:   cleanup,
	}, true
}
