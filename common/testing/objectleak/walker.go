package objectleak

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type objectWalker struct {
	objects    []trackedObject
	seen       map[uintptr]struct{}
	pruneTypes patterns
}

type trackedObject struct {
	addr      uintptr
	path      path
	typeName  string
	collected *atomic.Bool
	cleanup   runtime.Cleanup
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
		if obj, ok := trackPointerObject(ptr, addr, path, v.Type().String()); ok {
			w.objects = append(w.objects, obj)
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
