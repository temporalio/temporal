package leakcheck

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

type objectWalker struct {
	objects []trackedObject
	seen    map[uintptr]struct{}
}

type trackedObject struct {
	path      objectPath
	typeName  string
	collected *atomic.Bool
	cleanup   runtime.Cleanup
}

func newGraphWalker() objectWalker {
	return objectWalker{
		seen: make(map[uintptr]struct{}),
	}
}

func (w *objectWalker) track(rootPath string, root any) {
	w.walk(reflect.ValueOf(root), newObjectPath(rootPath))
}

func (w *objectWalker) walk(v reflect.Value, path objectPath) {
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
			w.walk(v.Field(i), path.Field(field.Name))
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			w.walk(v.Index(i), path.Index(i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for i := 0; iter.Next(); i++ {
			w.walk(iter.Key(), path.MapKey(i))
			w.walk(iter.Value(), path.Index(i))
		}
	}
}

func trackPointerObject(addr uintptr, path objectPath, typeName string) (trackedObject, bool) {
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
