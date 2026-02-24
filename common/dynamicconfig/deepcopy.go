package dynamicconfig

import (
	"fmt"
	"reflect"
	"time"
)

// deepCopyForMapstructure does a simple deep copy of T. Fancy cases (anything other than plain old data)
// is not handled and will panic.
func deepCopyForMapstructure[T any](t T) T {
	// nolint:revive // this will be triggered from a static initializer before it can be triggered from production code
	return deepCopyValue(reflect.ValueOf(t)).Interface().(T)
}

func deepCopyValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.String:
		nv := reflect.New(v.Type()).Elem()
		nv.Set(v)
		return nv
	case reflect.Array:
		nv := reflect.New(v.Type()).Elem()
		for i := range v.Len() {
			nv.Index(i).Set(deepCopyValue(v.Index(i)))
		}
		return nv
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		nv := reflect.MakeMapWithSize(v.Type(), v.Len())
		for i := v.MapRange(); i.Next(); {
			nv.SetMapIndex(i.Key(), deepCopyValue(i.Value()))
		}
		return nv
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		return deepCopyValue(v.Elem()).Addr()
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		nv := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			nv.Index(i).Set(deepCopyValue(v.Index(i)))
		}
		return nv
	case reflect.Struct:
		// Special case for time.Time: it has unexported fields so we can't copy it field by
		// field, but we can copy zero values (which is all we need for default values).
		if v.Type() == reflect.TypeFor[time.Time]() {
			if v.Interface().(time.Time).IsZero() {
				return reflect.ValueOf(time.Time{})
			}
			// nolint:forbidigo // this will be triggered from a static initializer before it can be triggered from production code
			panic(fmt.Sprintf("Can't deep copy non-zero time.Time: %v", v.Interface()))
		}
		nv := reflect.New(v.Type()).Elem()
		for i := range v.Type().NumField() {
			nv.Field(i).Set(deepCopyValue(v.Field(i)))
		}
		return nv
	case reflect.Interface, reflect.Func, reflect.Chan:
		// only nil values of any other reference types allowed!
		if v.IsNil() {
			return v
		}
		fallthrough
	default:
		// nolint:forbidigo // this will be triggered from a static initializer before it can be triggered from production code
		panic(fmt.Sprintf("Can't deep copy value of type %s: %v", v.Type(), v))
	}
}
