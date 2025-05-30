// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package dynamicconfig

import (
	"fmt"
	"reflect"
)

// deepCopyForMapstructure does a simple deep copy of T. Fancy cases (anything other than plain
// old data) is not handled and will panic.
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
		nv := reflect.New(v.Type()).Elem()
		for i := range v.Type().NumField() {
			nv.Field(i).Set(deepCopyValue(v.Field(i)))
		}
		return nv
	default:
		// nolint:forbidigo // this will be triggered from a static initializer before it can be triggered from production code
		panic(fmt.Sprintf("Can't deep copy value of type %T: %v", v, v))
	}
}
