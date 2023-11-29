// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package protoassert

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var isPublic = regexp.MustCompile("^[A-Z]")

// pretty-print the public state of the proto.Message. we don't use prettyPrint the way testify does
// as the private state isn't worth comparing; it's all for serialization.
//
// This is custom and not from testify.
func prettyPrint(values any) string {
	var b strings.Builder
	prettyPrintAny(&b, reflect.ValueOf(values), 0)
	return b.String()
}

func prettyPrintAny(b *strings.Builder, v reflect.Value, depth int) {
	switch v.Type().Kind() {
	case reflect.Array:
		prettyPrintSlice(b, v, depth)
	case reflect.Chan:
		panic("Not implemented")
	case reflect.Func:
		panic("Not implemented")
	case reflect.Interface:
		prettyPrintStruct(b, v.Elem(), depth)
	case reflect.Map:
		prettyPrintMap(b, v, depth)
	case reflect.Pointer:
		if v.IsValid() {
			if v.IsNil() {
				b.WriteString("nil")
			} else {
				b.WriteByte('&')
				prettyPrintStruct(b, v.Elem(), depth)
			}
		}
	case reflect.Slice:
		prettyPrintSlice(b, v, depth)
	case reflect.Struct:
		prettyPrintStruct(b, v, depth)
	default:
		fmt.Fprintf(b, "%v", v.Interface())
	}

}

func prettyPrintMap(b *strings.Builder, v reflect.Value, depth int) {
	fmt.Fprintf(b, "map[%s]%s", v.Type().Key().Name(), v.Type().Elem().Name())
	if v.Len() == 0 {
		b.WriteString("{}")
	}

	b.WriteByte('{')
	iter := v.MapRange()
	for iter.Next() {
		indent(b, depth+1)

		prettyPrintAny(b, iter.Key(), depth+1)
		b.WriteString(": ")
		prettyPrintAny(b, iter.Value(), depth+1)
	}
	indent(b, depth)
	b.WriteByte('\n')
}

func prettyPrintSlice(b *strings.Builder, v reflect.Value, depth int) {
	if v.Len() == 0 {
		b.WriteString("[]")
	}
	b.WriteByte('[')
	for j := 0; j < v.Len(); j++ {
		indent(b, depth+1)
		prettyPrintAny(b, v.Index(j), depth+1)
	}
	indent(b, depth)
	b.WriteByte(']')
}

func prettyPrintStruct(b *strings.Builder, v reflect.Value, depth int) {
	ty := v.Type()
	b.WriteString(ty.Name())
	if ty.NumField() == 0 {
		b.WriteString("{}")
		return
	}

	b.WriteString("{")
	for i := 0; i < ty.NumField(); i++ {
		f := ty.Field(i)
		vf := v.Field(i)
		if !isPublic.MatchString(f.Name) {
			continue
		}
		indent(b, depth+1)
		b.WriteString(f.Name)
		b.WriteString(": ")
		prettyPrintAny(b, vf, depth+1)
	}
	indent(b, depth)
	b.WriteString("}")
}

func indent(b *strings.Builder, depth int) {
	b.WriteByte('\n')
	for i := 0; i < depth; i++ {
		b.WriteString("  ")
	}
}
