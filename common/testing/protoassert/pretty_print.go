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
		if v.Elem().Type().Kind() == reflect.Pointer {
			prettyPrintPointer(b, v.Elem(), depth)
		} else {
			prettyPrintStruct(b, v.Elem(), depth)
		}
	case reflect.Map:
		prettyPrintMap(b, v, depth)
	case reflect.Pointer:
		prettyPrintPointer(b, v, depth)
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
		return
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
	b.WriteByte('}')
}

func prettyPrintSlice(b *strings.Builder, v reflect.Value, depth int) {
	if v.Len() == 0 {
		b.WriteString("[]")
		return
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

func prettyPrintPointer(b *strings.Builder, v reflect.Value, depth int) {
	if !v.IsValid() {
		return
	}
	if v.IsNil() {
		b.WriteString("nil")
	} else {
		b.WriteByte('&')
		prettyPrintStruct(b, v.Elem(), depth)
	}
}

func indent(b *strings.Builder, depth int) {
	b.WriteByte('\n')
	for i := 0; i < depth; i++ {
		b.WriteString("  ")
	}
}
