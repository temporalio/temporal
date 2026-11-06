package testlogger

import (
	"fmt"
	"reflect"
	"strings"

	"go.temporal.io/server/common/log/tag"
)

// formatValue renders a tag value for matching and failure messages.
func formatValue(t tag.Tag) string {
	return formatAny(t.Value())
}

// formatAny renders v without ever deep-walking it. Passing arbitrary values to fmt's %v traverses
// pointers, structs and maps without taking any locks, which races with concurrent writers when the
// value is a live object (e.g. a service component passed to tag.Any). Only fmt.Stringer, error and
// basic kinds are rendered by content; slices and arrays are rendered element-wise with the same
// rules, and everything else is rendered as its type (plus address for pointers).
func formatAny(v any) string {
	switch v := v.(type) {
	case nil:
		return "<nil>"
	case fmt.Stringer:
		return v.String()
	case string:
		return v
	case error:
		return v.Error()
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String:
		return fmt.Sprintf("%v", v)
	case reflect.Slice, reflect.Array:
		var b strings.Builder
		b.WriteByte('[')
		for i := range rv.Len() {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(formatAny(rv.Index(i).Interface()))
		}
		b.WriteByte(']')
		return b.String()
	case reflect.Pointer:
		if rv.IsNil() {
			return "<nil>"
		}
		return fmt.Sprintf("%T(%p)", v, v)
	default:
		return fmt.Sprintf("%T", v)
	}
}
