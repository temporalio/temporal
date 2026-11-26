// Package protodiff compares arbitrary values, comparing any proto messages inside them by protobuf content.
package protodiff

import (
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

// Diff returns a description of the differences between want and got, or "" if they are equal. Proto messages
// anywhere in the values are compared with proto semantics. Structs with unexported fields make cmp panic unless an
// option such as cmp.AllowUnexported covers them.
func Diff(want, got any, opts ...cmp.Option) string {
	return cmp.Diff(want, got, withProtoTransform(opts)...)
}

// Equal reports whether want and got are equal under the same rules as Diff.
func Equal(want, got any, opts ...cmp.Option) bool {
	return cmp.Equal(want, got, withProtoTransform(opts)...)
}

func withProtoTransform(opts []cmp.Option) []cmp.Option {
	return append([]cmp.Option{protocmp.Transform()}, opts...)
}
