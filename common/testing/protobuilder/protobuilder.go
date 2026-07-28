// Package protobuilder builds proto messages by filling a caller-supplied
// message with default values for the fields it left unset.
//
// Callers write an ordinary proto literal with only the fields they care about,
// and WithDefaults fills the rest from a defaults message (typically wired to a
// test environment, e.g. the namespace and test variables):
//
//	req := protobuilder.WithDefaults(
//		&foopb.Request{WorkflowId: "wf-1"}, // caller's fields win
//		&foopb.Request{Namespace: "ns", Identity: "id"}, // defaults fill the gaps
//	)
package protobuilder

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// WithDefaults returns a copy of msg with every unset top-level field populated
// from defaults. Only fields that defaults sets are considered.
//
// A field counts as set when it holds a non-zero scalar, a non-nil message, or
// a non-empty repeated/map value (proto3 presence rules). Consequently a
// default cannot override a field the caller deliberately set to a zero value;
// mutate the returned message directly for that rare case.
//
// msg is not modified, so a caller may safely reuse the same literal across
// calls.
func WithDefaults[M proto.Message](msg M, defaults M) M {
	//nolint:revive // unchecked-type-assertion: Clone preserves the dynamic type.
	out := proto.Clone(msg).(M)
	dst := out.ProtoReflect()
	defaults.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if !dst.Has(fd) {
			dst.Set(fd, v)
		}
		return true
	})
	return out
}
