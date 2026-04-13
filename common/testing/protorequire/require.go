package protorequire

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/protoassert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

type helper interface {
	Helper()
}

type ProtoAssertions struct {
	t require.TestingT
}

func New(t require.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

func ProtoEqual(t require.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoEqual(t, a, b) {
		t.FailNow()
	}
}

// ProtoEqualIgnoreFields compares two proto messages for equality, ignoring the specified fields on the given message
// type. Fields are specified by their proto name (snake_case).
func ProtoEqualIgnoreFields(t require.TestingT, a proto.Message, b proto.Message, msgType proto.Message, fields ...protoreflect.Name) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform(), protocmp.IgnoreFields(msgType, fields...)); diff != "" {
		require.Fail(t, fmt.Sprintf("Proto mismatch (-want +got):\n%v", diff))
	}
}

func NotProtoEqual(t require.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.NotProtoEqual(t, a, b) {
		t.FailNow()
	}
}

// ProtoSliceEqual compares elements in a slice of proto.Message.
// This is not a method on the suite type because methods cannot have
// generic parameters and slice casting (say from []historyEvent) to
// []proto.Message is impossible
func ProtoSliceEqual[T proto.Message](t require.TestingT, a []T, b []T) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoSliceEqual(t, a, b) {
		t.FailNow()
	}
}

func (x ProtoAssertions) ProtoEqual(a proto.Message, b proto.Message) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) NotProtoEqual(a proto.Message, b proto.Message) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.NotProtoEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) DeepEqual(a any, b any) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.DeepEqual(x.t, a, b) {
		x.t.FailNow()
	}
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return protoassert.ProtoElementsMatch(x.t, a, b)
}

func (x ProtoAssertions) ProtoEqualIgnoreFields(a proto.Message, b proto.Message, msgType proto.Message, fields ...protoreflect.Name) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	ProtoEqualIgnoreFields(x.t, a, b, msgType, fields...)
}
