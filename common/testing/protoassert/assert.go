// Assert wraps testify's require package with useful helpers
package protoassert

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

type helper interface {
	Helper()
}

type ProtoAssertions struct {
	t assert.TestingT
}

func New(t assert.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

// ProtoEqual compares two proto.Message objects for equality
func ProtoEqual(t assert.TestingT, a proto.Message, b proto.Message) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff != "" {
		return assert.Fail(t, fmt.Sprintf("Proto mismatch (-want +got):\n%v", diff))
	}
	return true
}

func NotProtoEqual(t assert.TestingT, a proto.Message, b proto.Message) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff == "" {
		return assert.Fail(t, "Expected protos to differ but they did not")
	}
	return true
}

// ProtoSliceEqual compares elements in a slice of proto.Message.
// This is not a method on the suite type because methods cannot have
// generic parameters and slice casting (say from []historyEvent) to
// []proto.Message is impossible
func ProtoSliceEqual[T proto.Message](t assert.TestingT, a []T, b []T) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if diff := cmp.Diff(a[i], b[i], protocmp.Transform()); diff != "" {
			return assert.Fail(t, fmt.Sprintf("Proto mismatch at index %d (-want +got):\n%v", i, diff))
		}
	}

	return true
}

// ProtoElementsMatch behaves like assert.ElementsMatch except in that it works for
// google/protobuf-generated structs
func ProtoElementsMatch(t assert.TestingT, a any, b any, msgAndArgs ...any) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}

	if isEmpty(a) && isEmpty(b) {
		return true
	}

	extraA, extraB := diffLists(a, b)
	if len(extraA) == 0 && len(extraB) == 0 {
		return true
	}

	return assert.Fail(t, formatListDiff(a, b, extraA, extraB), msgAndArgs...)
}

func DeepEqual(t assert.TestingT, a any, b any) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}

	return temporalproto.DeepEqual(a, b)
}

func (x ProtoAssertions) ProtoEqual(a proto.Message, b proto.Message) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	return ProtoEqual(x.t, a, b)
}

func (x ProtoAssertions) NotProtoEqual(a proto.Message, b proto.Message) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	return NotProtoEqual(x.t, a, b)
}

func (x ProtoAssertions) DeepEqual(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return temporalproto.DeepEqual(a, b)
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return ProtoElementsMatch(x.t, a, b)
}
