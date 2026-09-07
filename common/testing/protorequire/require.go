package protorequire

import (
	"fmt"
	"reflect"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

type helper interface {
	Helper()
}

// config holds settings populated by Option functions. Add new fields here for custom comparison behaviors beyond cmp
// options.
type config struct {
	cmpOpts []cmp.Option
}

// Option configures how proto comparison behaves.
type Option func(msg proto.Message, cfg *config)

// IgnoreFields returns an Option that ignores the specified fields (by proto name) on the top-level message type
// when comparing.
func IgnoreFields(fields ...protoreflect.Name) Option {
	return func(msg proto.Message, cfg *config) {
		cfg.cmpOpts = append(cfg.cmpOpts, protocmp.IgnoreFields(msg, fields...))
	}
}

type ProtoAssertions struct {
	t require.TestingT
}

func New(t require.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

// ProtoEqual compares two proto messages for equality using proto semantics. Options can be passed to customize
// comparison behavior, e.g. protorequire.IgnoreFields to exclude specific fields.
func ProtoEqual(t require.TestingT, a proto.Message, b proto.Message, opts ...Option) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	cfg := &config{}
	for _, opt := range opts {
		opt(a, cfg)
	}
	cmpOpts := append([]cmp.Option{protocmp.Transform()}, cfg.cmpOpts...)
	if diff := cmp.Diff(a, b, cmpOpts...); diff != "" {
		require.Fail(t, fmt.Sprintf("Proto mismatch (-want +got):\n%v", diff))
	}
}

func NotProtoEqual(t require.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if diff := cmp.Diff(a, b, protocmp.Transform()); diff == "" {
		require.Fail(t, "Expected protos to differ but they did not")
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
	if len(a) != len(b) {
		require.Fail(t, fmt.Sprintf("Proto slice length mismatch: want %d, got %d", len(a), len(b)))
		return
	}
	for i := range a {
		if diff := cmp.Diff(a[i], b[i], protocmp.Transform()); diff != "" {
			require.Fail(t, fmt.Sprintf("Proto mismatch at index %d (-want +got):\n%v", i, diff))
			return
		}
	}
}

// ProtoElementsMatch behaves like require.ElementsMatch except in that it works for protobuf-generated structs.
// Both arguments must be slices or arrays.
func ProtoElementsMatch(t require.TestingT, a any, b any, msgAndArgs ...any) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)
	if aVal.Len() != bVal.Len() {
		require.Fail(t, fmt.Sprintf("element count mismatch: want %d, got %d\nA: %+v\nB: %+v", aVal.Len(), bVal.Len(), a, b), msgAndArgs...)
		return
	}
	used := make([]bool, bVal.Len())
	for i := 0; i < aVal.Len(); i++ {
		want := aVal.Index(i).Interface()
		matched := false
		for j := 0; j < bVal.Len(); j++ {
			if used[j] {
				continue
			}
			if temporalproto.DeepEqual(want, bVal.Index(j).Interface()) {
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			require.Fail(t, fmt.Sprintf("element not found in B: %+v\nA: %+v\nB: %+v", want, a, b), msgAndArgs...)
			return
		}
	}
}

func DeepEqual(t require.TestingT, a any, b any) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !temporalproto.DeepEqual(a, b) {
		require.Fail(t, "Values are not deeply equal")
	}
}

func (x ProtoAssertions) ProtoEqual(a proto.Message, b proto.Message, opts ...Option) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	ProtoEqual(x.t, a, b, opts...)
}

func (x ProtoAssertions) NotProtoEqual(a proto.Message, b proto.Message) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	NotProtoEqual(x.t, a, b)
}

func (x ProtoAssertions) DeepEqual(a any, b any) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	DeepEqual(x.t, a, b)
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any, msgAndArgs ...any) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	ProtoElementsMatch(x.t, a, b, msgAndArgs...)
}
