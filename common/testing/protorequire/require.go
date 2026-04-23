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
