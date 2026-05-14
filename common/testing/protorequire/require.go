package protorequire

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/testing/protoassert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

type helper interface {
	Helper()
}

type failNower interface {
	FailNow()
}

type suiteTestingT interface {
	T() *testing.T
}

func failNow(t assert.TestingT) {
	if f, ok := t.(failNower); ok {
		f.FailNow()
		return
	}
	if s, ok := t.(suiteTestingT); ok {
		s.T().FailNow()
		return
	}
	panic("protorequire: test failed and t is missing FailNow()")
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
	t assert.TestingT
}

func New(t assert.TestingT) ProtoAssertions {
	return ProtoAssertions{t}
}

// ProtoEqual compares two proto messages for equality using proto semantics. Options can be passed to customize
// comparison behavior, e.g. protorequire.IgnoreFields to exclude specific fields.
func ProtoEqual(t assert.TestingT, a proto.Message, b proto.Message, opts ...Option) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	cfg := &config{}
	for _, opt := range opts {
		opt(a, cfg)
	}
	cmpOpts := append([]cmp.Option{protocmp.Transform()}, cfg.cmpOpts...)
	if diff := cmp.Diff(a, b, cmpOpts...); diff != "" {
		assert.Fail(t, fmt.Sprintf("Proto mismatch (-want +got):\n%v", diff))
		failNow(t)
	}
}

func NotProtoEqual(t assert.TestingT, a proto.Message, b proto.Message) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.NotProtoEqual(t, a, b) {
		failNow(t)
	}
}

// ProtoSliceEqual compares elements in a slice of proto.Message.
// This is not a method on the suite type because methods cannot have
// generic parameters and slice casting (say from []historyEvent) to
// []proto.Message is impossible
func ProtoSliceEqual[T proto.Message](t assert.TestingT, a []T, b []T) {
	if th, ok := t.(helper); ok {
		th.Helper()
	}
	if !protoassert.ProtoSliceEqual(t, a, b) {
		failNow(t)
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
		failNow(x.t)
	}
}

func (x ProtoAssertions) DeepEqual(a any, b any) {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}
	if !protoassert.DeepEqual(x.t, a, b) {
		failNow(x.t)
	}
}

func (x ProtoAssertions) ProtoElementsMatch(a any, b any) bool {
	if th, ok := x.t.(helper); ok {
		th.Helper()
	}

	return protoassert.ProtoElementsMatch(x.t, a, b)
}
