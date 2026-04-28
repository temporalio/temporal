package umpire

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

func TestRPCRegistryRejectsDuplicateSpec(t *testing.T) {
	var registry RPCRegistry
	spec := RPCSpecFor(
		"/test.Duration/Call",
		&durationpb.Duration{},
		&durationpb.Duration{},
	)

	registry.Register(spec)

	require.Panics(t, func() {
		registry.Register(spec)
	})
}

type durationRPCSpec struct {
	Seconds FieldSpec[int64]
	Nanos   FieldSpec[int32]
}

func (durationRPCSpec) RPCMeta() RPCMessageMeta[*durationpb.Duration] {
	return RPCMessageMeta[*durationpb.Duration]{
		Message: &durationpb.Duration{},
		Fields: []RPCFieldMeta{
			{Name: "Seconds", Path: "seconds"},
			{Name: "Nanos", Path: "nanos"},
		},
	}
}

func TestBuildRPCSpecUsesFieldOptions(t *testing.T) {
	spec := BuildRPCSpec(
		"/test.Duration/Call",
		durationRPCSpec{
			Seconds: Field[int64](Mutable[int64](), Mutation[int64]("wrong-seconds")),
			Nanos:   Field[int32](Ignored[int32]()),
		},
		durationRPCSpec{
			Seconds: Field[int64](),
			Nanos:   Field[int32](),
		},
	)

	require.Len(t, spec.RequestFields, 1)
	require.Equal(t, "seconds", spec.RequestFields[0].Path)
	require.Equal(t, RPCFieldMutationRandom, spec.RequestFields[0].Mutation)
	require.Len(t, spec.ResponseFields, 2)
	require.Len(t, spec.Mutations, 1)
	require.Equal(t, "wrong-seconds", spec.Mutations[0].Name)
	require.Equal(t, []RPCFieldSpec{spec.RequestFields[0]}, spec.Mutations[0].Fields)
}

func TestBuildRPCSpecRequiresExhaustiveFields(t *testing.T) {
	require.PanicsWithError(t,
		"request spec for /test.Duration/Call: field nanos is not set",
		func() {
			BuildRPCSpec(
				"/test.Duration/Call",
				durationRPCSpec{
					Seconds: Field[int64](),
				},
				durationRPCSpec{
					Seconds: Field[int64](),
					Nanos:   Field[int32](),
				},
			)
		},
	)
}

func TestRPCRegistryStrategyRunsFieldAssertions(t *testing.T) {
	var requestAsserted atomic.Bool
	var responseAsserted atomic.Bool
	var registry RPCRegistry
	registry.Register(BuildRPCSpec(
		"/test.Duration/Call",
		durationRPCSpec{
			Seconds: Field[int64](Assert[int64](func(t *T, value int64) {
				requestAsserted.Store(true)
				t.Equal(int64(1), value)
			})),
			Nanos: Field[int32](),
		},
		durationRPCSpec{
			Seconds: Field[int64](),
			Nanos: Field[int32](Assert[int32](func(t *T, value int32) {
				responseAsserted.Store(true)
				t.Equal(int32(2), value)
			})),
		},
	))
	strategy := RPCRegistryStrategy(nil, &registry)

	rapid.Check(t, func(rt *rapid.T) {
		pt := newT(rt)
		strategy.SetT(pt)
		reply := &durationpb.Duration{}

		err := strategy.Client(
			context.Background(),
			"/test.Duration/Call",
			durationpb.New(time.Second),
			reply,
			nil,
			func(
				ctx context.Context,
				method string,
				req, reply any,
				cc *grpc.ClientConn,
				opts ...grpc.CallOption,
			) error {
				reply.(*durationpb.Duration).Nanos = 2
				return nil
			},
		)

		require.NoError(t, err)
	})
	require.True(t, requestAsserted.Load())
	require.True(t, responseAsserted.Load())
}

func TestRPCRegistryStrategySkipsFieldAssertionsForMutatedRequests(t *testing.T) {
	var asserted atomic.Bool
	var registry RPCRegistry
	registry.Register(BuildRPCSpec(
		"/test.Duration/Call",
		durationRPCSpec{
			Seconds: Field[int64](Mutable[int64](), Assert[int64](func(t *T, value int64) {
				asserted.Store(true)
				t.FailNow()
			})),
			Nanos: Field[int32](),
		},
		durationRPCSpec{
			Seconds: Field[int64](),
			Nanos:   Field[int32](),
		},
	))
	interceptor := NewInterceptor(
		RPCMutationStrategy(NewRequestMutations(WithRequestMutationProbability(1, 1)), &registry, nil),
		RPCRegistryStrategy(nil, &registry),
	)

	rapid.Check(t, func(rt *rapid.T) {
		pt := newT(rt)
		interceptor.SetRequestMutationT(pt)

		err := interceptor.UnaryClient()(
			context.Background(),
			"/test.Duration/Call",
			durationpb.New(time.Second),
			&durationpb.Duration{},
			nil,
			func(
				ctx context.Context,
				method string,
				req, reply any,
				cc *grpc.ClientConn,
				opts ...grpc.CallOption,
			) error {
				return nil
			},
		)

		require.NoError(t, err)
	})
	require.False(t, asserted.Load())
}

func TestRPCRegistryStrategyRecordsUnknownRPCIssue(t *testing.T) {
	u := &Umpire{}
	var registry RPCRegistry

	interceptor := RPCRegistryStrategy(u, &registry, WithUnregisteredRPCViolations()).Client
	err := interceptor(
		context.Background(),
		"/test.Duration/Call",
		durationpb.New(time.Second),
		&durationpb.Duration{},
		nil,
		func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			opts ...grpc.CallOption,
		) error {
			return nil
		},
	)

	require.NoError(t, err)
	u.AddRule(RPCRegistryRule())
	violations := u.CheckRules(false)
	require.Len(t, violations, 1)
	require.Equal(t, "rpc-registry", violations[0].Rule)
	require.Equal(t, "unregistered RPC", violations[0].Message)
	require.Equal(t, "/test.Duration/Call", violations[0].Tags["method"])
}

func TestRequestMutationInterceptorUsesRPCRegistryFields(t *testing.T) {
	var registry RPCRegistry
	nanos := NewRPCFieldRef(&durationpb.Duration{}, 2)
	registry.Register(NewRPCSpec(
		"/test.Duration/Call",
		&durationpb.Duration{},
		&durationpb.Duration{},
	).RequestFields(
		MutableRPCFieldOf(nanos),
	).Build())
	mutations := NewRequestMutations(WithRequestMutationProbability(1, 1))

	rapid.Check(t, func(rt *rapid.T) {
		pt := newT(rt)
		mutations.SetRequestMutationT(pt)
		req := durationpb.New(time.Second)

		err := mutations.UnaryClientInterceptorForRegistry(&registry, nil)(
			context.Background(),
			"/test.Duration/Call",
			req,
			&durationpb.Duration{},
			nil,
			func(
				ctx context.Context,
				method string,
				callReq, reply any,
				cc *grpc.ClientConn,
				opts ...grpc.CallOption,
			) error {
				require.Equal(t, int64(1), callReq.(*durationpb.Duration).Seconds)
				return nil
			},
		)

		require.NoError(t, err)
	})
}
