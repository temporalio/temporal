package umpire

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

// Strategy is one composable layer of behavior for the umpire interceptor:
// fault injection, request mutation, traffic observation, etc. Strategies
// plug into the client side, the server side, or both, and are chained in
// declaration order — the first listed runs outermost.
//
// Each strategy is a value with optional function fields; fields left nil
// are skipped. SetT receives the current rapid step before each action so
// per-step random draws stay deterministic.
type Strategy struct {
	Name   string
	Client grpc.UnaryClientInterceptor
	Server grpc.UnaryServerInterceptor
	SetT   func(*T)
}

// Interceptor composes a set of Strategy values into one client-side and one
// server-side gRPC interceptor. Add or remove behavior by passing different
// strategies to New — there's no fixed set of options.
//
// Embed *Interceptor in the property test model so each rapid step pushes
// its *T through to every strategy automatically (Interceptor satisfies the
// requestMutationTSetter contract used by Model.setT).
type Interceptor struct {
	strategies []Strategy
}

// NewInterceptor builds an Interceptor that runs the given strategies. Order
// matters: the first listed runs outermost on the client and server
// interceptor chains. Strategies that apply only to one side leave the other
// field nil.
func NewInterceptor(strategies ...Strategy) *Interceptor {
	return &Interceptor{strategies: strategies}
}

// UnaryClient returns the composed client-side interceptor. Wire via
// env.SetClientUnaryInterceptor.
func (i *Interceptor) UnaryClient() grpc.UnaryClientInterceptor {
	var ints []grpc.UnaryClientInterceptor
	for _, s := range i.strategies {
		if s.Client != nil {
			ints = append(ints, s.Client)
		}
	}
	return chainUnaryClient(ints...)
}

// UnaryServer returns the composed server-side interceptor. Wire via
// env.SetServerUnaryInterceptor.
func (i *Interceptor) UnaryServer() grpc.UnaryServerInterceptor {
	var ints []grpc.UnaryServerInterceptor
	for _, s := range i.strategies {
		if s.Server != nil {
			ints = append(ints, s.Server)
		}
	}
	return chainUnaryServer(ints...)
}

// SetRequestMutationT pushes the current rapid step into every strategy.
// Implements requestMutationTSetter so Model.setT picks it up via embedding.
func (i *Interceptor) SetRequestMutationT(t *T) {
	for _, s := range i.strategies {
		if s.SetT != nil {
			s.SetT(t)
		}
	}
}

// MethodFault configures the fault choices for a single gRPC method. At most
// one fault fires per call, resolved in order: Block, Cancel, Error, Delay.
// Unset probabilities disable that fault. Errors must be non-empty for
// Error to fire; MaxDelay must be > 0 for Delay to fire.
type MethodFault struct {
	Block    Probability
	Cancel   Probability
	Error    Probability
	Delay    Probability
	Errors   []error
	MaxDelay time.Duration
}

// FaultStrategy returns a Strategy that injects faults on either side of the
// chain. The same plan is consulted on the client and server side; wire only
// the side(s) you want.
func FaultStrategy(plan map[string]MethodFault) Strategy {
	var currentT atomic.Pointer[T]
	inject := func(ctx context.Context, method string) error {
		cfg, ok := plan[method]
		if !ok {
			return nil
		}
		t := currentT.Load()
		if t == nil {
			return nil
		}
		if cfg.Block.fires(t, "fault."+method+".block?") {
			t.Logf("fault: method=%s type=block", method)
			<-ctx.Done()
			return ctx.Err()
		}
		if cfg.Cancel.fires(t, "fault."+method+".cancel?") {
			t.Logf("fault: method=%s type=cancel", method)
			return status.Error(codes.Canceled, "umpire: synthetic cancel")
		}
		if len(cfg.Errors) > 0 && cfg.Error.fires(t, "fault."+method+".error?") {
			err := drawGen(t, "fault."+method+".error", rapid.SampledFrom(cfg.Errors))
			t.Logf("fault: method=%s type=error err=%v", method, err)
			return err
		}
		if cfg.MaxDelay > 0 && cfg.Delay.fires(t, "fault."+method+".delay?") {
			ns := drawGen(t, "fault."+method+".delay", rapid.Int64Range(1, int64(cfg.MaxDelay)))
			delay := time.Duration(ns)
			t.Logf("fault: method=%s type=delay duration=%s", method, delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		return nil
	}
	return Strategy{
		Name: "fault",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			if err := inject(ctx, method); err != nil {
				return err
			}
			return invoker(ctx, method, req, reply, cc, opts...)
		},
		Server: func(
			ctx context.Context,
			req any,
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			if err := inject(ctx, info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		},
		SetT: func(t *T) { currentT.Store(t) },
	}
}

// MutationStrategy returns a Strategy that mutates outbound proto requests
// via RequestMutations. Client-side only — by the time a request reaches a
// server handler it is already serialized.
func MutationStrategy(
	mutations *RequestMutations,
	shouldMutateMethod func(string) bool,
	fallbackOnMutationError func(error) bool,
) Strategy {
	return Strategy{
		Name:   "mutation",
		Client: mutations.UnaryClientInterceptor(shouldMutateMethod, fallbackOnMutationError),
		SetT:   mutations.SetRequestMutationT,
	}
}

func RPCMutationStrategy(
	mutations *RequestMutations,
	registry *RPCRegistry,
	fallbackOnMutationError func(error) bool,
) Strategy {
	return Strategy{
		Name:   "mutation",
		Client: mutations.UnaryClientInterceptorForRegistry(registry, fallbackOnMutationError),
		SetT:   mutations.SetRequestMutationT,
	}
}

// ObservedCall is recorded for every gRPC call seen by ObserveStrategy.
// Rules can scan history for *ObservedCall to reason about RPC traffic
// without each model action recording facts manually.
type ObservedCall struct {
	Method string
	Req    any
	Resp   any
	Err    error
}

func (o *ObservedCall) Key() string { return o.Method }

// ObserveStrategy returns a Strategy that records every client-side call as
// an *ObservedCall fact in u's history. Records on the client side only —
// recording on both sides would duplicate observations for calls made
// through the test client.
func ObserveStrategy(u *Umpire) Strategy {
	var currentT atomic.Pointer[T]
	return Strategy{
		Name: "observe",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err != nil {
				if t := currentT.Load(); t != nil {
					t.Logf("rpc error: method=%s err=%v", method, err)
				}
			}
			u.Record(&ObservedCall{Method: method, Req: req, Resp: reply, Err: err})
			return err
		},
		SetT: func(t *T) { currentT.Store(t) },
	}
}

// chainUnaryClient composes interceptors. The first listed runs outermost
// (sees the call first, sees the response last); the last runs immediately
// around the actual invoker.
func chainUnaryClient(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	if len(interceptors) == 0 {
		return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}
	if len(interceptors) == 1 {
		return interceptors[0]
	}
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var build func(idx int) grpc.UnaryInvoker
		build = func(idx int) grpc.UnaryInvoker {
			if idx == len(interceptors) {
				return invoker
			}
			return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return interceptors[idx](ctx, method, req, reply, cc, build(idx+1), opts...)
			}
		}
		return build(0)(ctx, method, req, reply, cc, opts...)
	}
}

// chainUnaryServer composes server interceptors. The first listed runs
// outermost (sees the call first, sees the response last).
func chainUnaryServer(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	if len(interceptors) == 0 {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	if len(interceptors) == 1 {
		return interceptors[0]
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var build func(idx int) grpc.UnaryHandler
		build = func(idx int) grpc.UnaryHandler {
			if idx == len(interceptors) {
				return handler
			}
			return func(ctx context.Context, req any) (any, error) {
				return interceptors[idx](ctx, req, info, build(idx+1))
			}
		}
		return build(0)(ctx, req)
	}
}
