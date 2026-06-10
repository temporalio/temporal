package faultinjection

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FaultKind identifies a kind of fault that the gRPC fault injector can inject: added latency
// (Latency) and transient retryable errors (Error). Both are recoverable — they model a slow,
// flaky network/peer that the system is expected to ride out.
type FaultKind string

const (
	// FaultLatency injects latency before a call reaches its handler. The latency is
	// drawn from a normal distribution. Unlike the persistence faults, it returns no
	// error: it models slow, contended machines and lets the caller's own deadline fail
	// naturally under the added latency.
	FaultLatency FaultKind = "Latency"
	// FaultError injects a transient, retryable error before the handler runs. The system is
	// expected to recover (the internal clients retry these codes), so it exercises retry and
	// idempotency paths without causing legitimate test failures.
	FaultError FaultKind = "Error"
)

// transientErrorCodes are retryable gRPC codes injected by the Error fault. They are
// converted to the corresponding retryable serviceerror on the client side.
var transientErrorCodes = []codes.Code{codes.Unavailable, codes.ResourceExhausted}

// LatencyConfig configures the latency distribution for the Latency fault.
type LatencyConfig struct {
	// MeanMs is the mean of the normal distribution latency is drawn from.
	MeanMs float64
	// StddevMs is the standard deviation of that distribution.
	StddevMs float64
	// MaxMs clamps the sampled latency. 0 means unbounded.
	MaxMs float64
}

// Config configures the gRPC fault injector.
type Config struct {
	// Rate is the probability (0..1) that any given RPC receives a Latency fault. 0 disables it.
	Rate float64
	// ErrorRate is the probability (0..1) that any given RPC fails with a transient, retryable
	// error (the Error fault). 0 disables it. Independent of Rate.
	ErrorRate float64
	// Seed seeds the random number generator for reproducible runs. 0 uses a time-based seed.
	Seed int64
	// Latency configures the Latency fault's latency distribution.
	Latency LatencyConfig
	// Methods optionally restricts injection to gRPC full method names containing one of these
	// substrings. Empty means all methods (except those excluded by default).
	Methods []string
}

// Enabled reports whether the config would inject any fault.
func (c Config) Enabled() bool {
	return c.Rate > 0 || c.ErrorRate > 0
}

// defaultExcludedMethods are never faulted to avoid breaking health checking, which the
// test cluster relies on to detect service readiness.
var defaultExcludedMethods = []string{
	"grpc.health",
}

// Injector injects faults into gRPC server calls. It is safe for concurrent use.
type Injector struct {
	cfg Config

	rndMu sync.Mutex
	rnd   *rand.Rand // rand is not thread-safe; guarded by rndMu
}

// New returns an Injector for the given config.
func New(cfg Config) *Injector {
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Injector{
		cfg: cfg,
		rnd: rand.New(rand.NewSource(seed)),
	}
}

// shouldFault rolls the dice and reports whether this call should be faulted.
func (i *Injector) shouldFault(fullMethod string) bool {
	if !i.cfg.Enabled() || i.excluded(fullMethod) {
		return false
	}
	i.rndMu.Lock()
	roll := i.rnd.Float64()
	i.rndMu.Unlock()
	return roll < i.cfg.Rate
}

// excluded reports whether the method is outside the configured injection scope.
func (i *Injector) excluded(fullMethod string) bool {
	for _, m := range defaultExcludedMethods {
		if strings.Contains(fullMethod, m) {
			return true
		}
	}
	if len(i.cfg.Methods) == 0 {
		return false
	}
	for _, m := range i.cfg.Methods {
		if strings.Contains(fullMethod, m) {
			return false
		}
	}
	return true
}

// sampleLatency draws a latency from the configured normal distribution, clamped to
// [0, MaxMs].
func (i *Injector) sampleLatency() time.Duration {
	i.rndMu.Lock()
	ms := i.rnd.NormFloat64()*i.cfg.Latency.StddevMs + i.cfg.Latency.MeanMs
	i.rndMu.Unlock()
	if ms < 0 {
		ms = 0
	}
	if i.cfg.Latency.MaxMs > 0 && ms > i.cfg.Latency.MaxMs {
		ms = i.cfg.Latency.MaxMs
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// inject applies a fault to the call. Currently only the Latency fault is supported.
// It returns the context error if the context is cancelled while waiting, which aborts
// the RPC just as a deadline would under real contention.
func (i *Injector) inject(ctx context.Context) error {
	d := i.sampleLatency()
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Delay is the transport-agnostic fault primitive: if the call identified by fullMethod
// is selected for a fault, it blocks for the sampled latency (or until ctx is done) and
// returns the context error on cancellation. It is used by the interceptors and can also
// wrap typed clients directly in tests.
func (i *Injector) Delay(ctx context.Context, fullMethod string) error {
	if !i.shouldFault(fullMethod) {
		return nil
	}
	return i.inject(ctx)
}

// maybeError returns a transient, retryable error for the call with probability ErrorRate,
// or nil. The error code is chosen from transientErrorCodes. These codes are retried by the
// internal clients, so the fault is recoverable: a test should still pass under it.
func (i *Injector) maybeError(fullMethod string) error {
	if i.cfg.ErrorRate <= 0 || i.excluded(fullMethod) {
		return nil
	}
	i.rndMu.Lock()
	roll := i.rnd.Float64()
	code := transientErrorCodes[i.rnd.Intn(len(transientErrorCodes))]
	i.rndMu.Unlock()
	if roll < i.cfg.ErrorRate {
		return status.Errorf(code, "faultinjection: injected transient %s", code)
	}
	return nil
}

// apply runs both faults for a call: optional latency, then an optional transient error.
func (i *Injector) apply(ctx context.Context, fullMethod string) error {
	if err := i.Delay(ctx, fullMethod); err != nil {
		return err
	}
	return i.maybeError(fullMethod)
}

// UnaryServerInterceptor returns a gRPC unary server interceptor that injects faults
// before invoking the handler.
func (i *Injector) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if err := i.apply(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream server interceptor that injects faults
// before invoking the handler.
func (i *Injector) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if err := i.apply(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// UnaryClientInterceptor returns a gRPC unary client interceptor that injects faults
// before invoking the call. It is the client-side counterpart to UnaryServerInterceptor.
func (i *Injector) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if err := i.apply(ctx, method); err != nil {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
