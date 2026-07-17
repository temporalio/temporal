package testcontext

import (
	"cmp"
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc/metadata"
)

const (
	defaultTimeout = 90 * time.Second
	// maxTimeout caps how far EnsureRemaining can extend a test context.
	maxTimeout          = 2 * time.Minute
	testNameMetadataKey = "temporal-test-name"
)

// contextStore tracks one context state per test.
type contextStore struct {
	sync.Mutex
	byTest map[testing.TB]*contextState
}

// testContexts is process-global so repeated helpers in the same test share
// one context and one cleanup.
var testContexts = contextStore{
	byTest: make(map[testing.TB]*contextState),
}

// config records options for creating a [testContext].
type config struct {
	// timeout is the explicitly requested timeout, or zero to use the default.
	timeout time.Duration
}

// contextDecorator records a keyed transformation to replay on replacement contexts.
type contextDecorator struct {
	key      any
	decorate func(context.Context) context.Context
}

// testContext bundles a context with its cancel function and applied deadline.
type testContext struct {
	ctx      context.Context
	cancel   context.CancelFunc
	deadline time.Time
}

func newTestContext(tb testing.TB, deadline time.Time, decorators []contextDecorator) testContext {
	if goTestDeadline, ok := tb.Context().Deadline(); ok {
		// The Go test deadline is a hard external cap.
		deadline = util.MinTime(deadline, goTestDeadline)
	}

	ctx, cancel := context.WithDeadline(tb.Context(), deadline)

	// Annotate gRPC requests with the test name for OTEL tracing.
	ctx = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())

	// Apply context decorators, in order.
	for _, decorator := range decorators {
		ctx = decorator.decorate(ctx)
	}

	return testContext{
		ctx:      ctx,
		cancel:   cancel,
		deadline: deadline,
	}
}

// DefaultTimeout returns the effective default timeout for [testContext]s.
func DefaultTimeout() time.Duration {
	return effectiveTimeout(0)
}

// For returns the [testContext] for tb. The context is canceled
// when the test ends or when the configured test timeout expires.
//
// The first call creates the per-test context and fixes its timeout. Later calls
// return the same context, but an explicit different timeout fails instead of
// being silently ignored.
func For(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()

	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getOrCreateContextState(tb, cfg)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.currentContext()
}

// Get returns the current [testContext] for tb, if one exists.
func Get(tb testing.TB) (context.Context, bool) {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return nil, false
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	return st.currentContext(), true
}

// Option configures the [testContext] returned by [For].
type Option func(*config)

// WithTimeout sets a custom timeout for the [testContext].
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout <= 0 {
			return
		}
		cfg.timeout = effectiveTimeout(timeout)
	}
}

// AttachDecorator applies decorator to the [testContext] once for key.
// Reusing the same key is a no-op. If the test context does not exist yet,
// AttachDecorator creates it with the default timeout. Call [For] with [WithTimeout]
// first when using a custom timeout.
func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()

	st := getOrCreateContextState(tb, config{})

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	st.mu.Lock()
	defer st.mu.Unlock()

	if any(key) == nil {
		tb.Fatal("testcontext: context decorator key must not be nil")
	}
	if decorator == nil {
		tb.Fatal("testcontext: context decorator must not be nil")
	}
	for _, existing := range st.decorators {
		if existing.key == key {
			return
		}
	}
	next := contextDecorator{
		key:      key,
		decorate: decorator,
	}
	st.contextStack = append(st.contextStack, next.decorate(st.currentContext()))
	st.decorators = append(st.decorators, next)
}

// EnsureRemaining extends the [testContext] so at least minRemaining
// remains from now. Capped at [maxTimeout].
//
// If ctx is one of this test's contexts, EnsureRemaining returns the current
// [testContext]. Otherwise, it fails because ctx is not derived from the
// test context chain.
func EnsureRemaining(ctx context.Context, tb testing.TB, minRemaining time.Duration) context.Context {
	tb.Helper()
	if ctx == nil {
		tb.Fatal("testcontext: nil context")
		return nil
	}
	if minRemaining <= 0 {
		tb.Fatalf("testcontext: min remaining must be positive: %v", minRemaining)
		return ctx
	}

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return ctx
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	testDeadline, ok := st.currentContext().Deadline()
	if !ok {
		tb.Fatal("testcontext: current context has no deadline")
		return ctx
	}
	if !slices.Contains(st.contextStack, ctx) {
		tb.Fatalf("testcontext: context is not derived from this test's context; are you using context.Background()?")
		return ctx
	}

	// Cap the requested deadline to the max deadline.
	maxDeadline := st.createdAt.Add(max(effectiveTimeout(maxTimeout), st.timeout))
	requestedDeadline := util.MinTime(time.Now().Add(minRemaining), maxDeadline)

	// Extend the test context if the requested deadline is after the current deadline.
	if requestedDeadline.After(testDeadline) {
		next := newTestContext(tb, requestedDeadline, st.decorators)
		st.pushContext(next)
	}

	return st.currentContext()
}

// contextState is the mutable per-test context state shared by test helpers.
type contextState struct {
	createdAt time.Time
	timeout   time.Duration

	mu sync.Mutex
	// contextStack lets EnsureRemaining recognize stale test contexts after
	// deadline resets. The last entry is the current context.
	contextStack []context.Context
	// cancelStack tracks every replacement context so cleanup can release them all.
	cancelStack []context.CancelFunc
	decorators  []contextDecorator
}

func newContextState(tb testing.TB, timeout time.Duration) *contextState {
	st := &contextState{
		createdAt: time.Now(),
		timeout:   timeout,
	}
	st.pushContext(newTestContext(tb, st.createdAt.Add(timeout), st.decorators))
	return st
}

func getOrCreateContextState(tb testing.TB, cfg config) *contextState {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	if !ok {
		st = newContextState(tb, cmp.Or(cfg.timeout, DefaultTimeout()))
		testContexts.byTest[tb] = st

		tb.Cleanup(func() {
			timedOut, timeout := st.cleanup()
			testContexts.Lock()
			delete(testContexts.byTest, tb)
			testContexts.Unlock()
			if timedOut {
				tb.Errorf("test exceeded timeout of %v", timeout)
			}
		})
	}
	testContexts.Unlock()

	// A freshly created context adopts the requested timeout, so only an
	// existing one can conflict with an explicitly requested timeout.
	if ok && cfg.timeout != 0 {
		st.mu.Lock()
		configured := st.timeout
		st.mu.Unlock()
		if cfg.timeout != configured {
			tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", configured, cfg.timeout)
		}
	}
	return st
}

func (s *contextState) currentContext() context.Context {
	return s.contextStack[len(s.contextStack)-1]
}

func (s *contextState) pushContext(ctx testContext) {
	s.contextStack = append(s.contextStack, ctx.ctx)
	s.cancelStack = append(s.cancelStack, ctx.cancel)
}

func (s *contextState) cleanup() (timedOut bool, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timedOut = s.currentContext().Err() == context.DeadlineExceeded
	timeout = s.timeout

	if deadline, ok := s.currentContext().Deadline(); ok {
		timeout = deadline.Sub(s.createdAt)
	}

	for _, cancel := range slices.Backward(s.cancelStack) {
		cancel()
	}

	s.contextStack = nil
	s.cancelStack = nil
	s.decorators = nil
	return timedOut, timeout
}

func effectiveTimeout(customTimeout time.Duration) (timeout time.Duration) {
	defer func() {
		// Build flag TEMPORAL_DEBUG applies a timeout multiplier to all test timeouts.
		timeout *= debug.TimeoutMultiplier
	}()

	// 1. Custom timeout (via WithTimeout option).
	if customTimeout > 0 {
		return customTimeout
	}

	// 2. TEMPORAL_TEST_TIMEOUT environment variable.
	if envTimeout := os.Getenv("TEMPORAL_TEST_TIMEOUT"); envTimeout != "" {
		if dur, err := time.ParseDuration(envTimeout); err == nil && dur > 0 {
			return dur
		}
	}

	// 3. Default timeout.
	return defaultTimeout
}
