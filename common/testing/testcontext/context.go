package testcontext

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc/metadata"
)

const (
	defaultTimeout = 90 * time.Second
	// maxTimeout caps how far EnsureRemaining can extend a test context.
	maxTimeout          = 2 * time.Minute
	testNameMetadataKey = "temporal-test-name"
)

type contextStore struct {
	sync.Mutex
	byTest map[testing.TB]*contextState
}

// testContexts is process-global so repeated helpers in the same test share
// one context and one cleanup.
var testContexts = contextStore{
	byTest: make(map[testing.TB]*contextState),
}

type config struct {
	timeout    time.Duration
	timeoutSet bool
}

type contextDecorator struct {
	key      any
	decorate func(context.Context) context.Context
}

// DefaultTimeout returns the effective default timeout for test-scoped contexts.
func DefaultTimeout() time.Duration {
	return effectiveTimeout(0)
}

// For returns the test-scoped context for tb. The context is canceled
// when the test ends or when the configured test timeout expires.
//
// The first call creates the per-test context and fixes its timeout. Later calls
// return the same context, but an explicit different timeout fails instead of
// being silently ignored.
func For(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()

	cfg := config{timeout: DefaultTimeout()}
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getContextState(tb, cfg.timeout)
	st.configure(tb, cfg)
	return st.context()
}

// Option configures the test-scoped context returned by [For].
type Option func(*config)

// WithTimeout sets a custom timeout for the test-scoped context.
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout <= 0 {
			return
		}
		cfg.timeout = effectiveTimeout(timeout)
		cfg.timeoutSet = true
	}
}

// AttachDecorator applies decorator to the test-scoped context once for key.
// Reusing the same key is a no-op. If the test context does not exist yet,
// AttachDecorator creates it with the default timeout. Call [For] with [WithTimeout]
// first when using a custom timeout.
func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()

	st := getContextState(tb, DefaultTimeout())
	st.attachDecorator(tb, contextDecorator{
		key:      key,
		decorate: decorator,
	})
}

type contextState struct {
	mu                sync.Mutex
	ctx               context.Context
	ownedContexts     []context.Context
	cancels           []context.CancelFunc
	testStart         time.Time
	configuredTimeout time.Duration
	decorators        map[any]struct{}
	orderedDecorators []contextDecorator
}

func getContextState(tb testing.TB, timeout time.Duration) *contextState {
	tb.Helper()

	testContexts.Lock()
	defer testContexts.Unlock()

	if st, ok := testContexts.byTest[tb]; ok {
		return st
	}

	st := &contextState{
		testStart:         time.Now(),
		configuredTimeout: timeout,
		decorators:        make(map[any]struct{}),
	}
	st.setDeadline(tb, st.testStart.Add(timeout))
	testContexts.byTest[tb] = st

	tb.Cleanup(func() {
		err := st.err()
		st.cancel()
		testContexts.Lock()
		delete(testContexts.byTest, tb)
		testContexts.Unlock()
		if err == context.DeadlineExceeded {
			tb.Errorf("test exceeded timeout of %v", st.currentTimeout())
		}
		st.release()
	})
	return st
}

// EnsureRemaining extends the test-scoped context so at least d remains from
// now, if its current deadline is earlier. It is capped so the total test
// timeout never exceeds max(maxTestTimeout, configuredTimeout). If tb has no
// test context created by this package, EnsureRemaining is a no-op.
//
// If ctx is one of this test's contexts, EnsureRemaining returns the current
// test-scoped context. Otherwise, it caps ctx at the test-scoped context
// deadline when needed.
func EnsureRemaining(tb testing.TB, ctx context.Context, d time.Duration) context.Context {
	tb.Helper()
	if d <= 0 {
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

	currentDeadline, ok := st.ctx.Deadline()
	if !ok {
		return ctx
	}

	target := time.Now().Add(d)

	// Preserve longer configured timeouts; otherwise bound extensions.
	effectiveMaxTimeout := maxTimeout * debug.TimeoutMultiplier
	capDeadline := st.testStart.Add(max(effectiveMaxTimeout, st.configuredTimeout))
	if capDeadline.Before(target) {
		target = capDeadline
	}
	if target.After(currentDeadline) {
		// Context deadlines are immutable; replace and replay decorators.
		currentDeadline = st.setDeadline(tb, target)
	}

	return st.capContextDeadline(tb, ctx, currentDeadline)
}

func (s *contextState) capContextDeadline(
	tb testing.TB,
	ctx context.Context,
	deadline time.Time,
) context.Context {
	tb.Helper()
	if ctx == nil {
		return ctx
	}
	if slices.Contains(s.ownedContexts, ctx) {
		return s.ctx
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && !deadline.Before(ctxDeadline) {
		return ctx
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	tb.Cleanup(cancel)
	return ctx
}

func (s *contextState) configure(tb testing.TB, cfg config) {
	tb.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.timeoutSet && cfg.timeout != s.configuredTimeout {
		tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", s.configuredTimeout, cfg.timeout)
	}
}

func (s *contextState) attachDecorator(tb testing.TB, decorator contextDecorator) {
	tb.Helper()

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	s.mu.Lock()
	defer s.mu.Unlock()

	if decorator.key == nil {
		tb.Fatal("testcontext: context decorator key must not be nil")
	}
	if decorator.decorate == nil {
		tb.Fatal("testcontext: context decorator must not be nil")
	}
	if _, ok := s.decorators[decorator.key]; ok {
		return
	}
	s.ctx = decorator.decorate(s.ctx)
	s.ownedContexts = append(s.ownedContexts, s.ctx)
	s.decorators[decorator.key] = struct{}{}
	s.orderedDecorators = append(s.orderedDecorators, decorator)
}

func (s *contextState) context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx
}

func (s *contextState) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx.Err()
}

func (s *contextState) setDeadline(tb testing.TB, deadline time.Time) time.Time {
	if goTestDeadline, ok := tb.Context().Deadline(); ok && goTestDeadline.Before(deadline) {
		// The Go test deadline is a hard external cap.
		deadline = goTestDeadline
	}

	ctx, cancel := context.WithDeadline(tb.Context(), deadline)

	// Annotate gRPC requests with the test name for OTEL tracing.
	ctx = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())

	for _, decorator := range s.orderedDecorators {
		ctx = decorator.decorate(ctx)
	}
	s.ctx = ctx
	s.ownedContexts = append(s.ownedContexts, ctx)
	s.cancels = append(s.cancels, cancel)
	return deadline
}

func (s *contextState) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.cancels) - 1; i >= 0; i-- {
		s.cancels[i]()
	}
}

func (s *contextState) currentTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if deadline, ok := s.ctx.Deadline(); ok {
		return deadline.Sub(s.testStart)
	}
	return s.configuredTimeout
}

func (s *contextState) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = nil
	s.ownedContexts = nil
	s.cancels = nil
	s.decorators = nil
	s.orderedDecorators = nil
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
