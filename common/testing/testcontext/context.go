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

	st := getContextState(tb, cfg)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.ctx
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

	st := getContextState(tb, config{timeout: DefaultTimeout()})
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

func getContextState(tb testing.TB, cfg config) *contextState {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	if !ok {
		st = &contextState{
			testStart:         time.Now(),
			configuredTimeout: cfg.timeout,
			decorators:        make(map[any]struct{}),
		}
		st.resetContextDeadline(tb, st.testStart.Add(cfg.timeout))
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

	// A freshly created context adopts cfg.timeout, so only an existing one can
	// conflict with an explicitly requested timeout.
	if ok && cfg.timeoutSet {
		st.mu.Lock()
		configured := st.configuredTimeout
		st.mu.Unlock()
		if cfg.timeout != configured {
			tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", configured, cfg.timeout)
		}
	}
	return st
}

// EnsureRemaining extends the test-scoped context so at least d remains from
// now, if its current deadline is earlier. It is capped so the total test
// timeout never exceeds max(maxTimeout, configuredTimeout). If tb has no
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

	testDeadline, ok := st.ctx.Deadline()
	if !ok {
		return ctx
	}

	requestedDeadline := time.Now().Add(d)

	// Preserve longer configured timeouts; otherwise bound extensions.
	effectiveMaxTimeout := maxTimeout * debug.TimeoutMultiplier
	maxDeadline := st.testStart.Add(max(effectiveMaxTimeout, st.configuredTimeout))
	if maxDeadline.Before(requestedDeadline) {
		requestedDeadline = maxDeadline
	}
	if requestedDeadline.After(testDeadline) {
		// Context deadlines are immutable; replace and replay decorators.
		testDeadline = st.resetContextDeadline(tb, requestedDeadline)
	}

	if ctx == nil {
		return ctx
	}
	if slices.Contains(st.ownedContexts, ctx) {
		return st.ctx
	}
	if ctxDeadline, ok := ctx.Deadline(); ok && !testDeadline.Before(ctxDeadline) {
		return ctx
	}
	ctx, cancel := context.WithDeadline(ctx, testDeadline)
	tb.Cleanup(cancel)
	return ctx
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

func (s *contextState) resetContextDeadline(tb testing.TB, deadline time.Time) time.Time {
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

func (s *contextState) cleanup() (timedOut bool, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timedOut = s.ctx.Err() == context.DeadlineExceeded
	timeout = s.configuredTimeout

	if deadline, ok := s.ctx.Deadline(); ok {
		timeout = deadline.Sub(s.testStart)
	}
	for _, cancel := range slices.Backward(s.cancels) {
		cancel()
	}

	s.ctx = nil
	s.ownedContexts = nil
	s.cancels = nil
	s.decorators = nil
	s.orderedDecorators = nil
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
