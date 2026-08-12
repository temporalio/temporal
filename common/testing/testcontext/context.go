package testcontext

import (
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
	// maxTimeout caps the *total* lifetime of a test context that uses the
	// default timeout, measured from its creation - it is not a per-extension
	// budget. [EnsureRemaining] may extend such a context until
	// createdAt+maxTimeout and no further.
	//
	// An explicitly configured timeout ([WithTimeout] or TEMPORAL_TEST_TIMEOUT)
	// is its own ceiling and is never extended beyond it.
	maxTimeout          = 2 * time.Minute
	testNameMetadataKey = "temporal-test-name"
	testTimeoutEnvVar   = "TEMPORAL_TEST_TIMEOUT"

	notDerivedMessage = "testcontext: context is not derived from this test's context; " +
		"pass the test context or a context derived from it"
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

// config records options for creating a test context.
type config struct {
	// timeout is the explicitly requested timeout, or zero to use the default.
	timeout time.Duration
}

// contextDecorator records a keyed transformation to replay on replacement contexts.
type contextDecorator struct {
	key      any
	decorate func(context.Context) context.Context
}

// ownerKey marks a context as belonging to a test's context chain. Context
// values are inherited, so any context derived from a test context - including
// an outdated one - carries the mark too.
type ownerKey struct{}

// GoTestDeadline returns the deadline imposed by `go test -timeout`, if any.
//
// It is a hard external cap for any test-scoped deadline: crossing it panics
// the whole test binary - taking every sibling test with it - instead of
// failing just this test.
//
// NOTE: tb.Context() does NOT carry this deadline; only [testing.T.Deadline] has it.
func GoTestDeadline(tb testing.TB) (deadline time.Time, ok bool) {
	// testing.TB does not expose Deadline, but *testing.T and *testing.B do.
	d, hasDeadline := tb.(interface{ Deadline() (time.Time, bool) })
	if !hasDeadline {
		return time.Time{}, false
	}
	// Inside a synctest bubble the clock is fake and Deadline panics; a
	// real-clock deadline would be meaningless there anyway.
	defer func() { _ = recover() }()
	return d.Deadline()
}

// newTestContext creates a context for st that expires at deadline, capped by
// the `go test -timeout` deadline.
func newTestContext(tb testing.TB, st *contextState, deadline time.Time) (context.Context, context.CancelFunc) {
	if goTestDeadline, ok := GoTestDeadline(tb); ok {
		deadline = util.MinTime(deadline, goTestDeadline)
	}

	ctx, cancel := context.WithDeadline(tb.Context(), deadline)
	ctx = context.WithValue(ctx, ownerKey{}, st)

	// Annotate gRPC requests with the test name for OTEL tracing.
	ctx = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())

	// Apply context decorators, in order.
	for _, decorator := range st.decorators {
		ctx = decorator.decorate(ctx)
	}

	return ctx, cancel
}

// DefaultTimeout returns the effective default timeout for test contexts.
func DefaultTimeout() time.Duration {
	timeout, _ := effectiveTimeout(0)
	return timeout
}

// GetOrCreate returns the test context for tb. The context is canceled
// when the test ends or when the configured test timeout expires.
//
// The first call creates the test context and fixes its timeout. Later calls
// return the current context, but an explicit different timeout fails instead
// of being silently ignored.
func GetOrCreate(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()

	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getOrCreateContextState(tb, cfg)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.current
}

// GetOrDefault returns the current test context for tb, or tb.Context if none exists.
func GetOrDefault(tb testing.TB) context.Context {
	tb.Helper()

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return tb.Context()
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	return st.current
}

// Option configures the test context returned by [GetOrCreate].
type Option func(*config)

// WithTimeout sets a custom timeout for the test context. An explicitly
// requested timeout is also the ceiling for [EnsureRemaining].
func WithTimeout(timeout time.Duration) Option {
	return func(cfg *config) {
		if timeout <= 0 {
			return
		}
		cfg.timeout = timeout
	}
}

// AttachDecorator applies decorator to the test context once for key.
// Reusing the same key is a no-op. If the test context does not exist yet,
// AttachDecorator creates it with the default timeout. Call [GetOrCreate] with [WithTimeout]
// first when using a custom timeout.
func AttachDecorator[K comparable](tb testing.TB, key K, decorator func(context.Context) context.Context) {
	tb.Helper()

	if any(key) == nil {
		tb.Fatal("testcontext: context decorator key must not be nil")
		return
	}
	if decorator == nil {
		tb.Fatal("testcontext: context decorator must not be nil")
		return
	}

	st := getOrCreateContextState(tb, config{})

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	st.mu.Lock()
	defer st.mu.Unlock()

	for _, existing := range st.decorators {
		if existing.key == key {
			return
		}
	}
	next := contextDecorator{
		key:      key,
		decorate: decorator,
	}
	st.current = next.decorate(st.current)
	st.decorators = append(st.decorators, next)
}

// EnsureRemaining extends the test context so at least minRemaining
// remains from now.
//
// A context created with an explicit timeout ([WithTimeout] or
// TEMPORAL_TEST_TIMEOUT) is never extended past that timeout; one using the
// default timeout may grow to a total lifetime of 2 minutes.
//
// ctx must be the test context or derived from it (possibly from an outdated
// one, if the context was extended before); EnsureRemaining then returns the
// current test context. Contexts belonging to another test - or to no test at
// all - fail the test. If tb has no test context, ctx is returned unchanged.
//
// tb.Context() is accepted and returned unchanged: it is the test context's own
// parent, so there is nothing to extend, but it is not a foreign context either.
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

	if !st.owns(ctx) {
		if ctx == tb.Context() {
			// The testing context has no deadline to extend, and it is what
			// [GetOrDefault] hands out before a test context exists.
			return ctx
		}
		tb.Fatal(notDerivedMessage)
		return ctx
	}

	testDeadline, ok := st.current.Deadline()
	if !ok {
		tb.Fatal("testcontext: current context has no deadline")
		return ctx
	}

	// Cap the requested deadline at the context's ceiling.
	requestedDeadline := util.MinTime(time.Now().Add(minRemaining), st.maxDeadline())

	// Extend the test context if the requested deadline is after the current deadline.
	if requestedDeadline.After(testDeadline) {
		st.push(newTestContext(tb, st, requestedDeadline))
	}

	return st.current
}

// contextState is the mutable test context state shared by test helpers.
type contextState struct {
	createdAt time.Time
	// timeout is the timeout the context was created with; immutable.
	timeout time.Duration
	// explicitTimeout records whether timeout was requested explicitly (via
	// [WithTimeout] or TEMPORAL_TEST_TIMEOUT) rather than defaulted; immutable.
	explicitTimeout bool

	mu sync.Mutex
	// current is the newest context; [EnsureRemaining] replaces it when the
	// deadline is extended. Never nil, so late callers see a canceled context
	// instead of a panic.
	current context.Context
	// cancels tracks every context created for this test so cleanup can release them all.
	cancels    []context.CancelFunc
	decorators []contextDecorator
}

func newContextState(tb testing.TB, timeout time.Duration, explicitTimeout bool) *contextState {
	st := &contextState{
		createdAt:       time.Now(),
		timeout:         timeout,
		explicitTimeout: explicitTimeout,
	}
	st.push(newTestContext(tb, st, st.createdAt.Add(timeout)))
	return st
}

func getOrCreateContextState(tb testing.TB, cfg config) *contextState {
	tb.Helper()

	timeout, explicitTimeout := effectiveTimeout(cfg.timeout)

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	if !ok {
		st = newContextState(tb, timeout, explicitTimeout)
		testContexts.byTest[tb] = st

		tb.Cleanup(func() {
			// Deregister first: a concurrent helper must not find state that is
			// about to be torn down.
			testContexts.Lock()
			delete(testContexts.byTest, tb)
			testContexts.Unlock()

			if timedOut, timeout := st.cleanup(); timedOut {
				tb.Errorf("test exceeded timeout of %v", timeout)
			}
		})
	}
	testContexts.Unlock()

	// A freshly created context adopts the requested timeout, so only an
	// existing one can conflict with an explicitly requested timeout.
	if ok && cfg.timeout != 0 && timeout != st.timeout {
		tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", st.timeout, timeout)
	}
	return st
}

// owns reports whether ctx is one of this test's contexts, or derived from one.
func (s *contextState) owns(ctx context.Context) bool {
	return ctx.Value(ownerKey{}) == s
}

// maxDeadline is the furthest deadline [EnsureRemaining] may extend to.
func (s *contextState) maxDeadline() time.Time {
	limit := s.timeout
	if !s.explicitTimeout {
		// Only a defaulted timeout may grow; see [maxTimeout].
		limit = max(limit, maxTimeout*debug.TimeoutMultiplier)
	}
	return s.createdAt.Add(limit)
}

func (s *contextState) push(ctx context.Context, cancel context.CancelFunc) {
	s.current = ctx
	s.cancels = append(s.cancels, cancel)
}

func (s *contextState) cleanup() (timedOut bool, timeout time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timedOut = s.current.Err() == context.DeadlineExceeded
	timeout = s.timeout

	if deadline, ok := s.current.Deadline(); ok {
		timeout = deadline.Sub(s.createdAt)
	}

	for _, cancel := range slices.Backward(s.cancels) {
		cancel()
	}

	// Keep current: it is canceled now, but callers still racing with cleanup
	// must get a context, not a panic. Clearing cancels makes cleanup idempotent.
	s.cancels = nil
	s.decorators = nil
	return timedOut, timeout
}

// effectiveTimeout resolves the timeout to use and reports whether it was
// explicitly configured (as opposed to defaulted).
func effectiveTimeout(customTimeout time.Duration) (timeout time.Duration, explicit bool) {
	defer func() {
		// Build flag TEMPORAL_DEBUG applies a timeout multiplier to all test timeouts.
		timeout *= debug.TimeoutMultiplier
	}()

	// 1. Custom timeout (via WithTimeout option).
	if customTimeout > 0 {
		return customTimeout, true
	}

	// 2. TEMPORAL_TEST_TIMEOUT environment variable.
	if envTimeout := os.Getenv(testTimeoutEnvVar); envTimeout != "" {
		if dur, err := time.ParseDuration(envTimeout); err == nil && dur > 0 {
			return dur, true
		}
	}

	// 3. Default timeout.
	return defaultTimeout, false
}
