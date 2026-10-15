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
	// default or TEMPORAL_TEST_TIMEOUT-configured timeout, measured from its
	// creation - it is not a per-extension budget. [EnsureRemaining] may
	// extend such a context until createdAt+max(timeout, maxTimeout) and no
	// further.
	//
	// A timeout requested via [WithTimeout] is its own ceiling and is never
	// extended beyond it.
	maxTimeout          = 2 * time.Minute
	testNameMetadataKey = "temporal-test-name"
	testTimeoutEnvVar   = "TEMPORAL_TEST_TIMEOUT"
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

// testContext wraps every context this package hands out, so [EnsureRemaining]
// can tell one apart from a context the caller derived further (adding its own
// deadline, cancellation, or values), which is not safe to replace.
type testContext struct {
	context.Context
}

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

	return &testContext{Context: ctx}, cancel
}

// DefaultTimeout returns the effective default timeout for test contexts.
func DefaultTimeout() time.Duration {
	timeout, _ := effectiveTimeout(0)
	return timeout
}

// For returns the test context for tb. The context is canceled when the test
// ends or when the configured test timeout expires.
//
// The first call creates the test context and fixes its timeout. Later calls
// return the current context, but an explicit different timeout fails instead
// of being silently ignored.
//
// The result is not worth caching: [EnsureRemaining] replaces the context when
// it extends the deadline, so a stored copy keeps the old, shorter one.
func For(tb testing.TB, opts ...Option) context.Context {
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

// Option configures the test context returned by [For].
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
// AttachDecorator creates it with the default timeout. Call [For] with [WithTimeout]
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
	st.current = &testContext{Context: next.decorate(st.current)}
	st.decorators = append(st.decorators, next)
}

// EnsureRemaining extends the test context so at least minRemaining remains
// from now, and returns the context to use.
//
// A context created with [WithTimeout] is never extended past that timeout;
// one using the default or a TEMPORAL_TEST_TIMEOUT-configured timeout may
// grow to a total lifetime of max(timeout, [maxTimeout]).
//
// Only the returned context - and later [For] calls - see the extension: a
// context captured earlier keeps its original, shorter deadline, since a
// context's effective deadline can only shrink as it is derived further, never
// grow.
//
// The context to extend is resolved from ctx, not tb, so a subtest's tb can
// extend a context owned by its parent test - the dominant pattern in tests/,
// e.g. await.Require(s.Context(), t, ...) inside a t.Run started from suite
// method s.
//
// ctx comes back unchanged if it is not a test context at all (extension is an
// optimization, so a foreign context is left alone rather than failing the
// caller), or if the caller derived it further - e.g.
// context.WithTimeout(env.Context(), ...) - because swapping it out would
// silently discard that wrapping. The underlying test context is still
// extended for later callers.
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

	st, _ := ctx.Value(ownerKey{}).(*contextState)
	if st == nil {
		// ctx isn't derived from any test context - e.g. one built directly
		// from context.Background(), or tb.Context() itself - so there is
		// nothing to extend.
		return ctx
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	testDeadline, ok := st.current.Deadline()
	if !ok {
		tb.Fatal("testcontext: current context has no deadline")
		return ctx
	}

	// Cap the requested deadline at the context's ceiling.
	requestedDeadline := util.MinTime(time.Now().Add(minRemaining), st.maxDeadline())

	// Only a context this package handed out can be swapped for the extended
	// one without dropping state the caller derived onto it.
	_, bare := ctx.(*testContext)

	// Extend the test context if the requested deadline is after the current deadline.
	if requestedDeadline.After(testDeadline) {
		st.push(newTestContext(st.owner, st, requestedDeadline))
	}

	if bare {
		return st.current
	}
	return ctx
}

// contextState is the mutable test context state shared by test helpers.
type contextState struct {
	// owner is the testing.TB this state was created for; immutable. Used to
	// derive replacement contexts even when [EnsureRemaining] is reached
	// through a different tb (e.g. a subtest given a parent's context).
	owner     testing.TB
	createdAt time.Time
	// timeout is the timeout the context was created with; immutable.
	timeout time.Duration
	// explicitTimeout records whether timeout was pinned via [WithTimeout],
	// making it a hard ceiling. TEMPORAL_TEST_TIMEOUT only raises the
	// baseline and remains extendable, like the default. Immutable.
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
		owner:           tb,
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

// maxDeadline is the furthest deadline [EnsureRemaining] may extend to.
func (s *contextState) maxDeadline() time.Time {
	limit := s.timeout
	if !s.explicitTimeout {
		// A defaulted or TEMPORAL_TEST_TIMEOUT-configured timeout may grow,
		// up to whichever of the two is larger; see [maxTimeout].
		limit = max(limit, maxTimeout*debug.TimeoutMultiplier)
	}
	return s.createdAt.Add(limit)
}

func (s *contextState) push(ctx context.Context, cancel context.CancelFunc) {
	s.current = ctx
	s.cancels = append(s.cancels, cancel)
}

// cleanup cancels every context created for the test and reports whether the
// test's context deadline had already fired, and how long after createdAt that was.
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

	// 2. TEMPORAL_TEST_TIMEOUT environment variable. Like the default, this
	// only raises the baseline - it does not pin a hard ceiling.
	if envTimeout := os.Getenv(testTimeoutEnvVar); envTimeout != "" {
		if dur, err := time.ParseDuration(envTimeout); err == nil && dur > 0 {
			return dur, false
		}
	}

	// 3. Default timeout.
	return defaultTimeout, false
}
