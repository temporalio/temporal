package testcontext

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
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

	extensionLimitTestContextCap  = "test context extension cap"
	extensionLimitGoTestTimeout   = "go test timeout"
	extensionLimitExplicitTimeout = "explicit test timeout"

	// Keep extension audit output useful even when many helpers request extensions.
	reportHeadExtensions = 1
	reportTailExtensions = 3
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

// ownerKey marks a context as belonging to a test's context chain. Context
// values are inherited, so any context derived from a test context carries the
// mark too.
type ownerKey struct{}

type extensionGrant struct {
	duration time.Duration
	elapsed  time.Duration
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
// After decorators are attached, the result may be cached: [EnsureRemaining]
// extends its active timeout without changing the context or its reported
// deadline. Deadline reports the latest possible expiration. Done closes at
// the current active expiration, initially the configured timeout.
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

	if slices.Contains(st.decoratorKeys, any(key)) {
		return
	}
	st.current = decorator(st.current)
	st.decoratorKeys = append(st.decoratorKeys, key)
}

// EnsureRemaining extends the test context so at least minRemaining remains
// from now.
//
// A context created with [WithTimeout] is never extended past that timeout;
// one using the default or a TEMPORAL_TEST_TIMEOUT-configured timeout may
// grow to a total lifetime of max(timeout, [maxTimeout]). A context that has
// already expired is not revived.
//
// The context to extend is resolved from ctx, not tb, so a subtest's tb can
// extend a context owned by its parent test - the dominant pattern in tests/,
// e.g. await.Require(s.Context(), t, ...) inside a t.Run started from suite
// method s.
//
// If ctx is not a test context at all, extension is an optimization, so a
// foreign context is left alone rather than failing the caller. If the caller
// derived it further - e.g. context.WithTimeout(env.Context(), ...) - its own
// tighter deadline remains intact while the underlying test context is
// extended.
func EnsureRemaining(ctx context.Context, tb testing.TB, minRemaining time.Duration) {
	tb.Helper()
	if ctx == nil {
		tb.Fatal("testcontext: nil context")
		return
	}
	if minRemaining <= 0 {
		tb.Fatalf("testcontext: min remaining must be positive: %v", minRemaining)
		return
	}

	st, _ := ctx.Value(ownerKey{}).(*contextState)
	if st == nil {
		// ctx isn't derived from any test context - e.g. one built directly
		// from context.Background(), or tb.Context() itself - so there is
		// nothing to extend.
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	activeExpiration := st.timeoutContext.effectiveExpiration()
	requestedDeadline := time.Now().Add(minRemaining)
	if !requestedDeadline.After(activeExpiration) {
		return
	}

	// Cap the requested deadline at the context's ceiling.
	limit := ""
	ceiling, _ := st.timeoutContext.Deadline()
	if ceiling.Before(requestedDeadline) {
		requestedDeadline = ceiling
		limit = st.extensionCeilingLimit
	}

	st.timeoutContext.extend(requestedDeadline)
	extendedExpiration := st.timeoutContext.effectiveExpiration()
	if extendedExpiration.After(activeExpiration) {
		st.extensionGrants = append(st.extensionGrants, extensionGrant{
			duration: extendedExpiration.Sub(activeExpiration),
			elapsed:  time.Since(st.createdAt),
		})
		st.extensionLimit = limit
	} else {
		st.extensionDenied++
		st.extensionLimit = limit
	}
}

// ExtensionAudit returns the extension history for the test context that owns ctx.
func ExtensionAudit(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	st, _ := ctx.Value(ownerKey{}).(*contextState)
	if st == nil {
		return ""
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	return st.extensionAuditLocked()
}

// contextState is the mutable test context state shared by test helpers.
type contextState struct {
	createdAt time.Time
	// timeout is the timeout the context was created with; immutable.
	timeout        time.Duration
	timeoutContext *timeoutContext

	mu sync.Mutex
	// current is the context with every decorator attached. Never nil, so late
	// callers see a canceled context instead of a panic.
	current               context.Context
	decoratorKeys         []any
	extensionGrants       []extensionGrant
	extensionDenied       int
	extensionLimit        string
	extensionCeilingLimit string
}

func newContextState(tb testing.TB, timeout time.Duration, explicitTimeout bool) *contextState {
	createdAt := time.Now()
	activeExpiration := createdAt.Add(timeout)
	limit := timeout
	if !explicitTimeout {
		// A defaulted or TEMPORAL_TEST_TIMEOUT-configured timeout may grow,
		// up to whichever of the two is larger; see [maxTimeout].
		limit = max(limit, maxTimeout*debug.TimeoutMultiplier)
	}
	ceiling := createdAt.Add(limit)
	ceilingLimit := extensionLimitTestContextCap
	if explicitTimeout {
		ceilingLimit = extensionLimitExplicitTimeout
	}
	if goTestDeadline, ok := GoTestDeadline(tb); ok && goTestDeadline.Before(ceiling) {
		ceiling = goTestDeadline
		ceilingLimit = extensionLimitGoTestTimeout
	}
	if parentDeadline, ok := tb.Context().Deadline(); ok && parentDeadline.Before(ceiling) {
		ceiling = parentDeadline
		ceilingLimit = ""
	}

	st := &contextState{
		createdAt:             createdAt,
		timeout:               timeout,
		extensionCeilingLimit: ceilingLimit,
	}
	st.timeoutContext = newTimeoutContext(tb.Context(), ceiling, activeExpiration)
	ctx := context.WithValue(st.timeoutContext, ownerKey{}, st)

	// Annotate gRPC requests with the test name for OTEL tracing.
	st.current = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())
	if ceilingLimit == extensionLimitGoTestTimeout && !ceiling.After(activeExpiration) {
		st.extensionLimit = ceilingLimit
	}
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

			if message := st.cleanup(); message != "" {
				tb.Errorf("%s", message)
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

// cleanup cancels the test context and reports whether its active timeout had
// already fired, returning its failure message.
func (s *contextState) cleanup() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.timeoutContext.cancel()
	var timeoutMessage string
	if s.timeoutContext.Err() == context.DeadlineExceeded {
		timeoutMessage = s.timeoutExceededMessageLocked()
	}

	// Keep current: it is canceled now, but callers still racing with cleanup
	// must get a context, not a panic.
	return timeoutMessage
}

func (s *contextState) timeoutExceededMessageLocked() string {
	currentTimeout := s.timeoutContext.effectiveExpiration().Sub(s.createdAt)

	message := fmt.Sprintf("test exceeded timeout of %v", reportDuration(s.timeout))
	if s.extensionLimit == extensionLimitGoTestTimeout {
		message = fmt.Sprintf("test exceeded go test timeout before test context timeout of %v", reportDuration(s.timeout))
	} else if currentTimeout > s.timeout && s.extensionLimit == extensionLimitTestContextCap {
		ceiling, _ := s.timeoutContext.Deadline()
		message = fmt.Sprintf(
			"test exceeded test context extension cap of %v (originally %v)",
			reportDuration(ceiling.Sub(s.createdAt)),
			reportDuration(s.timeout),
		)
	} else if currentTimeout > s.timeout {
		message = fmt.Sprintf("test exceeded extended timeout of %v (originally %v)", reportDuration(currentTimeout), reportDuration(s.timeout))
	}
	if audit := s.extensionAuditLocked(); audit != "" {
		message += "\n" + audit
	}
	return message
}

func (s *contextState) extensionAuditLocked() string {
	if len(s.extensionGrants) == 0 && s.extensionDenied == 0 {
		return ""
	}

	var total time.Duration
	for _, grant := range s.extensionGrants {
		total += grant.duration
	}
	var message strings.Builder
	fmt.Fprintf(&message, "ctx extensions   = %d (+%v total", len(s.extensionGrants), reportDuration(total))
	if s.extensionLimit != "" {
		message.WriteString("; limited by " + s.extensionLimit)
	}
	message.WriteString(")")
	writeGrant := func(i int, grant extensionGrant) {
		fmt.Fprintf(&message, "\n  %d. +%v after %v", i+1, reportDuration(grant.duration), reportDuration(grant.elapsed))
	}
	if len(s.extensionGrants) <= reportHeadExtensions+reportTailExtensions {
		for i, grant := range s.extensionGrants {
			writeGrant(i, grant)
		}
	} else {
		for i, grant := range s.extensionGrants[:reportHeadExtensions] {
			writeGrant(i, grant)
		}
		omitted := len(s.extensionGrants) - reportHeadExtensions - reportTailExtensions
		fmt.Fprintf(&message, "\n  ... %d extensions omitted ...", omitted)
		for i, grant := range s.extensionGrants[len(s.extensionGrants)-reportTailExtensions:] {
			writeGrant(len(s.extensionGrants)-reportTailExtensions+i, grant)
		}
	}
	if s.extensionDenied == 1 {
		message.WriteString("\n1 context extension denied")
	} else if s.extensionDenied > 1 {
		fmt.Fprintf(&message, "\n%d context extensions denied", s.extensionDenied)
	}
	return message.String()
}

// Keep this formatting consistent with await timeout reports, which embed this text.
func reportDuration(d time.Duration) string {
	if d > -time.Millisecond && d < time.Millisecond {
		rounded := d.Round(time.Microsecond)
		if rounded != 0 {
			return rounded.String()
		}
	}
	return d.Round(time.Millisecond).String()
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
