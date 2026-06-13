package testcontext

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc/metadata"
)

const (
	defaultTimeout        = 30 * time.Second
	defaultMaxTestTimeout = 2 * time.Minute
	testNameMetadataKey   = "temporal-test-name"

	deadlineLimitTestContextCap = "test context extension cap"
	deadlineLimitGoTestTimeout  = "go test timeout"
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

type extensionGrant struct {
	duration time.Duration
	elapsed  time.Duration
}

// Extension describes the effect of an EnsureRemaining call.
type Extension struct {
	Deadline time.Time
	Granted  time.Duration
	Report   string
	Limit    string

	currentContext context.Context
	contexts       []context.Context
	state          *contextState
}

// AppliesTo reports whether ctx is the test-scoped context observed by
// EnsureRemaining.
func (e Extension) AppliesTo(ctx context.Context) bool {
	for _, testCtx := range e.contexts {
		if ctx == testCtx {
			return true
		}
	}
	return false
}

// Context returns the current test-scoped context after EnsureRemaining.
func (e Extension) Context() context.Context {
	return e.currentContext
}

// SuppressCleanupReport marks the timeout as already reported.
func (e Extension) SuppressCleanupReport() {
	if e.state != nil {
		e.state.markTimeoutReported()
	}
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
// AttachDecorator creates it with the default timeout. Call For with WithTimeout
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
	mu                      sync.Mutex
	ctx                     context.Context
	contexts                []context.Context
	cancels                 []context.CancelFunc
	testStart               time.Time
	originalTimeout         time.Duration
	decorators              map[any]struct{}
	orderedDecorators       []contextDecorator
	extensionGrants         []extensionGrant
	extensionDenied         int
	extensionRequestedTotal time.Duration
	deadlineLimit           string
	timeoutReported         bool
}

func getContextState(tb testing.TB, timeout time.Duration) *contextState {
	tb.Helper()

	testContexts.Lock()
	defer testContexts.Unlock()

	if st, ok := testContexts.byTest[tb]; ok {
		return st
	}

	st := &contextState{
		testStart:       time.Now(),
		originalTimeout: timeout,
		decorators:      make(map[any]struct{}),
	}
	st.setDeadline(tb, st.testStart.Add(timeout), "")
	testContexts.byTest[tb] = st

	tb.Cleanup(func() {
		err := st.err()
		st.cancel()
		testContexts.Lock()
		delete(testContexts.byTest, tb)
		testContexts.Unlock()
		if errors.Is(err, context.DeadlineExceeded) && st.markTimeoutReported() {
			tb.Errorf("%v", st.timeoutExceededError(err))
		}
	})
	return st
}

// EnsureRemaining extends the test-scoped context so at least d remains from
// now, if its current deadline is earlier. It is capped so the total test
// timeout never exceeds max(maxTestTimeout, configuredTimeout). If tb has no
// test context created by this package, EnsureRemaining is a no-op.
//
// Existing context values returned by earlier calls to For keep their
// original deadline; call For again after EnsureRemaining to observe the
// extended deadline.
func EnsureRemaining(tb testing.TB, d time.Duration) Extension {
	tb.Helper()
	if d <= 0 {
		return Extension{}
	}

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return Extension{}
	}

	return st.ensureRemainingUntil(tb, time.Now().Add(d))
}

func (s *contextState) ensureRemainingUntil(tb testing.TB, target time.Time) Extension {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentDeadline, ok := s.ctx.Deadline()
	if !ok {
		return Extension{}
	}

	requested := target.Sub(currentDeadline)
	if requested <= 0 {
		return s.extension(currentDeadline, 0)
	}

	s.extensionRequestedTotal += requested
	limit := ""
	capDeadline := s.testStart.Add(max(maxTestTimeout(), s.originalTimeout))
	if capDeadline.Before(target) {
		target = capDeadline
		limit = deadlineLimitTestContextCap
	}
	if goTestDeadline, ok := tb.Context().Deadline(); ok && goTestDeadline.Before(target) {
		target = goTestDeadline
		limit = deadlineLimitGoTestTimeout
	}
	granted := target.Sub(currentDeadline)
	if granted <= 0 {
		if limit != "" {
			s.deadlineLimit = limit
		}
		s.extensionDenied++
		return s.extension(currentDeadline, 0)
	}

	s.extensionGrants = append(s.extensionGrants, extensionGrant{
		duration: granted,
		elapsed:  time.Since(s.testStart),
	})
	s.setDeadline(tb, target, limit)
	return s.extension(target, granted)
}

func (s *contextState) extension(deadline time.Time, granted time.Duration) Extension {
	return Extension{
		Deadline:       deadline,
		Granted:        granted,
		Report:         s.extensionReportLocked(),
		Limit:          s.deadlineLimit,
		currentContext: s.ctx,
		contexts:       append([]context.Context(nil), s.contexts...),
		state:          s,
	}
}

func (s *contextState) configure(tb testing.TB, cfg config) {
	tb.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.timeoutSet && cfg.timeout != s.originalTimeout {
		tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", s.originalTimeout, cfg.timeout)
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

func (s *contextState) setDeadline(tb testing.TB, deadline time.Time, limit string) {
	if goTestDeadline, ok := tb.Context().Deadline(); ok && goTestDeadline.Before(deadline) {
		limit = deadlineLimitGoTestTimeout
	}

	ctx, cancel := context.WithDeadline(tb.Context(), deadline)

	// Annotate gRPC requests with the test name for OTEL tracing.
	ctx = metadata.AppendToOutgoingContext(ctx, testNameMetadataKey, tb.Name())

	for _, decorator := range s.orderedDecorators {
		ctx = decorator.decorate(ctx)
	}
	s.ctx = ctx
	s.contexts = append(s.contexts, ctx)
	s.cancels = append(s.cancels, cancel)
	s.deadlineLimit = limit
}

func (s *contextState) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.cancels) - 1; i >= 0; i-- {
		s.cancels[i]()
	}
}

func (s *contextState) timeoutExceededError(err error) error {
	return fmt.Errorf("%w: %s", err, s.timeoutExceededMessage())
}

func (s *contextState) markTimeoutReported() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timeoutReported {
		return false
	}
	s.timeoutReported = true
	return true
}

func (s *contextState) timeoutExceededMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentTimeout := s.originalTimeout
	if deadline, ok := s.ctx.Deadline(); ok {
		currentTimeout = deadline.Sub(s.testStart)
	}
	if s.deadlineLimit == deadlineLimitGoTestTimeout {
		return s.withExtensionReportLocked(fmt.Sprintf("test exceeded go test timeout before test context timeout of %v", reportDuration(s.originalTimeout)))
	}
	if currentTimeout <= s.originalTimeout {
		return s.withExtensionReportLocked(fmt.Sprintf("test exceeded timeout of %v", reportDuration(s.originalTimeout)))
	}
	if currentTimeout-s.originalTimeout < s.extensionRequestedTotal {
		return s.withExtensionReportLocked(fmt.Sprintf(
			"test exceeded test context extension cap of %v (originally %v, extensions requested total %v)",
			reportDuration(maxTestTimeout()),
			reportDuration(s.originalTimeout),
			reportDuration(s.extensionRequestedTotal),
		))
	}
	return s.withExtensionReportLocked(fmt.Sprintf("test exceeded extended timeout of %v (originally %v)", reportDuration(currentTimeout), reportDuration(s.originalTimeout)))
}

func (s *contextState) withExtensionReportLocked(message string) string {
	if report := s.extensionReportLocked(); report != "" {
		return message + "\n" + report
	}
	return message
}

func (s *contextState) extensionReportLocked() string {
	if len(s.extensionGrants) == 0 && s.extensionDenied == 0 {
		return ""
	}
	if len(s.extensionGrants) == 0 {
		return contextExtensionDeniedMessage(s.extensionDenied)
	}
	var total time.Duration
	for _, grant := range s.extensionGrants {
		total += grant.duration
	}
	message := fmt.Sprintf("ctx extensions   = %d (+%v total)", len(s.extensionGrants), reportDuration(total))
	for i, grant := range s.extensionGrants {
		message += fmt.Sprintf("\n  %d. +%v after %v", i+1, reportDuration(grant.duration), reportDuration(grant.elapsed))
	}
	if s.extensionDenied > 0 {
		message += fmt.Sprintf("\n%s", contextExtensionDeniedMessage(s.extensionDenied))
	}
	return message
}

func contextExtensionDeniedMessage(count int) string {
	if count == 1 {
		return "1 context extension denied"
	}
	return fmt.Sprintf("%d context extensions denied", count)
}

func reportDuration(d time.Duration) string {
	if d > -time.Millisecond && d < time.Millisecond {
		rounded := d.Round(time.Microsecond)
		if rounded == 0 {
			return "0µs"
		}
		return rounded.String()
	}
	return d.Round(time.Millisecond).String()
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

func maxTestTimeout() time.Duration {
	return defaultMaxTestTimeout * debug.TimeoutMultiplier
}
