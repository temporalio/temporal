package testcontext

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
)

const defaultTimeout = 90 * time.Second
const defaultMaxTestTimeout = 2 * time.Minute

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
	decorators []contextDecorator
}

type contextDecorator struct {
	key      any
	decorate func(context.Context) context.Context
}

// New returns the test-scoped context for tb. The context is canceled when the
// test ends or when the configured test timeout expires.
//
// The first call creates the per-test context and fixes its timeout. Later calls
// may add decorators, but an explicit different timeout fails instead of being
// silently ignored.
func New(tb testing.TB, opts ...Option) context.Context {
	tb.Helper()

	cfg := config{timeout: effectiveTimeout(0)}
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getContextState(tb, cfg.timeout)
	st.configure(tb, cfg)
	return st.context()
}

// Option configures the test-scoped context returned by [New].
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

// WithContextDecorator applies decorator to the test-scoped context once for key.
// Reusing the same key is a no-op.
func WithContextDecorator[K comparable](key K, decorator func(context.Context) context.Context) Option {
	return func(cfg *config) {
		cfg.decorators = append(cfg.decorators, contextDecorator{
			key:      key,
			decorate: decorator,
		})
	}
}

type contextState struct {
	mu                      sync.Mutex
	ctx                     context.Context
	cancels                 []context.CancelFunc
	testStart               time.Time
	originalTimeout         time.Duration
	decorators              map[any]struct{}
	orderedDecorators       []contextDecorator
	extensionCapHit         bool
	extensionRequestedTotal time.Duration
}

func getContextState(tb testing.TB, timeout time.Duration) *contextState {
	tb.Helper()

	testContexts.Lock()
	defer testContexts.Unlock()

	if st, ok := testContexts.byTest[tb]; ok {
		return st
	}

	testStart := time.Now()
	ctx, cancel := context.WithDeadline(tb.Context(), testStart.Add(timeout))
	st := &contextState{
		ctx:             ctx,
		cancels:         []context.CancelFunc{cancel},
		testStart:       testStart,
		originalTimeout: timeout,
		decorators:      make(map[any]struct{}),
	}
	testContexts.byTest[tb] = st

	tb.Cleanup(func() {
		err := st.err()
		st.cancel()
		testContexts.Lock()
		delete(testContexts.byTest, tb)
		testContexts.Unlock()
		if err == context.DeadlineExceeded {
			tb.Errorf("%s", st.timeoutExceededMessage())
		}
	})
	return st
}

// Extend extends the test-scoped context's deadline by d, capped so the total
// test timeout never exceeds max(maxTestTimeout, configuredTimeout). It returns
// the new deadline and the amount actually granted. If tb has no test context
// created by this package, Extend is a no-op.
//
// Existing context values returned by earlier calls to New keep their original
// deadline; call New again after Extend to observe the extended deadline.
func Extend(tb testing.TB, d time.Duration) (newDeadline time.Time, granted time.Duration) {
	tb.Helper()
	if d <= 0 {
		return time.Time{}, 0
	}

	testContexts.Lock()
	st, ok := testContexts.byTest[tb]
	testContexts.Unlock()
	if !ok {
		return time.Time{}, 0
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	currentDeadline, ok := st.ctx.Deadline()
	if !ok {
		return time.Time{}, 0
	}

	st.extensionRequestedTotal += d
	target := currentDeadline.Add(d)
	capDeadline := st.testStart.Add(max(maxTestTimeout(), st.originalTimeout))
	if capDeadline.Before(target) {
		target = capDeadline
		st.extensionCapHit = true
	}
	granted = target.Sub(currentDeadline)
	if granted <= 0 {
		return currentDeadline, 0
	}

	ctx, cancel := context.WithDeadline(tb.Context(), target)
	for _, decorator := range st.orderedDecorators {
		ctx = decorator.decorate(ctx)
	}
	st.ctx = ctx
	st.cancels = append(st.cancels, cancel)
	return target, granted
}

func (s *contextState) configure(tb testing.TB, cfg config) {
	tb.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.timeoutSet && cfg.timeout != s.originalTimeout {
		tb.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", s.originalTimeout, cfg.timeout)
	}

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	for _, decorator := range cfg.decorators {
		if decorator.key == nil {
			tb.Fatal("testcontext: context decorator key must not be nil")
		}
		if decorator.decorate == nil {
			tb.Fatal("testcontext: context decorator must not be nil")
		}
		if _, ok := s.decorators[decorator.key]; ok {
			continue
		}
		s.ctx = decorator.decorate(s.ctx)
		s.decorators[decorator.key] = struct{}{}
		s.orderedDecorators = append(s.orderedDecorators, decorator)
	}
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

func (s *contextState) cancel() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.cancels) - 1; i >= 0; i-- {
		s.cancels[i]()
	}
}

func (s *contextState) timeoutExceededMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.timeoutExtended() {
		return fmt.Sprintf("Test exceeded timeout of %v", s.originalTimeout)
	}
	if s.extensionCapHit {
		return fmt.Sprintf(
			"Test exceeded %v cap (originally %v, extensions requested total %v)",
			maxTestTimeout(),
			s.originalTimeout,
			s.extensionRequestedTotal,
		)
	}
	return fmt.Sprintf("Test exceeded extended timeout of %v (originally %v)", s.currentTimeout(), s.originalTimeout)
}

func (s *contextState) timeoutExtended() bool {
	return s.currentTimeout() > s.originalTimeout
}

func (s *contextState) currentTimeout() time.Duration {
	deadline, ok := s.ctx.Deadline()
	if !ok {
		return s.originalTimeout
	}
	return deadline.Sub(s.testStart)
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

	// 3. Default 90 seconds.
	return defaultTimeout
}

func maxTestTimeout() time.Duration {
	return defaultMaxTestTimeout * debug.TimeoutMultiplier
}
