package testcontext

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/debug"
)

const defaultTimeout = 90 * time.Second

type contextStore struct {
	sync.Mutex
	byTest map[*testing.T]*contextState
}

// testContexts is process-global so repeated helpers in the same test share
// one context and one cleanup.
var testContexts = contextStore{
	byTest: make(map[*testing.T]*contextState),
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

// New returns the test-scoped context for t. The context is canceled when the
// test ends or when the configured test timeout expires.
//
// The first call creates the per-test context and fixes its timeout. Later calls
// may add decorators, but an explicit different timeout fails instead of being
// silently ignored.
func New(t *testing.T, opts ...Option) context.Context {
	t.Helper()

	cfg := config{timeout: effectiveTimeout(0)}
	for _, opt := range opts {
		opt(&cfg)
	}

	st := getContextState(t, cfg.timeout)
	st.configure(t, cfg)
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
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	timeout    time.Duration
	decorators map[any]struct{}
}

func getContextState(t *testing.T, timeout time.Duration) *contextState {
	t.Helper()

	testContexts.Lock()
	defer testContexts.Unlock()

	if st, ok := testContexts.byTest[t]; ok {
		return st
	}

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	st := &contextState{
		ctx:        ctx,
		cancel:     cancel,
		timeout:    timeout,
		decorators: make(map[any]struct{}),
	}
	testContexts.byTest[t] = st

	t.Cleanup(func() {
		st.cancel()
		testContexts.Lock()
		delete(testContexts.byTest, t)
		testContexts.Unlock()
		if st.err() == context.DeadlineExceeded {
			t.Errorf("Test exceeded timeout of %v", st.timeout)
		}
	})
	return st
}

func (s *contextState) configure(t *testing.T, cfg config) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.timeoutSet && cfg.timeout != s.timeout {
		t.Fatalf("testcontext: test context already exists with timeout %v; cannot change it to %v", s.timeout, cfg.timeout)
	}

	// Decorators may be registered by independent helpers, so apply each keyed
	// decorator at most once while preserving call order.
	for _, decorator := range cfg.decorators {
		if decorator.key == nil {
			t.Fatal("testcontext: context decorator key must not be nil")
		}
		if decorator.decorate == nil {
			t.Fatal("testcontext: context decorator must not be nil")
		}
		if _, ok := s.decorators[decorator.key]; ok {
			continue
		}
		s.ctx = decorator.decorate(s.ctx)
		s.decorators[decorator.key] = struct{}{}
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
