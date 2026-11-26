package testcore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/testx/failpoint"
)

// cleanupTB collects the cleanups of a test and records a fatal error, which it raises as a panic
// in place of the runtime.Goexit of testing.T, so that a test can run the cleanups itself.
type cleanupTB struct {
	testing.TB
	cleanups []func()
	errors   []string
}

type fatal struct{}

func (c *cleanupTB) Helper() {}

func (c *cleanupTB) Cleanup(f func()) { c.cleanups = append(c.cleanups, f) }

func (c *cleanupTB) Logf(string, ...any) {}

func (c *cleanupTB) Error(args ...any) { c.errors = append(c.errors, fmt.Sprint(args...)) }

func (c *cleanupTB) Fatalf(format string, args ...any) {
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
	panic(fatal{})
}

func (c *cleanupTB) runCleanups() {
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
}

func TestAddFaultCallbackToScopes_CollisionRemovesTheEarlierScope(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	// A failpoint of another owner, such as a rule, is registered for the second scope.
	other := failpoint.NewFailpoint(faultCallbackKey.ForScope("second"))
	registry.Set(other)

	tb := &cleanupTB{TB: t}
	require.Panics(t, func() {
		addFaultCallbackToScopes(tb, callbacks, faultCallbackKey, []string{"first", "second"},
			func(arg any) (any, bool, error) { return arg, false, nil })
	})
	require.Len(t, tb.errors, 1, "only the collision fails the test")
	require.Nil(t, registry.Get(faultCallbackKey.ForScope("first")), "the callback of the first scope is removed")
	require.Same(t, other, registry.Get(faultCallbackKey.ForScope("second")))

	tb.runCleanups()
	require.Len(t, tb.errors, 1, "the cleanup doesn't report a fault that never fired")
}

func TestAddFaultCallbackToScopes_CleanupRemovesTheCallbacks(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	callbacks := newFaultCallbacks(registry)
	tb := &cleanupTB{TB: t}
	addFaultCallbackToScopes(tb, callbacks, faultCallbackKey, []string{"first", "second"},
		func(arg any) (any, bool, error) { return arg, false, nil })
	require.NotNil(t, registry.Get(faultCallbackKey.ForScope("first")))
	require.NotNil(t, registry.Get(faultCallbackKey.ForScope("second")))

	tb.runCleanups()
	require.Nil(t, registry.Get(faultCallbackKey.ForScope("first")))
	require.Nil(t, registry.Get(faultCallbackKey.ForScope("second")))
	require.Len(t, tb.errors, 1, "the fault never fired")
}

func TestHTTPFaults_Outcome(t *testing.T) {
	t.Parallel()

	raw, err := http.NewRequest(http.MethodPost, "http://example.com/cb", nil)
	require.NoError(t, err)
	replacement := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	injectedErr := errors.New("injected")

	for name, tc := range map[string]struct {
		resp         *http.Response
		err          error
		wantInjected bool
		wantReplaced *http.Response
	}{
		"nothing":  {},
		"response": {resp: replacement, wantInjected: true, wantReplaced: replacement},
		"error":    {err: injectedErr, wantInjected: true},
		// An error wins over a response, so the response is not replaced.
		"error and response": {resp: replacement, err: injectedErr, wantInjected: true},
	} {
		t.Run("request/"+name, func(t *testing.T) {
			t.Parallel()
			fault := httpRequestFault(func(context.Context, *http.Request) (*http.Response, error) {
				return tc.resp, tc.err
			})
			req := &httpfaults.Request{Raw: raw}

			_, injected, err := fault(req)

			require.Equal(t, tc.wantInjected, injected)
			require.Equal(t, tc.err, err)
			require.Same(t, tc.wantReplaced, req.Replaced())
		})
		t.Run("response/"+name, func(t *testing.T) {
			t.Parallel()
			fault := httpResponseFault(func(context.Context, *http.Request, *http.Response, error) (*http.Response, error) {
				return tc.resp, tc.err
			})
			result := &httpfaults.Result{Request: &httpfaults.Request{Raw: raw}}

			_, injected, err := fault(result)

			require.Equal(t, tc.wantInjected, injected)
			require.Equal(t, tc.err, err)
			require.Same(t, tc.wantReplaced, result.Replaced())
		})
	}
}
