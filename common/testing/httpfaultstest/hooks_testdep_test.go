//go:build test_dep

package httpfaultstest_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testhooks"
)

func newRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://example.com/cb1", nil)
	require.NoError(t, err)
	return req
}

func TestWrap_NoGenerator(t *testing.T) {
	t.Parallel()

	handlerCalled := false
	next := func(*http.Request) (*http.Response, error) {
		handlerCalled = true
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	wrapped := httpfaults.Wrap(httpfaultstest.NewGenerator(testhooks.NewTestHooks()), httpfaults.Scope{NamespaceID: "namespace-id"}, next)

	resp, err := wrapped(newRequest(t))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, handlerCalled)
}

func TestWrap_BeforeHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	generator := httpfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterRequestCallback(httpfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome {
		return &httpfaults.Outcome{Error: injectedErr}
	})
	handlerCalled := false
	next := func(*http.Request) (*http.Response, error) {
		handlerCalled = true
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	wrapped := httpfaults.Wrap(httpfaultstest.NewGenerator(testHooks), httpfaults.Scope{NamespaceID: "namespace-id"}, next)

	_, err := wrapped(newRequest(t))

	require.ErrorIs(t, err, injectedErr)
	require.False(t, handlerCalled)
}

func TestWrap_AfterHandler(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := httpfaultstest.NewCallbackGenerator(testHooks)
	generator.RegisterResponseCallback(httpfaults.Scope{NamespaceName: "namespace-name"}, func(context.Context, string, *httpfaults.Request, *http.Response, error) *httpfaults.Outcome {
		return &httpfaults.Outcome{Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")}
	})
	wrapper := httpfaults.Wrap(
		httpfaultstest.NewGenerator(testHooks),
		httpfaults.Scope{NamespaceName: "namespace-name"},
		func(*http.Request) (*http.Response, error) {
			return httpfaults.NewResponse(http.StatusOK, "original"), nil
		},
	)

	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestNewCallbackGenerator_UnregisterRemovesHook(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	generator := httpfaultstest.NewCallbackGenerator(testHooks)
	unregister := generator.RegisterRequestCallback(httpfaults.Scope{NamespaceID: "namespace-id"}, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome {
		return nil
	})
	_, ok := testhooks.Get(testHooks, testhooks.HTTPRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.True(t, ok)

	unregister()
	_, ok = testhooks.Get(testHooks, testhooks.HTTPRequestFaultGeneratorByNamespaceID, namespace.ID("namespace-id"))
	require.False(t, ok)
}

func TestNewCallbackGenerator_RequiresNamespaceScope(t *testing.T) {
	t.Parallel()

	generator := httpfaultstest.NewCallbackGenerator(testhooks.NewTestHooks())
	require.PanicsWithValue(t, "fault injection test hooks require a namespace scope", func() {
		generator.RegisterRequestCallback(httpfaults.Scope{}, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome {
			return nil
		})
	})
}
