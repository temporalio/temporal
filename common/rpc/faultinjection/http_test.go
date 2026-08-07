//go:build test_dep

package faultinjection

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testhooks"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPRoundTripper_NoGenerator(t *testing.T) {
	t.Parallel()

	expected := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("response")),
	}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return expected, nil
	})
	request, err := http.NewRequest(http.MethodPost, "http://example.com/service/operation", nil)
	require.NoError(t, err)

	response, err := HTTPRoundTripper(base, "namespace-id", testhooks.NewTestHooks()).RoundTrip(request)

	require.NoError(t, err)
	require.Same(t, expected, response)
}

func TestHTTPRoundTripper_BeforeRequest(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	request, err := http.NewRequest(http.MethodPost, "http://example.com/service/operation", nil)
	require.NoError(t, err)
	testhooks.Set(
		testHooks,
		testhooks.RPCFaultGenerator,
		func(_ context.Context, method string, faultRequest, response any, err error) (bool, any, error) {
			require.Equal(t, "HTTP POST /service/operation", method)
			httpFaultRequest, ok := faultRequest.(*HTTPFaultRequest)
			require.True(t, ok)
			require.Equal(t, "namespace-id", httpFaultRequest.NamespaceID)
			require.Same(t, request, httpFaultRequest.Request)
			require.Nil(t, response)
			require.NoError(t, err)
			return true, nil, injectedErr
		},
		testhooks.GlobalScope,
	)
	baseCalled := false
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		baseCalled = true
		return nil, nil
	})

	response, err := HTTPRoundTripper(base, "namespace-id", testHooks).RoundTrip(request)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.False(t, baseCalled)
}

func TestHTTPRoundTripper_AfterRequest(t *testing.T) {
	t.Parallel()

	testHooks := testhooks.NewTestHooks()
	injectedErr := errors.New("injected")
	testhooks.Set(
		testHooks,
		testhooks.RPCFaultGenerator,
		func(_ context.Context, _ string, _ any, response any, _ error) (bool, any, error) {
			if response == nil {
				return false, nil, nil
			}
			return true, nil, injectedErr
		},
		testhooks.GlobalScope,
	)
	body := &closeTracker{Reader: strings.NewReader("response")}
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})
	request, err := http.NewRequest(http.MethodPost, "http://example.com/service/operation", nil)
	require.NoError(t, err)

	response, err := HTTPRoundTripper(base, "namespace-id", testHooks).RoundTrip(request)

	require.ErrorIs(t, err, injectedErr)
	require.Nil(t, response)
	require.True(t, body.closed)
}

type closeTracker struct {
	io.Reader
	closed bool
}

func (b *closeTracker) Close() error {
	b.closed = true
	return nil
}
