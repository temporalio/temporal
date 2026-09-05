package httpfaults_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/httpfaults"
)

func newRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://example.com/cb1", nil)
	require.NoError(t, err)
	return req
}

type trackingBody struct {
	io.Reader
	closed   bool
	closeErr error
}

func (b *trackingBody) Close() error {
	b.closed = true
	return b.closeErr
}

func TestWrap_NilGeneratorReturnsNext(t *testing.T) {
	t.Parallel()

	next := func(*http.Request) (*http.Response, error) { return nil, nil }
	requireSameFunction(t, next, httpfaults.Wrap(nil, httpfaults.Scope{}, next))
}

func TestWrap_RequestFault(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	generator := httpfaults.NewCallbackGenerator()
	generator.RegisterRequestCallback(httpfaults.Scope{}, func(_ context.Context, operation string, req *httpfaults.Request) *httpfaults.Outcome {
		require.Equal(t, "POST /cb1", operation)
		require.Equal(t, "/cb1", req.Raw.URL.Path)
		return &httpfaults.Outcome{Error: injectedErr}
	})

	called := false
	wrapped := httpfaults.Wrap(generator, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		called = true
		return nil, nil
	})
	resp, err := wrapped(newRequest(t))

	require.Nil(t, resp)
	require.ErrorIs(t, err, injectedErr)
	require.False(t, called)
}

func TestWrap_RequestFaultResponse(t *testing.T) {
	t.Parallel()

	injected := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	generator := httpfaults.NewCallbackGenerator()
	generator.RegisterRequestCallback(httpfaults.Scope{}, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome {
		return &httpfaults.Outcome{Response: injected}
	})

	wrapped := httpfaults.Wrap(generator, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		require.FailNow(t, "HTTP call should not run")
		return nil, nil
	})
	resp, err := wrapped(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
}

func TestWrap_ResponseFault(t *testing.T) {
	t.Parallel()

	body := &trackingBody{Reader: strings.NewReader("original")}
	original := &http.Response{StatusCode: http.StatusOK, Body: body}
	injected := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	generator := httpfaults.NewCallbackGenerator()
	generator.RegisterResponseCallback(httpfaults.Scope{}, func(
		_ context.Context,
		_ string,
		_ *httpfaults.Request,
		resp *http.Response,
		callErr error,
	) *httpfaults.Outcome {
		require.Same(t, original, resp)
		require.NoError(t, callErr)
		return &httpfaults.Outcome{Response: injected}
	})

	wrapper := httpfaults.Wrap(generator, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return original, nil
	})
	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
	require.True(t, body.closed)
	require.NoError(t, resp.Body.Close())
}

func TestWrap_ResponseFaultIncludesCloseError(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	closeErr := errors.New("close")
	body := &trackingBody{closeErr: closeErr}
	generator := httpfaults.NewCallbackGenerator()
	generator.RegisterResponseCallback(httpfaults.Scope{}, func(context.Context, string, *httpfaults.Request, *http.Response, error) *httpfaults.Outcome {
		return &httpfaults.Outcome{Error: injectedErr}
	})
	wrapper := httpfaults.Wrap(generator, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return &http.Response{Body: body}, nil
	})

	resp, err := wrapper(newRequest(t))

	require.Nil(t, resp)
	require.ErrorIs(t, err, injectedErr)
	require.ErrorIs(t, err, closeErr)
	require.True(t, body.closed)
}

func TestWrap_ScopedByNamespace(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	generator := httpfaults.NewCallbackGenerator()
	generator.RegisterRequestCallback(httpfaults.Scope{NamespaceID: "target-ns"}, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome {
		return &httpfaults.Outcome{Error: injectedErr}
	})
	next := func(*http.Request) (*http.Response, error) {
		return httpfaults.NewResponse(http.StatusOK, "ok"), nil
	}

	_, err := httpfaults.Wrap(generator, httpfaults.Scope{NamespaceID: "target-ns"}, next)(newRequest(t))
	require.ErrorIs(t, err, injectedErr)

	resp, err := httpfaults.Wrap(generator, httpfaults.Scope{NamespaceID: "other-ns"}, next)(newRequest(t))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestNewResponse(t *testing.T) {
	t.Parallel()

	resp := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "503 Service Unavailable", resp.Status)
	require.Equal(t, int64(len("injected")), resp.ContentLength)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "injected", string(body))
}

func requireSameFunction(
	t *testing.T,
	expected func(*http.Request) (*http.Response, error),
	actual func(*http.Request) (*http.Response, error),
) {
	t.Helper()
	require.Equal(t, reflect.ValueOf(expected).Pointer(), reflect.ValueOf(actual).Pointer())
}
