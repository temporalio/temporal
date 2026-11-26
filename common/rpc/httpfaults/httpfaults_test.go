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
	require.Equal(
		t,
		reflect.ValueOf(next).Pointer(),
		reflect.ValueOf(httpfaults.Wrap(nil, httpfaults.Scope{}, next)).Pointer(),
	)
}

// generator returns the outcomes of its functions; a nil function returns no fault.
type generator struct {
	request  func(operation string, req *httpfaults.Request) (*http.Response, error)
	response func(operation string, req *httpfaults.Request, resp *http.Response, err error) (*http.Response, error)
}

func (g generator) GenerateRequest(_ context.Context, operation string, req *httpfaults.Request) (*http.Response, error) {
	if g.request == nil {
		return nil, nil
	}
	return g.request(operation, req)
}

func (g generator) GenerateResponse(_ context.Context, operation string, req *httpfaults.Request, resp *http.Response, err error) (*http.Response, error) {
	if g.response == nil {
		return nil, nil
	}
	return g.response(operation, req, resp, err)
}

func TestWrap_NoFault(t *testing.T) {
	t.Parallel()

	original := httpfaults.NewResponse(http.StatusOK, "original")
	callErr := errors.New("call")
	wrapped := httpfaults.Wrap(generator{}, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return original, callErr
	})
	resp, err := wrapped(newRequest(t))

	require.ErrorIs(t, err, callErr)
	require.Same(t, original, resp)
}

func TestWrap_RequestFault(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	scope := httpfaults.Scope{NamespaceID: "namespace-id"}
	gen := generator{request: func(operation string, req *httpfaults.Request) (*http.Response, error) {
		require.Equal(t, "POST /cb1", operation)
		require.Equal(t, "/cb1", req.Raw.URL.Path)
		require.Equal(t, scope, req.Scope)
		return nil, injectedErr
	}}

	called := false
	wrapped := httpfaults.Wrap(gen, scope, func(*http.Request) (*http.Response, error) {
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
	gen := generator{request: func(string, *httpfaults.Request) (*http.Response, error) {
		return injected, nil
	}}

	wrapped := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
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
	gen := generator{response: func(operation string, _ *httpfaults.Request, resp *http.Response, callErr error) (*http.Response, error) {
		require.Equal(t, "POST /cb1", operation)
		require.Same(t, original, resp)
		require.NoError(t, callErr)
		return injected, nil
	}}

	wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return original, nil
	})
	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
	require.True(t, body.closed)
	require.NoError(t, resp.Body.Close())
}

func TestWrap_ResponseFaultKeepsOriginalResponse(t *testing.T) {
	t.Parallel()

	body := &trackingBody{Reader: strings.NewReader("original")}
	original := &http.Response{StatusCode: http.StatusOK, Body: body}
	gen := generator{response: func(_ string, _ *httpfaults.Request, resp *http.Response, _ error) (*http.Response, error) {
		return resp, nil
	}}

	wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return original, nil
	})
	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Same(t, original, resp)
	require.False(t, body.closed)
}

func TestWrap_ResponseReplacement(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close")
	tests := []struct {
		name string
		// replacement returns the replacement for the original response of the call.
		replacement   func(original *http.Response) *http.Response
		wantNewStatus int  // the status of the returned response
		wantClosed    bool // the original body was closed
	}{
		{
			name: "another response",
			replacement: func(*http.Response) *http.Response {
				return httpfaults.NewResponse(http.StatusServiceUnavailable, "")
			},
			wantNewStatus: http.StatusServiceUnavailable,
			wantClosed:    true,
		},
		{
			name:          "the original response",
			replacement:   func(original *http.Response) *http.Response { return original },
			wantNewStatus: http.StatusOK,
			wantClosed:    false,
		},
		{
			// Only the original response itself keeps the body open: a copy is another response,
			// which must not share the body of the original.
			name: "a copy of the original response",
			replacement: func(original *http.Response) *http.Response {
				replacement := *original
				replacement.StatusCode = http.StatusServiceUnavailable
				return &replacement
			},
			wantNewStatus: http.StatusServiceUnavailable,
			wantClosed:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &trackingBody{Reader: strings.NewReader("original"), closeErr: closeErr}
			original := &http.Response{StatusCode: http.StatusOK, Body: body}
			gen := generator{response: func(_ string, _ *httpfaults.Request, resp *http.Response, _ error) (*http.Response, error) {
				return tc.replacement(resp), nil
			}}
			wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
				return original, nil
			})

			resp, err := wrapper(newRequest(t))

			// A failure to close the original body doesn't drop the replacement.
			require.NoError(t, err)
			require.Equal(t, tc.wantNewStatus, resp.StatusCode)
			require.Equal(t, tc.wantClosed, body.closed)
		})
	}
}

func TestWrap_ResponseReplacesFailedCall(t *testing.T) {
	t.Parallel()

	injected := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	gen := generator{response: func(_ string, _ *httpfaults.Request, resp *http.Response, callErr error) (*http.Response, error) {
		require.Nil(t, resp)
		require.Error(t, callErr)
		return injected, nil
	}}
	wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("call")
	})

	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
}

// An error fails the call, whether or not the fault also returns a response.
func TestWrap_ErrorWinsOverResponse(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	replacement := httpfaults.NewResponse(http.StatusServiceUnavailable, "")
	t.Run("request", func(t *testing.T) {
		t.Parallel()
		wrapped := httpfaults.Wrap(
			generator{request: func(string, *httpfaults.Request) (*http.Response, error) { return replacement, injectedErr }},
			httpfaults.Scope{},
			func(*http.Request) (*http.Response, error) {
				require.FailNow(t, "HTTP call should not run")
				return nil, nil
			})

		resp, err := wrapped(newRequest(t))

		require.Nil(t, resp)
		require.ErrorIs(t, err, injectedErr)
	})
	t.Run("response", func(t *testing.T) {
		t.Parallel()
		body := &trackingBody{Reader: strings.NewReader("original")}
		wrapped := httpfaults.Wrap(
			generator{response: func(string, *httpfaults.Request, *http.Response, error) (*http.Response, error) {
				return replacement, injectedErr
			}},
			httpfaults.Scope{},
			func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
			})

		resp, err := wrapped(newRequest(t))

		require.Nil(t, resp)
		require.ErrorIs(t, err, injectedErr)
		require.True(t, body.closed)
	})
}

func TestWrap_ResponseFaultIncludesCloseError(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	closeErr := errors.New("close")
	body := &trackingBody{closeErr: closeErr}
	gen := generator{response: func(string, *httpfaults.Request, *http.Response, error) (*http.Response, error) {
		return nil, injectedErr
	}}
	wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return &http.Response{Body: body}, nil
	})

	resp, err := wrapper(newRequest(t))

	require.Nil(t, resp)
	require.ErrorIs(t, err, injectedErr)
	require.ErrorIs(t, err, closeErr)
	require.True(t, body.closed)
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
