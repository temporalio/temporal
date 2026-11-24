package httpfaults_test

import (
	"context"
	"errors"
	"fmt"
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

// uncomparableBody is a body whose type == can't compare.
type uncomparableBody struct {
	closed *bool
	_      []byte
}

func (uncomparableBody) Read([]byte) (int, error) { return 0, io.EOF }

func (b uncomparableBody) Close() error {
	*b.closed = true
	return nil
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

// generator returns the errors of its functions; a nil function returns no fault.
type generator struct {
	request  func(operation string, req *httpfaults.Request) error
	response func(operation string, req *httpfaults.Request, resp *http.Response, err error) error
}

func (g generator) GenerateRequest(_ context.Context, operation string, req *httpfaults.Request) error {
	if g.request == nil {
		return nil
	}
	return g.request(operation, req)
}

func (g generator) GenerateResponse(_ context.Context, operation string, req *httpfaults.Request, resp *http.Response, err error) error {
	if g.response == nil {
		return nil
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
	gen := generator{request: func(operation string, req *httpfaults.Request) error {
		require.Equal(t, "POST /cb1", operation)
		require.Equal(t, "/cb1", req.Raw.URL.Path)
		require.Equal(t, scope, req.Scope)
		return injectedErr
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
	gen := generator{request: func(string, *httpfaults.Request) error {
		return fmt.Errorf("wrapped: %w", httpfaults.ResponseOverride{Response: injected})
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
	gen := generator{response: func(operation string, _ *httpfaults.Request, resp *http.Response, callErr error) error {
		require.Equal(t, "POST /cb1", operation)
		require.Same(t, original, resp)
		require.NoError(t, callErr)
		return httpfaults.ResponseOverride{Response: injected}
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
	gen := generator{response: func(_ string, _ *httpfaults.Request, resp *http.Response, _ error) error {
		return httpfaults.ResponseOverride{Response: resp}
	}}

	wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return original, nil
	})
	resp, err := wrapper(newRequest(t))

	require.NoError(t, err)
	require.Same(t, original, resp)
	require.False(t, body.closed)
}

func TestWrap_ResponseOverride(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close")
	// Wrap also accepts the pointer form.
	pointerOverride := error(&httpfaults.ResponseOverride{Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "")})
	tests := []struct {
		name string
		// override returns the fault for the original response of the call.
		override      func(original *http.Response) error
		wantNewStatus int  // the status of the returned response
		wantClosed    bool // the original body was closed
	}{
		{
			name: "pointer form",
			override: func(*http.Response) error {
				return pointerOverride
			},
			wantNewStatus: http.StatusServiceUnavailable,
			wantClosed:    true,
		},
		{
			name: "shallow copy shares the body",
			override: func(original *http.Response) error {
				replacement := *original
				replacement.StatusCode = http.StatusServiceUnavailable
				return httpfaults.ResponseOverride{Response: &replacement}
			},
			wantNewStatus: http.StatusServiceUnavailable,
			wantClosed:    false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := &trackingBody{Reader: strings.NewReader("original"), closeErr: closeErr}
			original := &http.Response{StatusCode: http.StatusOK, Body: body}
			gen := generator{response: func(_ string, _ *httpfaults.Request, resp *http.Response, _ error) error {
				return tc.override(resp)
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

func TestWrap_ResponseOverrideUncomparableBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// replacementBody returns the body of the replacement for the original body.
		replacementBody func(original uncomparableBody) io.ReadCloser
	}{
		{
			name:            "shared body",
			replacementBody: func(original uncomparableBody) io.ReadCloser { return original },
		},
		{
			// == can't tell these apart, so the original is left open rather than risk closing a
			// body the replacement uses.
			name: "other body of the same type",
			replacementBody: func(uncomparableBody) io.ReadCloser {
				return uncomparableBody{closed: new(bool)}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := uncomparableBody{closed: new(bool)}
			original := &http.Response{StatusCode: http.StatusOK, Body: body}
			gen := generator{response: func(_ string, _ *httpfaults.Request, _ *http.Response, _ error) error {
				replacement := httpfaults.NewResponse(http.StatusServiceUnavailable, "")
				replacement.Body = tc.replacementBody(body)
				return httpfaults.ResponseOverride{Response: replacement}
			}}
			wrapper := httpfaults.Wrap(gen, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
				return original, nil
			})

			resp, err := wrapper(newRequest(t))

			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			require.False(t, *body.closed)
		})
	}
}

func TestWrap_ResponseOverrideWithoutResponse(t *testing.T) {
	t.Parallel()

	for _, override := range []error{httpfaults.ResponseOverride{}, &httpfaults.ResponseOverride{}} {
		t.Run(fmt.Sprintf("request/%T", override), func(t *testing.T) {
			t.Parallel()
			wrapped := httpfaults.Wrap(generator{request: func(string, *httpfaults.Request) error { return override }}, httpfaults.Scope{},
				func(*http.Request) (*http.Response, error) {
					require.FailNow(t, "HTTP call should not run")
					return nil, nil
				})

			resp, err := wrapped(newRequest(t))

			require.Nil(t, resp)
			require.ErrorIs(t, err, override)
		})
		t.Run(fmt.Sprintf("response/%T", override), func(t *testing.T) {
			t.Parallel()
			body := &trackingBody{Reader: strings.NewReader("original")}
			wrapped := httpfaults.Wrap(generator{response: func(string, *httpfaults.Request, *http.Response, error) error { return override }}, httpfaults.Scope{},
				func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
				})

			resp, err := wrapped(newRequest(t))

			require.Nil(t, resp)
			require.ErrorIs(t, err, override)
			require.True(t, body.closed)
		})
	}
}

func TestWrap_ResponseFaultIncludesCloseError(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	closeErr := errors.New("close")
	body := &trackingBody{closeErr: closeErr}
	gen := generator{response: func(string, *httpfaults.Request, *http.Response, error) error {
		return injectedErr
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

func TestResponseOverride_Error(t *testing.T) {
	t.Parallel()

	require.Equal(t,
		"httpfaults: response override with status 503 Service Unavailable",
		httpfaults.ResponseOverride{Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "")}.Error(),
	)
	require.Equal(t, "httpfaults: response override without a response", httpfaults.ResponseOverride{}.Error())
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
