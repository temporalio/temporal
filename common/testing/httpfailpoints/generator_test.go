package httpfailpoints_test

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpointadapter"
	_ "go.temporal.io/server/common/testing/failpointerrors"
	"go.temporal.io/server/common/testing/httpfailpoints"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
)

var scope = httpfaults.Scope{NamespaceID: "namespace-id"}

func newRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://example.com/cb1", nil)
	require.NoError(t, err)
	return req
}

func setFailpoint(source *failpoint.Registry, key failpoint.Key[error], configure func(*failpoint.Failpoint[error])) {
	fp := failpoint.NewFailpoint(key)
	configure(fp)
	source.Set(fp)
}

func TestWrap_NoFailpoints(t *testing.T) {
	t.Parallel()

	handlerCalled := false
	next := func(*http.Request) (*http.Response, error) {
		handlerCalled = true
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	wrapped := httpfaults.Wrap(httpfailpoints.NewGenerator(failpoint.NewRegistry(), failpointadapter.Namespaces{}), scope, next)

	resp, err := wrapped(newRequest(t))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, handlerCalled)
}

func TestWrap_BeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	tests := []struct {
		name    string
		key     failpoint.Key[error]
		scope   httpfaults.Scope
		wantErr error
	}{
		{name: "scoped to namespace", key: serverfailpoints.HTTPKey("POST /cb1").ForScope("namespace-id"), scope: scope, wantErr: injectedErr},
		{name: "scoped to other namespace", key: serverfailpoints.HTTPKey("POST /cb1").ForScope("other"), scope: scope, wantErr: nil},
		{name: "unscoped", key: serverfailpoints.HTTPKey("POST /cb1"), scope: scope, wantErr: injectedErr},
		{name: "other operation", key: serverfailpoints.HTTPKey("GET /cb1"), scope: scope, wantErr: nil},
		{name: "any operation", key: serverfailpoints.AnyHTTPKey.ForScope("namespace-id"), scope: scope, wantErr: injectedErr},
		{name: "namespace name only", key: serverfailpoints.AnyHTTPKey.ForScope("namespace-id"), scope: httpfaults.Scope{NamespaceName: "namespace-name"}, wantErr: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			setFailpoint(source, tc.key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWithArg(func(arg any) error {
					req := arg.(*httpfaults.Request)
					require.Equal(t, "/cb1", req.Raw.URL.Path)
					require.Equal(t, tc.scope, req.Scope)
					return injectedErr
				})
			})
			handlerCalls := 0
			next := func(*http.Request) (*http.Response, error) {
				handlerCalls++
				return &http.Response{StatusCode: http.StatusOK}, nil
			}
			wrapped := httpfaults.Wrap(httpfailpoints.NewGenerator(source, failpointadapter.Namespaces{}), tc.scope, next)

			resp, err := wrapped(newRequest(t))

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, resp)
				require.Equal(t, 0, handlerCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, 1, handlerCalls)
		})
	}
}

func TestWrap_ReplacementBeforeHandler(t *testing.T) {
	t.Parallel()

	source := failpoint.NewRegistry()
	injected := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	setFailpoint(source, serverfailpoints.HTTPKey("POST /cb1"), func(fp *failpoint.Failpoint[error]) {
		fp.CallWithArg(func(arg any) error {
			arg.(*httpfaults.Request).Replace(injected)
			return nil
		}, failpoint.Trigger{MaxFires: 1})
	})
	wrapped := httpfaults.Wrap(httpfailpoints.NewGenerator(source, failpointadapter.Namespaces{}), scope, func(*http.Request) (*http.Response, error) {
		require.FailNow(t, "HTTP call should not run")
		return nil, nil
	})

	resp, err := wrapped(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
}

func TestWrap_ReplacementAfterHandler(t *testing.T) {
	t.Parallel()

	for _, key := range []failpoint.Key[error]{
		serverfailpoints.HTTPResponseKey("POST /cb1").ForScope("namespace-id"),
		serverfailpoints.AnyHTTPResponseKey.ForScope("namespace-id"),
	} {
		t.Run(key.Name(), func(t *testing.T) {
			t.Parallel()
			source := failpoint.NewRegistry()
			original := httpfaults.NewResponse(http.StatusOK, "original")
			setFailpoint(source, key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWithArg(func(arg any) error {
					result := arg.(*httpfaults.Result)
					require.Equal(t, "POST /cb1", result.Operation)
					require.Equal(t, scope, result.Request.Scope)
					require.Same(t, original, result.Response)
					require.NoError(t, result.Error)
					result.Replace(httpfaults.NewResponse(http.StatusServiceUnavailable, "injected"))
					return nil
				})
			})
			wrapper := httpfaults.Wrap(httpfailpoints.NewGenerator(source, failpointadapter.Namespaces{}), scope, func(*http.Request) (*http.Response, error) {
				return original, nil
			})

			resp, err := wrapper(newRequest(t))

			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}

// countingBody counts the closes of a response body.
type countingBody struct {
	closes int
}

func (b *countingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *countingBody) Close() error             { b.closes++; return nil }

func newCountedResponse(status int) (*http.Response, *countingBody) {
	body := &countingBody{}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}, body
}

// A response that a fault replaces with another, or that an error drops, is closed once, and the
// response that the call returns is left open.
func TestWrap_DiscardedReplacementsAreClosedOnce(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected")
	for _, tc := range []struct {
		name string
		key  failpoint.Key[error]
		// fault replaces the response with each of the responses, in order.
		fault func(arg any, replacements ...*http.Response)
		err   error
	}{
		{
			name: "two replacements before the handler",
			key:  serverfailpoints.HTTPKey("POST /cb1"),
			fault: func(arg any, r ...*http.Response) {
				for _, resp := range r {
					arg.(*httpfaults.Request).Replace(resp)
				}
			},
		},
		{
			name: "two replacements after the handler",
			key:  serverfailpoints.HTTPResponseKey("POST /cb1"),
			fault: func(arg any, r ...*http.Response) {
				for _, resp := range r {
					arg.(*httpfaults.Result).Replace(resp)
				}
			},
		},
		{
			name: "a replacement and an error before the handler",
			key:  serverfailpoints.HTTPKey("POST /cb1"),
			fault: func(arg any, r ...*http.Response) {
				for _, resp := range r {
					arg.(*httpfaults.Request).Replace(resp)
				}
			},
			err: injected,
		},
		{
			name: "a replacement and an error after the handler",
			key:  serverfailpoints.HTTPResponseKey("POST /cb1"),
			fault: func(arg any, r ...*http.Response) {
				for _, resp := range r {
					arg.(*httpfaults.Result).Replace(resp)
				}
			},
			err: injected,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, firstBody := newCountedResponse(http.StatusBadGateway)
			second, secondBody := newCountedResponse(http.StatusServiceUnavailable)
			original, originalBody := newCountedResponse(http.StatusOK)
			source := failpoint.NewRegistry()
			setFailpoint(source, tc.key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWithArg(func(arg any) error {
					// The first is replaced twice by itself, which still closes it once.
					tc.fault(arg, first, first, second)
					return tc.err
				})
			})
			wrapped := httpfaults.Wrap(httpfailpoints.NewGenerator(source, failpointadapter.Namespaces{}), scope, func(*http.Request) (*http.Response, error) {
				return original, nil
			})

			resp, err := wrapped(newRequest(t))

			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, resp)
				// Every replacement is discarded, the second too.
				require.Equal(t, 1, firstBody.closes)
				require.Equal(t, 1, secondBody.closes)
			} else {
				require.NoError(t, err)
				require.Same(t, second, resp)
				require.Equal(t, 1, firstBody.closes)
				require.Zero(t, secondBody.closes, "the response that the call returns is open")
			}
			// The original is closed once if the handler ran and its response is not returned.
			if tc.key == serverfailpoints.HTTPKey("POST /cb1") {
				require.Zero(t, originalBody.closes, "the handler did not run")
			} else {
				require.Equal(t, 1, originalBody.closes)
			}
		})
	}
}

// TestWrap_BuggifyDecidesPerMethod checks that BUGGIFY sees one name per request method, not one
// per path, while failpoints still match the path.
func TestWrap_BuggifyDecidesPerMethod(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	b := buggify.New(registry)
	var activated []string
	b.OnDecision(func(e buggify.Event) {
		if e.Kind == buggify.EventActivation {
			activated = append(activated, e.Name)
		}
	})
	require.NoError(t, b.Set(buggify.Config{Seed: 1, Keys: []buggify.Key{{Match: "*", ActivationProbability: 1, FireProbability: 1, Error: "Unavailable"}}}))
	injected := errors.New("injected")
	setFailpoint(registry, serverfailpoints.HTTPKey("POST /explicit/1"), func(fp *failpoint.Failpoint[error]) {
		fp.Return(injected)
	})
	wrapped := httpfaults.Wrap(httpfailpoints.NewGenerator(registry, failpointadapter.Namespaces{}), scope, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK}, nil
	})
	post := func(path string) error {
		req, err := http.NewRequest(http.MethodPost, "http://example.com"+path, nil)
		require.NoError(t, err)
		_, err = wrapped(req)
		return err
	}

	for _, path := range []string{"/a/1", "/a/2", "/b"} {
		_, isUnavailable := errors.AsType[*serviceerror.Unavailable](post(path))
		require.True(t, isUnavailable, path)
	}
	require.ErrorIs(t, post("/explicit/1"), injected)
	require.Equal(t, []string{"http:POST"}, activated)
}
