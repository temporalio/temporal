package httpfaults_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/nexus/nexusrpc"
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

func TestConfiguredHTTPFaults(t *testing.T) {
	t.Parallel()
	for name, status := range map[string]int{
		"Unavailable":       http.StatusServiceUnavailable,
		"Internal":          http.StatusInternalServerError,
		"ResourceExhausted": http.StatusTooManyRequests,
	} {
		for _, stage := range []string{"request", "response"} {
			t.Run(name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				cfg := &config.CallFaultInjection{}
				fault := config.FaultInjectionMethodConfig{Errors: map[string]float64{name: 1}}
				if stage == "request" {
					cfg.Request = fault
				} else {
					cfg.Response = fault
				}
				generator, err := configuredGenerator(cfg, nil)
				require.NoError(t, err)
				body := &trackingBody{}
				called := false
				client := http.Client{Transport: httpfaults.WrapTransport(generator, roundTripperFunc(func(*http.Request) (*http.Response, error) {
					called = true
					return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
				}))}
				caller := client.Do
				req, err := http.NewRequest(http.MethodPost, "http://example.com/any/path", nil)
				require.NoError(t, err)
				resp, err := caller(req)
				require.NoError(t, err)
				require.Equal(t, status, resp.StatusCode)
				require.Equal(t, stage == "response", called)
				require.Equal(t, stage == "response", body.closed)
				data, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(data), name)
				require.NoError(t, resp.Body.Close())
				// Each injected response must have its own consumable body.
				resp, err = caller(req)
				require.NoError(t, err)
				data, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Contains(t, string(data), name)
				require.NoError(t, resp.Body.Close())
			})
		}
	}
}

func TestConfiguredHTTPPreservesCallError(t *testing.T) {
	t.Parallel()
	g, err := configuredGenerator(&config.CallFaultInjection{
		Response: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}},
	}, nil)
	require.NoError(t, err)
	callErr := errors.New("call failed")
	caller := httpfaults.Wrap(g, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
		return nil, callErr
	})
	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	require.NoError(t, err)
	resp, err := caller(req)
	require.ErrorIs(t, err, callErr)
	require.Nil(t, resp)
}

func TestConfiguredHTTPFaultsAreRetryableByNexus(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Unavailable", "Internal", "ResourceExhausted"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			generator, err := configuredGenerator(&config.CallFaultInjection{
				Request: config.FaultInjectionMethodConfig{Errors: map[string]float64{name: 1}},
			}, nil)
			require.NoError(t, err)
			client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
				BaseURL: "http://example.com",
				Service: "service",
				HTTPCaller: httpfaults.Wrap(generator, httpfaults.Scope{}, func(*http.Request) (*http.Response, error) {
					t.Fatal("injected request fault should skip the HTTP call")
					return nil, nil
				}),
			})
			require.NoError(t, err)
			_, err = client.StartOperation(t.Context(), "operation", nil, nexus.StartOperationOptions{})
			var handlerError *nexus.HandlerError
			require.ErrorAs(t, err, &handlerError)
			require.True(t, handlerError.Retryable())
		})
	}
}

func configuredGenerator(cfg *config.CallFaultInjection, fallback httpfaults.Generator) (httpfaults.Generator, error) {
	generators, err := httpfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Inbound: cfg}, httpfaults.Generators{Inbound: fallback})
	return generators.Inbound, err
}

func TestConfiguredHTTPInboundFaults(t *testing.T) {
	t.Parallel()
	for name, code := range map[string]int{"Unavailable": 503, "Internal": 500, "ResourceExhausted": 429} {
		for _, stage := range []string{"request", "response", "disabled", "miss", "handler error", "flush"} {
			t.Run(name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				cfg := &config.CallFaultInjection{}
				fault := config.FaultInjectionMethodConfig{Errors: map[string]float64{name: 1}}
				if stage == "request" {
					cfg.Request = fault
				} else {
					cfg.Response = fault
				}
				if stage == "disabled" {
					cfg.Response.Errors[name] = 0
				}
				if stage == "miss" {
					cfg.Response.Errors[name] = 0.01
					cfg.Response.Seed = 2208
				}
				generators, err := httpfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Inbound: cfg}, httpfaults.Generators{})
				require.NoError(t, err)
				called := false
				handler := httpfaults.WrapHandler(generators.Inbound, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					called = true
					w.Header().Set("Content-Length", "2")
					w.Header().Set("X-Original", "present")
					if stage == "handler error" {
						w.WriteHeader(http.StatusBadRequest)
					}
					_, _ = io.WriteString(w, "ok")
					w.Header().Set("X-Original", "late")
					if stage == "flush" {
						w.(http.Flusher).Flush()
					}
				}))
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, newRequest(t))
				require.Equal(t, stage != "request", called)
				switch stage {
				case "disabled", "miss", "flush":
					require.Equal(t, http.StatusOK, recorder.Code)
					require.Equal(t, "ok", recorder.Body.String())
					require.Equal(t, "present", recorder.Result().Header.Get("X-Original"))
				case "handler error":
					require.Equal(t, http.StatusBadRequest, recorder.Code)
					require.Equal(t, "ok", recorder.Body.String())
				default:
					require.Equal(t, code, recorder.Code)
					require.Equal(t, "fault injection: "+name, recorder.Body.String())
					require.Empty(t, recorder.Header().Get("Content-Length"))
					require.Empty(t, recorder.Header().Get("X-Original"))
				}
			})
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestHTTPTransportScopeAndResponseError(t *testing.T) {
	t.Parallel()
	fallback := httpfaults.NewCallbackGenerator()
	scope := httpfaults.Scope{NamespaceID: "target"}
	count := 0
	fallback.RegisterRequestCallback(scope, func(context.Context, string, *httpfaults.Request) *httpfaults.Outcome { count++; return nil })
	generators, err := httpfaults.NewConfiguredGenerators(&config.TransportFaultInjection{Outbound: &config.CallFaultInjection{
		Response: config.FaultInjectionMethodConfig{Errors: map[string]float64{"Unavailable": 1}},
	}}, httpfaults.Generators{Outbound: fallback})
	require.NoError(t, err)
	fallback.RegisterResponseCallback(scope, func(_ context.Context, _ string, _ *httpfaults.Request, _ *http.Response, err error) *httpfaults.Outcome {
		require.NoError(t, err)
		return nil
	})
	original := httpfaults.NewResponse(http.StatusBadRequest, "original")
	client := http.Client{Transport: httpfaults.WrapTransport(generators.Outbound, roundTripperFunc(func(*http.Request) (*http.Response, error) { return original, nil }))}
	resp, err := client.Do(httpfaults.WithScope(newRequest(t), scope))
	require.NoError(t, err)
	require.Same(t, original, resp)
	require.Equal(t, 1, count)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
	require.NoError(t, resp.Body.Close())
}
