package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type readerFromResponseRecorder struct {
	*httptest.ResponseRecorder
}

func (r *readerFromResponseRecorder) ReadFrom(src io.Reader) (int64, error) {
	return io.Copy(r.Body, src)
}

type fixedIDGenerator struct{}

func (fixedIDGenerator) NewIDs(context.Context) (oteltrace.TraceID, oteltrace.SpanID) {
	return oteltrace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
}

func (fixedIDGenerator) NewSpanID(context.Context, oteltrace.TraceID) oteltrace.SpanID {
	return oteltrace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
}

type readTrackingCloser struct {
	io.Reader
	read bool
}

func (r *readTrackingCloser) Read(p []byte) (int, error) {
	r.read = true
	return r.Reader.Read(p)
}

func (r *readTrackingCloser) Close() error { return nil }

type readWriteCloser struct {
	io.Reader
	written bytes.Buffer
}

func (r *readWriteCloser) Write(p []byte) (int, error) { return r.written.Write(p) }

func (r *readWriteCloser) Close() error { return nil }

type failingReadCloser struct {
	payload []byte
	read    bool
}

func (r *failingReadCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, r.payload), errTestBodyRead
}

func (r *failingReadCloser) Close() error { return nil }

type httpTraceEnv struct {
	t              *testing.T
	recorder       *tracetest.SpanRecorder
	tracerProvider *trace.TracerProvider
}

func newHTTPTraceEnv(t *testing.T, options ...trace.TracerProviderOption) *httpTraceEnv {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	options = append(options, trace.WithSpanProcessor(recorder))
	return &httpTraceEnv{
		t:              t,
		recorder:       recorder,
		tracerProvider: trace.NewTracerProvider(options...),
	}
}

func (env *httpTraceEnv) newClientTransport(rt http.RoundTripper) http.RoundTripper {
	return NewHTTPClientTransport(rt, env.tracerProvider, nil)
}

func (env *httpTraceEnv) newHandler(handler http.Handler) http.Handler {
	return NewHTTPHandler(handler, "test-handler", env.tracerProvider, nil)
}

func (env *httpTraceEnv) requireSpans(count int) []trace.ReadOnlySpan {
	env.t.Helper()
	spans := env.recorder.Ended()
	require.Len(env.t, spans, count)
	return spans
}

func (env *httpTraceEnv) spanAttrs() map[string]any {
	env.t.Helper()
	spans := env.requireSpans(1)
	attrs := spans[0].Attributes()
	attrsByKey := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		attrsByKey[string(attr.Key)] = attr.Value.AsInterface()
	}
	return attrsByKey
}

var errTestBodyRead = errors.New("body read failed")

// Verifies client transport construction, propagation, and debug body handling.
func TestNewHTTPClientTransport(t *testing.T) {
	// A nil tracer provider disables instrumentation without changing the transport identity.
	t.Run("Disabled", func(t *testing.T) {
		t.Parallel()

		rt := http.DefaultTransport
		require.Same(t, rt, NewHTTPClientTransport(rt, nil, nil))
	})

	// Debug-only headers and payloads must remain absent unless debug mode is enabled.
	t.Run("SkipsHeadersAndPayloadsByDefault", func(t *testing.T) {
		t.Parallel()

		traceEnv := newHTTPTraceEnv(t)

		var traceparent string
		rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			traceparent = r.Header.Get("traceparent")
			payload, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, "request body", string(payload))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("response body")),
				Header: http.Header{
					"Response-Header": []string{"response-value"},
				},
				Request: r,
			}, nil
		}))

		req := httptest.NewRequest(http.MethodPost, "http://example.com", strings.NewReader("request body"))
		req.Header.Set("Request-Header", "request-value")
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, "response body", string(body))

		require.NotEmpty(t, traceparent)
		attrs := traceEnv.spanAttrs()
		require.NotContains(t, attrs, "http.request.payload")
		require.NotContains(t, attrs, "http.response.payload")
		require.NotContains(t, attrs, "http.request.headers.request-header")
		require.NotContains(t, attrs, "http.response.headers.response-header")
	})

	// Debug mode adds diagnostic HTTP headers and payloads to client spans.
	t.Run("DebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

		// Non-recording client spans should not pay debug payload-capture costs.
		t.Run("SkipsCaptureForNonRecordingSpan", func(t *testing.T) {
			requestBody := &readTrackingCloser{Reader: strings.NewReader("request body")}
			responseBody := &readTrackingCloser{Reader: strings.NewReader("response body")}
			var receivedBody io.ReadCloser
			transport := &debugHTTPClientSpanTransport{rt: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				receivedBody = r.Body
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       responseBody,
					Header:     http.Header{},
					Request:    r,
				}, nil
			})}

			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = requestBody
			_, span := noop.NewTracerProvider().Tracer("test").Start(req.Context(), "test")
			req = req.WithContext(oteltrace.ContextWithSpan(req.Context(), span))

			resp, err := transport.RoundTrip(req)
			require.NoError(t, err)
			require.Same(t, requestBody, receivedBody)
			require.Same(t, responseBody, resp.Body)
			require.NoError(t, resp.Body.Close())
		})

		// Debug spans must contain the headers and payloads needed to diagnose an exchange.
		t.Run("AnnotatesHeadersAndPayloads", func(t *testing.T) {
			const (
				requestPayload  = "request body"
				responsePayload = "response body"
			)

			traceEnv := newHTTPTraceEnv(t, trace.WithIDGenerator(fixedIDGenerator{}))

			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, requestPayload, string(payload))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString(responsePayload)),
					Header: http.Header{
						"Response-Header": []string{"response-value"},
					},
					Request: r,
				}, nil
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString(requestPayload))
			req.Header.Set("Request-Header", "request-value")

			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, responsePayload, string(body))
			require.NoError(t, resp.Body.Close())

			require.Equal(t, map[string]any{
				"http.request.headers.request-header":   []string{"request-value"},
				"http.request.headers.traceparent":      []string{"00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"},
				"http.request.method":                   "POST",
				"http.request.payload":                  requestPayload,
				"http.response.headers.response-header": []string{"response-value"},
				"http.response.payload":                 responsePayload,
				"http.response.status_code":             int64(http.StatusOK),
				"network.protocol.version":              "1.1",
				"server.address":                        "example.com",
				"url.full":                              "http://example.com",
			}, traceEnv.spanAttrs())
		})

		// After an HTTP 101 response, callers use Response.Body to read from and write to the upgraded connection.
		t.Run("PreservesUpgradedConnection", func(t *testing.T) {
			const (
				serverMessage = "server message"
				clientMessage = "client message"
			)

			traceEnv := newHTTPTraceEnv(t)
			upgradedConnection := &readWriteCloser{Reader: bytes.NewBufferString(serverMessage)}
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusSwitchingProtocols, // HTTP 101
					Body:       upgradedConnection,
					Header:     http.Header{},
					Request:    r,
				}, nil
			}))

			resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)

			// The wrapped connection must still receive server messages.
			connection, ok := resp.Body.(io.ReadWriteCloser)
			require.True(t, ok)
			responsePayload, err := io.ReadAll(connection)
			require.NoError(t, err)
			require.Equal(t, serverMessage, string(responsePayload))

			// The wrapped connection must still send client messages.
			written, err := connection.Write([]byte(clientMessage))
			require.NoError(t, err)
			require.Equal(t, len(clientMessage), written)
			require.Equal(t, clientMessage, upgradedConnection.written.String())
			require.NoError(t, connection.Close())
		})

		// Decoders may stop after a complete value without reading a chunked body to EOF.
		t.Run("AnnotatesChunkedResponsePayloadOnClose", func(t *testing.T) {
			traceEnv := newHTTPTraceEnv(t)
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          &readTrackingCloser{Reader: bytes.NewBufferString(`{"ok":true}`)},
					ContentLength: -1,
					Header:        http.Header{},
					Request:       r,
				}, nil
			}))

			resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			var decoded map[string]bool
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
			require.NoError(t, resp.Body.Close())

			attrs := traceEnv.spanAttrs()
			require.Equal(t, `{"ok":true}`, attrs["http.response.payload"])
		})

		// Instrumentation must not consume a streaming response before application code reads it.
		t.Run("DoesNotReadResponseBodyBeforeCaller", func(t *testing.T) {
			traceEnv := newHTTPTraceEnv(t)
			body := &readTrackingCloser{Reader: bytes.NewBufferString("response body")}
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
					Header:     http.Header{},
					Request:    r,
				}, nil
			}))

			resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			require.False(t, body.read)

			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
		})

		// io.ReadFull can consume the declared length without performing the read that returns EOF.
		t.Run("AnnotatesFixedLengthPayloadsWithoutEOF", func(t *testing.T) {
			const (
				requestPayload  = "request body"
				responsePayload = "response body"
			)

			traceEnv := newHTTPTraceEnv(t)
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				requestBody := make([]byte, r.ContentLength)
				requestReadBytes, err := io.ReadFull(r.Body, requestBody)
				require.NoError(t, err)
				require.Equal(t, len(requestPayload), requestReadBytes)
				require.Equal(t, requestPayload, string(requestBody))
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(bytes.NewBufferString(responsePayload)),
					ContentLength: int64(len(responsePayload)),
					Header:        http.Header{},
					Request:       r,
				}, nil
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString(requestPayload))
			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)
			responseBody := make([]byte, resp.ContentLength)
			responseReadBytes, err := io.ReadFull(resp.Body, responseBody)
			require.NoError(t, err)
			require.Equal(t, len(responsePayload), responseReadBytes)
			require.Equal(t, responsePayload, string(responseBody))
			require.NoError(t, resp.Body.Close())

			attrs := traceEnv.spanAttrs()
			require.Equal(t, requestPayload, attrs["http.request.payload"])
			require.Equal(t, responsePayload, attrs["http.response.payload"])
		})

		// Wrapping the request body must not mask errors returned by the original body.
		t.Run("PreservesRequestBodyReadErrors", func(t *testing.T) {
			traceEnv := newHTTPTraceEnv(t)
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				_, err := io.ReadAll(r.Body)
				return nil, err
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = &failingReadCloser{payload: []byte("partial request")}
			_, err := rt.RoundTrip(req)
			require.ErrorIs(t, err, errTestBodyRead)
		})

		// Wrapping the response body must not mask errors returned by the original body.
		t.Run("PreservesResponseBodyReadErrors", func(t *testing.T) {
			traceEnv := newHTTPTraceEnv(t)
			rt := traceEnv.newClientTransport(roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       &failingReadCloser{payload: []byte("partial response")},
					Header:     http.Header{},
					Request:    r,
				}, nil
			}))

			resp, err := rt.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			_, err = io.ReadAll(resp.Body)
			require.ErrorIs(t, err, errTestBodyRead)
			require.NoError(t, resp.Body.Close())
		})
	})
}

// Verifies server handler construction, standard tracing, and debug body handling.
func TestNewHTTPHandler(t *testing.T) {
	// Debug-only headers and payloads must remain absent unless debug mode is enabled.
	t.Run("SkipsHeadersAndPayloadsByDefault", func(t *testing.T) {
		t.Parallel()

		traceEnv := newHTTPTraceEnv(t)
		var handlerErr error
		handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, handlerErr = io.ReadAll(r.Body)
			if handlerErr != nil {
				return
			}
			w.Header().Set("Response-Header", "response-value")
			_, handlerErr = w.Write([]byte("response body"))
		}))

		req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
		req.Header.Set("Request-Header", "request-value")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.NoError(t, handlerErr)
		require.Equal(t, "response body", rec.Body.String())

		attrs := traceEnv.spanAttrs()
		require.NotContains(t, attrs, "http.request.payload")
		require.NotContains(t, attrs, "http.response.payload")
		require.NotContains(t, attrs, "http.request.headers.request-header")
		require.NotContains(t, attrs, "http.response.headers.response-header")
	})

	// Debug mode adds diagnostic HTTP headers and payloads to server spans.
	t.Run("DebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

		// Non-recording server spans should not pay debug payload-capture costs.
		t.Run("SkipsCaptureForNonRecordingSpan", func(t *testing.T) {
			requestBody := &readTrackingCloser{Reader: strings.NewReader("request body")}
			recorder := httptest.NewRecorder()
			var receivedBody io.ReadCloser
			var receivedWriter http.ResponseWriter
			handler := &debugHTTPHandler{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedBody = r.Body
				receivedWriter = w
			})}

			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = requestBody
			_, span := noop.NewTracerProvider().Tracer("test").Start(req.Context(), "test")
			req = req.WithContext(oteltrace.ContextWithSpan(req.Context(), span))
			handler.ServeHTTP(recorder, req)

			require.Same(t, requestBody, receivedBody)
			require.Same(t, recorder, receivedWriter)
		})

		// Debug spans must contain the headers and payloads needed to diagnose an exchange.
		t.Run("AnnotatesHeadersAndPayloads", func(t *testing.T) {
			const (
				requestPayload  = "request body"
				responsePayload = "response body"
			)

			traceEnv := newHTTPTraceEnv(t)
			handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("ReadAll() error = %v", err)
				}
				if string(payload) != requestPayload {
					t.Errorf("payload = %q, want %q", string(payload), requestPayload)
				}
				w.Header().Set("Response-Header", "response-value")
				_, err = w.Write([]byte(responsePayload))
				if err != nil {
					t.Errorf("Write() error = %v", err)
				}
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString(requestPayload))
			req.Header.Set("Request-Header", "request-value")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, responsePayload, rec.Body.String())

			require.Equal(t, map[string]any{
				"client.address":                        "192.0.2.1",
				"http.request.body.size":                int64(len(requestPayload)),
				"http.request.headers.request-header":   []string{"request-value"},
				"http.request.method":                   "POST",
				"http.request.payload":                  requestPayload,
				"http.response.body.size":               int64(len(responsePayload)),
				"http.response.headers.response-header": []string{"response-value"},
				"http.response.payload":                 responsePayload,
				"http.response.status_code":             int64(http.StatusOK),
				"network.peer.address":                  "192.0.2.1",
				"network.peer.port":                     int64(1234),
				"network.protocol.version":              "1.1",
				"server.address":                        "example.com",
				"url.scheme":                            "http",
			}, traceEnv.spanAttrs())
		})

		// Instrumentation must leave request consumption under the application handler's control.
		t.Run("DoesNotReadRequestBodyBeforeHandler", func(t *testing.T) {
			traceEnv := newHTTPTraceEnv(t)
			body := &readTrackingCloser{Reader: bytes.NewBufferString("request body")}
			var readBeforeHandler bool
			var handlerErr error
			var handlerPayload []byte
			handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				readBeforeHandler = body.read
				handlerPayload, handlerErr = io.ReadAll(r.Body)
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = body
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.False(t, readBeforeHandler)
			require.NoError(t, handlerErr)
			require.Equal(t, "request body", string(handlerPayload))
		})

		// Handlers may consume the expected bytes from an unknown-length body without reading EOF.
		t.Run("AnnotatesChunkedRequestPayloadWithoutEOF", func(t *testing.T) {
			const requestPayload = "request body"

			traceEnv := newHTTPTraceEnv(t)
			handlerPayload := make([]byte, len(requestPayload))
			var handlerReadBytes int
			var handlerErr error
			handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerReadBytes, handlerErr = io.ReadFull(r.Body, handlerPayload)
			}))

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString(requestPayload))
			req.ContentLength = -1
			req.TransferEncoding = []string{"chunked"}
			handler.ServeHTTP(httptest.NewRecorder(), req)
			require.NoError(t, handlerErr)
			require.Equal(t, len(requestPayload), handlerReadBytes)
			require.Equal(t, requestPayload, string(handlerPayload))

			attrs := traceEnv.spanAttrs()
			require.Equal(t, requestPayload, attrs["http.request.payload"])
		})

		// io.Copy can use ReaderFrom and bypass the Write hook that tracks standard response size.
		t.Run("AnnotatesResponseSizeWhenUsingReadFrom", func(t *testing.T) {
			const responsePayload = "response body"

			traceEnv := newHTTPTraceEnv(t)
			var handlerErr error
			handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, handlerErr = io.Copy(w, io.LimitReader(bytes.NewBufferString(responsePayload), int64(len(responsePayload))))
			}))

			rec := &readerFromResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, handlerErr)
			require.Equal(t, responsePayload, rec.Body.String())

			attrs := traceEnv.spanAttrs()
			require.Equal(t, int64(len(responsePayload)), attrs["http.response.body.size"])
		})
	})
}

// Verifies end-to-end span propagation and protocol attribution over TLS-negotiated HTTP/2.
func TestHTTP2Instrumentation(t *testing.T) {
	type handlerResult struct {
		payload string
		err     error
	}

	traceEnv := newHTTPTraceEnv(t)
	resultCh := make(chan handlerResult, 1)
	handler := traceEnv.newHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err == nil {
			_, err = w.Write([]byte("response body"))
		}
		resultCh <- handlerResult{payload: string(payload), err: err}
	}))

	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	client := server.Client()
	client.Transport = traceEnv.newClientTransport(client.Transport)
	req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString("request body"))
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	responsePayload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, "request body", result.payload)
	require.Equal(t, "response body", string(responsePayload))
	require.NotNil(t, resp.TLS)
	require.Equal(t, 2, resp.ProtoMajor)

	var clientSpan, serverSpan trace.ReadOnlySpan
	for _, span := range traceEnv.requireSpans(2) {
		switch span.SpanKind() {
		case oteltrace.SpanKindClient:
			clientSpan = span
		case oteltrace.SpanKindServer:
			serverSpan = span
		default:
			continue
		}
	}
	require.NotNil(t, clientSpan)
	require.NotNil(t, serverSpan)
	require.Equal(t, clientSpan.SpanContext().TraceID(), serverSpan.SpanContext().TraceID())
	require.Equal(t, clientSpan.SpanContext().SpanID(), serverSpan.Parent().SpanID())

	require.Contains(t, serverSpan.Attributes(), attribute.String("network.protocol.version", "2.0"))
}
