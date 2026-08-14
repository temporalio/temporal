package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/telemetry"
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

var errTestBodyRead = errors.New("body read failed")

func TestNewHTTPClientTransport(t *testing.T) {
	t.Run("Disabled", func(t *testing.T) {
		t.Parallel()

		rt := http.DefaultTransport
		require.Same(t, rt, telemetry.NewHTTPClientTransport(rt, nil, nil))
	})

	t.Run("InjectsTraceContext", func(t *testing.T) {
		t.Parallel()

		recorder := tracetest.NewSpanRecorder()
		tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))

		var traceparent string
		rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			traceparent = r.Header.Get("traceparent")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
				Header:     http.Header{},
				Request:    r,
			}, nil
		})

		wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
		resp, err := wrapped.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		require.NotEmpty(t, traceparent)
		require.NotEmpty(t, recorder.Ended())
	})

	t.Run("DebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

		t.Run("AnnotatesHeadersAndPayloads", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(
				trace.WithSpanProcessor(recorder),
				trace.WithIDGenerator(fixedIDGenerator{}),
			)

			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, "request body", string(payload))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString("response body")),
					Header: http.Header{
						"Response-Header": []string{"response-value"},
					},
					Request: r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
			req.Header.Set("Request-Header", "request-value")

			resp, err := wrapped.RoundTrip(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "response body", string(body))
			require.NoError(t, resp.Body.Close())

			require.Equal(t, map[string]any{
				"http.request.headers.request-header":   []string{"request-value"},
				"http.request.headers.traceparent":      []string{"00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01"},
				"http.request.method":                   "POST",
				"http.request.payload":                  "request body",
				"http.response.headers.response-header": []string{"response-value"},
				"http.response.payload":                 "response body",
				"http.response.status_code":             int64(http.StatusOK),
				"network.protocol.version":              "1.1",
				"server.address":                        "example.com",
				"url.full":                              "http://example.com",
			}, spanAttrsByKey(recorder.Ended()[0].Attributes()))
		})

		t.Run("PreservesReadWriteCloserResponseBodies", func(t *testing.T) {
			tp := trace.NewTracerProvider()
			body := &readWriteCloser{Reader: bytes.NewBufferString("server message")}
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusSwitchingProtocols,
					Body:       body,
					Header:     http.Header{},
					Request:    r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			resp, err := wrapped.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			responseBody, ok := resp.Body.(io.ReadWriteCloser)
			require.True(t, ok)
			_, err = responseBody.Write([]byte("client message"))
			require.NoError(t, err)
			require.Equal(t, "client message", body.written.String())
			require.NoError(t, responseBody.Close())
		})

		t.Run("AnnotatesChunkedResponsePayloadOnClose", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          &readTrackingCloser{Reader: bytes.NewBufferString(`{"ok":true}`)},
					ContentLength: -1,
					Header:        http.Header{},
					Request:       r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			resp, err := wrapped.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			var decoded map[string]bool
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
			require.NoError(t, resp.Body.Close())

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, `{"ok":true}`, attrs["http.response.payload"])
		})

		t.Run("DoesNotReadResponseBodyBeforeCaller", func(t *testing.T) {
			tp := trace.NewTracerProvider()
			body := &readTrackingCloser{Reader: bytes.NewBufferString("response body")}
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
					Header:     http.Header{},
					Request:    r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			resp, err := wrapped.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			require.False(t, body.read)

			_, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
		})

		t.Run("AnnotatesFixedLengthPayloadsWithoutEOF", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				requestPayload := make([]byte, r.ContentLength)
				_, err := io.ReadFull(r.Body, requestPayload)
				require.NoError(t, err)
				return &http.Response{
					StatusCode:    http.StatusOK,
					Body:          io.NopCloser(bytes.NewBufferString("response body")),
					ContentLength: int64(len("response body")),
					Header:        http.Header{},
					Request:       r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
			resp, err := wrapped.RoundTrip(req)
			require.NoError(t, err)
			responsePayload := make([]byte, resp.ContentLength)
			_, err = io.ReadFull(resp.Body, responsePayload)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, "request body", attrs["http.request.payload"])
			require.Equal(t, "response body", attrs["http.response.payload"])
		})

		t.Run("PreservesRequestBodyReadErrors", func(t *testing.T) {
			tp := trace.NewTracerProvider()
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				_, err := io.ReadAll(r.Body)
				return nil, err
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = &failingReadCloser{payload: []byte("partial request")}
			_, err := wrapped.RoundTrip(req)
			require.ErrorIs(t, err, errTestBodyRead)
		})

		t.Run("PreservesResponseBodyReadErrors", func(t *testing.T) {
			tp := trace.NewTracerProvider()
			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       &failingReadCloser{payload: []byte("partial response")},
					Header:     http.Header{},
					Request:    r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			resp, err := wrapped.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, err)
			_, err = io.ReadAll(resp.Body)
			require.ErrorIs(t, err, errTestBodyRead)
			require.NoError(t, resp.Body.Close())
		})
	})
}

func TestNewHTTPHandler(t *testing.T) {
	t.Run("SkipsHeadersAndPayloadsByDefault", func(t *testing.T) {
		t.Parallel()

		recorder := tracetest.NewSpanRecorder()
		tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
		var handlerErr error
		handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, handlerErr = io.ReadAll(r.Body)
			if handlerErr != nil {
				return
			}
			w.Header().Set("Response-Header", "response-value")
			_, handlerErr = w.Write([]byte("response body"))
		}), "test-handler", tp, nil)

		req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
		req.Header.Set("Request-Header", "request-value")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.NoError(t, handlerErr)
		require.Equal(t, "response body", rec.Body.String())

		attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
		require.NotContains(t, attrs, "http.request.payload")
		require.NotContains(t, attrs, "http.response.payload")
		require.NotContains(t, attrs, "http.request.headers.request-header")
		require.NotContains(t, attrs, "http.response.headers.response-header")
	})

	t.Run("DebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

		t.Run("AnnotatesHeadersAndPayloads", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("ReadAll() error = %v", err)
				}
				if string(payload) != "request body" {
					t.Errorf("payload = %q, want %q", string(payload), "request body")
				}
				w.Header().Set("Response-Header", "response-value")
				_, err = w.Write([]byte("response body"))
				if err != nil {
					t.Errorf("Write() error = %v", err)
				}
			}), "test-handler", tp, nil)

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
			req.Header.Set("Request-Header", "request-value")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, "response body", rec.Body.String())

			require.Equal(t, map[string]any{
				"client.address":                        "192.0.2.1",
				"http.request.body.size":                int64(len("request body")),
				"http.request.headers.request-header":   []string{"request-value"},
				"http.request.method":                   "POST",
				"http.request.payload":                  "request body",
				"http.response.body.size":               int64(len("response body")),
				"http.response.headers.response-header": []string{"response-value"},
				"http.response.payload":                 "response body",
				"http.response.status_code":             int64(http.StatusOK),
				"network.peer.address":                  "192.0.2.1",
				"network.peer.port":                     int64(1234),
				"network.protocol.version":              "1.1",
				"server.address":                        "example.com",
				"url.scheme":                            "http",
			}, spanAttrsByKey(recorder.Ended()[0].Attributes()))
		})

		t.Run("DoesNotReadRequestBodyBeforeHandler", func(t *testing.T) {
			tp := trace.NewTracerProvider()
			body := &readTrackingCloser{Reader: bytes.NewBufferString("request body")}
			var readBeforeHandler bool
			var handlerErr error
			var handlerPayload []byte
			handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				readBeforeHandler = body.read
				handlerPayload, handlerErr = io.ReadAll(r.Body)
			}), "test-handler", tp, nil)

			req := httptest.NewRequest(http.MethodPost, "http://example.com", nil)
			req.Body = body
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.False(t, readBeforeHandler)
			require.NoError(t, handlerErr)
			require.Equal(t, "request body", string(handlerPayload))
		})

		t.Run("AnnotatesChunkedRequestPayloadWithoutEOF", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			var handlerErr error
			handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload := make([]byte, len("request body"))
				_, handlerErr = io.ReadFull(r.Body, payload)
			}), "test-handler", tp, nil)

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
			req.ContentLength = -1
			req.TransferEncoding = []string{"chunked"}
			handler.ServeHTTP(httptest.NewRecorder(), req)
			require.NoError(t, handlerErr)

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, "request body", attrs["http.request.payload"])
		})

		t.Run("AnnotatesResponseSizeWhenUsingReadFrom", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			var handlerErr error
			handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, handlerErr = io.Copy(w, io.LimitReader(bytes.NewBufferString("response body"), int64(len("response body"))))
			}), "test-handler", tp, nil)

			rec := &readerFromResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://example.com", nil))
			require.NoError(t, handlerErr)
			require.Equal(t, "response body", rec.Body.String())

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, int64(len("response body")), attrs["http.response.body.size"])
		})

		t.Run("AnnotatesLargePayloads", func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
			payload := bytes.Repeat([]byte("a"), 2*1024*1024+1)
			var handlerErr error
			handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, handlerErr = io.Copy(w, r.Body)
			}), "test-handler", tp, nil)

			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader(payload))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.NoError(t, handlerErr)
			require.Equal(t, payload, rec.Body.Bytes())

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, string(payload), attrs["http.request.payload"])
			require.Equal(t, string(payload), attrs["http.response.payload"])
		})
	})
}

func TestHTTP2Instrumentation(t *testing.T) {
	t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

	type handlerResult struct {
		payload string
		err     error
	}

	recorder := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	resultCh := make(chan handlerResult, 1)
	handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err == nil {
			_, err = w.Write([]byte("response body"))
		}
		resultCh <- handlerResult{payload: string(payload), err: err}
	}), "test-handler", tp, nil)

	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	client := server.Client()
	client.Transport = telemetry.NewHTTPClientTransport(client.Transport, tp, nil)
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
	for _, span := range recorder.Ended() {
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

	clientAttrs := spanAttrsByKey(clientSpan.Attributes())
	require.Equal(t, "request body", clientAttrs["http.request.payload"])
	require.Equal(t, "response body", clientAttrs["http.response.payload"])
	serverAttrs := spanAttrsByKey(serverSpan.Attributes())
	require.Equal(t, "2.0", serverAttrs["network.protocol.version"])
	require.Equal(t, "request body", serverAttrs["http.request.payload"])
	require.Equal(t, "response body", serverAttrs["http.response.payload"])
}

func spanAttrsByKey(attrs []attribute.KeyValue) map[string]any {
	attrsByKey := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		attrsByKey[string(attr.Key)] = attr.Value.AsInterface()
	}
	return attrsByKey
}
