package telemetry_test

import (
	"bytes"
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
	"go.temporal.io/server/common/telemetry"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

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
		rt := http.DefaultTransport
		require.Same(t, rt, telemetry.NewHTTPClientTransport(rt, nil, nil))
	})

	t.Run("InjectsTraceContext", func(t *testing.T) {
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
			tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))

			rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, "request body", string(payload))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBufferString("response body")),
					Header: http.Header{
						"Response-Header": []string{"response-value"},
						"Set-Cookie":      []string{"session=secret", "csrf=secret"},
					},
					Request: r,
				}, nil
			})

			wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
			req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewBufferString("request body"))
			req.Header.Set("Request-Header", "request-value")
			req.Header.Set("Authorization", "Bearer secret")
			req.Header.Set("Cookie", "session=secret")
			req.Header.Set("Nexus-Callback-Temporal-Callback-Token", "callback secret")
			req.Header.Set("Proxy-Authorization", "Basic secret")
			req.Header.Set("Temporal-Callback-Token", "callback secret")

			resp, err := wrapped.RoundTrip(req)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, "response body", string(body))
			require.NoError(t, resp.Body.Close())

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, "request body", attrs["http.request.payload"].Value.AsString())
			require.Equal(t, "response body", attrs["http.response.payload"].Value.AsString())
			require.Equal(t, []string{"request-value"}, attrs["http.request.headers.request-header"].Value.AsStringSlice())
			require.Equal(t, []string{"response-value"}, attrs["http.response.headers.response-header"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.request.headers.authorization"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.request.headers.cookie"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.request.headers.nexus-callback-temporal-callback-token"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.request.headers.proxy-authorization"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.request.headers.temporal-callback-token"].Value.AsStringSlice())
			require.Equal(t, []string{"<redacted>"}, attrs["http.response.headers.set-cookie"].Value.AsStringSlice())
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
			require.Equal(t, `{"ok":true}`, attrs["http.response.payload"].Value.AsString())
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
			require.Equal(t, "request body", attrs["http.request.payload"].Value.AsString())
			require.Equal(t, "response body", attrs["http.response.payload"].Value.AsString())
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

			attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
			require.Equal(t, "request body", attrs["http.request.payload"].Value.AsString())
			require.Equal(t, "response body", attrs["http.response.payload"].Value.AsString())
			require.Equal(t, []string{"request-value"}, attrs["http.request.headers.request-header"].Value.AsStringSlice())
			require.Equal(t, []string{"response-value"}, attrs["http.response.headers.response-header"].Value.AsStringSlice())
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

		t.Run("OmitsOversizedPayloads", func(t *testing.T) {
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
			require.NotContains(t, attrs, "http.request.payload")
			require.NotContains(t, attrs, "http.response.payload")
		})
	})
}

func spanAttrsByKey(attrs []attribute.KeyValue) map[string]attribute.KeyValue {
	attrsByKey := make(map[string]attribute.KeyValue, len(attrs))
	for _, attr := range attrs {
		attrsByKey[string(attr.Key)] = attr
	}
	return attrsByKey
}
