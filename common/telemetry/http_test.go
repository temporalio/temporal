package telemetry_test

import (
	"bytes"
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

	t.Run("AnnotatesNexusRequest", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))

		rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
				Header:     http.Header{},
				Request:    r,
			}, nil
		})

		wrapped := telemetry.NewHTTPClientTransport(rt, tp, nil)
		req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
		req.Header.Set(telemetry.NexusRequestIDHeader, "request-id")
		telemetry.MarkNexusHTTPRequest(req, "caller-namespace", "target-namespace")

		resp, err := wrapped.RoundTrip(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
		require.True(t, attrs["temporal.nexus.request"].Value.AsBool())
		require.Equal(t, "caller-namespace", attrs["temporal.namespace"].Value.AsString())
		require.Equal(t, "target-namespace", attrs["temporal.nexus.namespace"].Value.AsString())
		require.Equal(t, "request-id", attrs["temporal.nexus.request_id"].Value.AsString())
	})

	t.Run("AnnotatesHeadersAndPayloadsInDebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

		recorder := tracetest.NewSpanRecorder()
		tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))

		rt := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			payload, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, "request body", string(payload))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("response body")),
				Header:     http.Header{"Response-Header": []string{"response-value"}},
				Request:    r,
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

		attrs := spanAttrsByKey(recorder.Ended()[0].Attributes())
		require.Equal(t, "request body", attrs["http.request.payload"].Value.AsString())
		require.Equal(t, "response body", attrs["http.response.payload"].Value.AsString())
		require.Equal(t, []string{"request-value"}, attrs["http.request.headers.request-header"].Value.AsStringSlice())
		require.Equal(t, []string{"response-value"}, attrs["http.response.headers.response-header"].Value.AsStringSlice())
	})
}

func TestNewHTTPHandler(t *testing.T) {
	t.Run("SkipsHeadersAndPayloadsByDefault", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		tp := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
		handler := telemetry.NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll() error = %v", err)
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
		require.NotContains(t, attrs, "http.request.payload")
		require.NotContains(t, attrs, "http.response.payload")
		require.NotContains(t, attrs, "http.request.headers.request-header")
		require.NotContains(t, attrs, "http.response.headers.response-header")
	})

	t.Run("AnnotatesHeadersAndPayloadsInDebugMode", func(t *testing.T) {
		t.Setenv("TEMPORAL_OTEL_DEBUG", "true")

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
}

func spanAttrsByKey(attrs []attribute.KeyValue) map[string]attribute.KeyValue {
	attrsByKey := make(map[string]attribute.KeyValue, len(attrs))
	for _, attr := range attrs {
		attrsByKey[string(attr.Key)] = attr
	}
	return attrsByKey
}
