package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type internalRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f internalRoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type internalReadCloser struct {
	io.Reader
}

func (*internalReadCloser) Close() error { return nil }

// Non-recording client spans should not pay debug payload-capture costs.
func TestDebugHTTPClientSpanTransportSkipsNonRecordingSpan(t *testing.T) {
	t.Parallel()

	requestBody := &internalReadCloser{Reader: strings.NewReader("request body")}
	responseBody := &internalReadCloser{Reader: strings.NewReader("response body")}
	var receivedBody io.ReadCloser
	transport := &debugHTTPClientSpanTransport{rt: internalRoundTripperFunc(func(r *http.Request) (*http.Response, error) {
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
}

// Non-recording server spans should not pay debug payload-capture costs.
func TestDebugHTTPHandlerSkipsNonRecordingSpan(t *testing.T) {
	t.Parallel()

	requestBody := &internalReadCloser{Reader: strings.NewReader("request body")}
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
}
