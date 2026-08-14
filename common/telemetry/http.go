package telemetry

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	maxHTTPDebugPayloadSize = 2 * 1024 * 1024
	redactedHTTPHeaderValue = "<redacted>"
)

type debugHTTPClientTransport struct {
	rt http.RoundTripper
}

type debugHTTPHandler struct {
	handler http.Handler
}

type payloadCapture struct {
	payload  bytes.Buffer
	overflow bool
}

type payloadCapturingReadCloser struct {
	io.ReadCloser
	capture       payloadCapture
	contentLength int64
	readSize      int64
	finished      bool
	onFinish      func(string)
}

// NewHTTPClientTransport wraps an HTTP RoundTripper with otelhttp so outbound requests
// carry TraceContext headers and produce a client span.
func NewHTTPClientTransport(
	rt http.RoundTripper,
	tracerProvider trace.TracerProvider,
	propagator propagation.TextMapPropagator,
) http.RoundTripper {
	if !isEnabled(tracerProvider) {
		return rt
	}
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}
	if DebugMode() {
		if rt == nil {
			rt = http.DefaultTransport
		}
		rt = &debugHTTPClientTransport{rt: rt}
	}
	return otelhttp.NewTransport(
		rt,
		otelhttp.WithTracerProvider(tracerProvider),
		otelhttp.WithPropagators(propagator),
	)
}

// NewHTTPHandler wraps an HTTP handler with otelhttp so inbound requests extract
// TraceContext headers and produce a server span.
func NewHTTPHandler(
	handler http.Handler,
	operation string,
	tracerProvider trace.TracerProvider,
	propagator propagation.TextMapPropagator,
) http.Handler {
	if !isEnabled(tracerProvider) {
		return handler
	}
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}
	if DebugMode() {
		handler = &debugHTTPHandler{handler: handler}
	}
	return otelhttp.NewHandler(
		handler,
		operation,
		otelhttp.WithTracerProvider(tracerProvider),
		otelhttp.WithPropagators(propagator),
	)
}

func (t *debugHTTPClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(req.Context())
	annotateHTTPHeaders(span, "http.request.headers.", req.Header)
	req.Body = newPayloadCapturingReadCloser(req.Body, req.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.request.payload", payload))
	})

	resp, err := t.rt.RoundTrip(req)
	if resp == nil {
		return resp, err
	}

	annotateHTTPHeaders(span, "http.response.headers.", resp.Header)
	resp.Body = newPayloadCapturingReadCloser(resp.Body, resp.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.response.payload", payload))
	})
	return resp, err
}

func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	annotateHTTPHeaders(span, "http.request.headers.", r.Header)
	r.Body = newPayloadCapturingReadCloser(r.Body, r.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.request.payload", payload))
	})

	var responseBody payloadCapture
	w = httpsnoop.Wrap(w, httpsnoop.Hooks{
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				n, err := next(p)
				if n > 0 {
					_, _ = responseBody.Write(p[:n])
				}
				return n, err
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				return next(io.TeeReader(src, &responseBody))
			}
		},
	})

	h.handler.ServeHTTP(w, r)

	annotateHTTPHeaders(span, "http.response.headers.", w.Header())
	if payload, ok := responseBody.Value(); ok {
		span.SetAttributes(attribute.String("http.response.payload", payload))
	}
}

func annotateHTTPHeaders(span trace.Span, prefix string, headers http.Header) {
	for key, values := range headers {
		if isSensitiveHTTPHeader(key) {
			values = []string{redactedHTTPHeaderValue}
		}
		span.SetAttributes(attribute.StringSlice(prefix+strings.ToLower(key), values))
	}
}

func isSensitiveHTTPHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Authorization", "Cookie", "Set-Cookie", "Proxy-Authorization":
		return true
	default:
		return false
	}
}

func newPayloadCapturingReadCloser(body io.ReadCloser, contentLength int64, onFinish func(string)) io.ReadCloser {
	if body == nil || body == http.NoBody {
		return body
	}
	return &payloadCapturingReadCloser{
		ReadCloser:    body,
		contentLength: contentLength,
		onFinish:      onFinish,
	}
}

func (r *payloadCapturingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.capture.Write(p[:n])
		r.readSize += int64(n)
	}
	if err == io.EOF || r.contentLength > 0 && r.readSize == r.contentLength {
		r.finish()
	}
	return n, err
}

func (r *payloadCapturingReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish()
	return err
}

func (r *payloadCapturingReadCloser) finish() {
	if r.finished {
		return
	}
	r.finished = true
	if payload, ok := r.capture.Value(); ok {
		r.onFinish(payload)
	}
}

func (c *payloadCapture) Write(p []byte) (int, error) {
	if c.overflow {
		return len(p), nil
	}
	if c.payload.Len()+len(p) > maxHTTPDebugPayloadSize {
		c.payload.Reset()
		c.overflow = true
		return len(p), nil
	}
	return c.payload.Write(p)
}

func (c *payloadCapture) Value() (string, bool) {
	return c.payload.String(), !c.overflow && c.payload.Len() > 0
}
