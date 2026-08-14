package telemetry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type debugHTTPClientTransport struct {
	rt http.RoundTripper
}

type debugHTTPClientSpanTransport struct {
	rt http.RoundTripper
}

type debugHTTPHandler struct {
	handler http.Handler
}

// Debug mode intentionally buffers complete payloads without a size limit.
type payloadCapture struct {
	bytes.Buffer
	finished bool
	onFinish func(string)
}

type payloadCapturingReadCloser struct {
	io.ReadCloser
	capture       payloadCapture
	contentLength int64
}

type closeFinishingReadCloser struct {
	io.ReadCloser
	onClose func()
}

type debugHTTPClientRequestStateKey struct{}

// The request state bridges the inner capture to the outer closer so annotation precedes span completion.
type debugHTTPClientRequestState struct {
	finishResponse func()
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
	isDebug := DebugMode()
	if isDebug {
		if rt == nil {
			rt = http.DefaultTransport
		}
		rt = &debugHTTPClientSpanTransport{rt: rt}
	}
	rt = otelhttp.NewTransport(
		rt,
		otelhttp.WithTracerProvider(tracerProvider),
		otelhttp.WithPropagators(propagator),
	)
	if isDebug {
		rt = &debugHTTPClientTransport{rt: rt}
	}
	return rt
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
	state := &debugHTTPClientRequestState{}
	req = req.WithContext(context.WithValue(req.Context(), debugHTTPClientRequestStateKey{}, state))
	resp, err := t.rt.RoundTrip(req)
	if resp == nil || state.finishResponse == nil {
		return resp, err
	}
	resp.Body = newCloseFinishingReadCloser(resp.Body, state.finishResponse)
	return resp, err
}

func (t *debugHTTPClientSpanTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(req.Context())
	if !span.IsRecording() {
		return t.rt.RoundTrip(req)
	}
	annotateHTTPHeaders(span, "http.request.headers.", req.Header)
	req.Body, _ = newPayloadCapturingReadCloser(req.Body, req.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.request.payload", payload))
	})

	resp, err := t.rt.RoundTrip(req)
	if resp == nil {
		return resp, err
	}

	annotateHTTPHeaders(span, "http.response.headers.", resp.Header)
	var responseCapture *payloadCapture
	resp.Body, responseCapture = newPayloadCapturingReadCloser(resp.Body, resp.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.response.payload", payload))
	})
	if state, ok := req.Context().Value(debugHTTPClientRequestStateKey{}).(*debugHTTPClientRequestState); ok && responseCapture != nil {
		state.finishResponse = responseCapture.finish
	}
	return resp, err
}

func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	if !span.IsRecording() {
		h.handler.ServeHTTP(w, r)
		return
	}
	annotateHTTPHeaders(span, "http.request.headers.", r.Header)
	var requestCapture *payloadCapture
	r.Body, requestCapture = newPayloadCapturingReadCloser(r.Body, r.ContentLength, func(payload string) {
		span.SetAttributes(attribute.String("http.request.payload", payload))
	})

	responseBody := payloadCapture{onFinish: func(payload string) {
		span.SetAttributes(attribute.String("http.response.payload", payload))
	}}
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
		ReadFrom: func(httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				// Hide ReaderFrom so io.Copy uses Write and otelhttp can account for response bytes.
				return io.Copy(struct{ io.Writer }{w}, src)
			}
		},
	})

	h.handler.ServeHTTP(w, r)
	// Handlers can consume an unknown-length body without performing the extra read that returns EOF.
	if requestCapture != nil {
		requestCapture.finish()
	}

	annotateHTTPHeaders(span, "http.response.headers.", w.Header())
	responseBody.finish()
}

func annotateHTTPHeaders(span trace.Span, prefix string, headers http.Header) {
	// Debug mode is explicitly opt-in, so sensitive header values are intentionally recorded verbatim for diagnostics.
	for key, values := range headers {
		span.SetAttributes(attribute.StringSlice(prefix+strings.ToLower(key), values))
	}
}

func newPayloadCapturingReadCloser(
	body io.ReadCloser,
	contentLength int64,
	onFinish func(string),
) (io.ReadCloser, *payloadCapture) {
	if body == nil || body == http.NoBody {
		return body, nil
	}
	capturingBody := &payloadCapturingReadCloser{
		ReadCloser:    body,
		capture:       payloadCapture{onFinish: onFinish},
		contentLength: contentLength,
	}
	return preserveBodyWriter(body, capturingBody), &capturingBody.capture
}

func newCloseFinishingReadCloser(body io.ReadCloser, onClose func()) io.ReadCloser {
	finishingBody := &closeFinishingReadCloser{
		ReadCloser: body,
		onClose:    onClose,
	}
	return preserveBodyWriter(body, finishingBody)
}

func preserveBodyWriter(body io.ReadCloser, wrapped io.ReadCloser) io.ReadCloser {
	if writer, ok := body.(io.Writer); ok {
		return struct {
			io.ReadCloser
			io.Writer
		}{wrapped, writer}
	}
	return wrapped
}

func (r *payloadCapturingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		_, _ = r.capture.Write(p[:n])
	}
	// A final successful read can consume the declared length without returning EOF.
	if err == io.EOF || r.contentLength > 0 && int64(r.capture.Len()) == r.contentLength {
		r.capture.finish()
	}
	return n, err
}

func (r *payloadCapturingReadCloser) Close() error {
	r.capture.finish()
	return r.ReadCloser.Close()
}

func (r *closeFinishingReadCloser) Close() error {
	r.onClose()
	return r.ReadCloser.Close()
}

func (c *payloadCapture) finish() {
	if c.finished {
		return
	}
	c.finished = true
	if c.Len() > 0 {
		c.onFinish(c.String())
	}
}
