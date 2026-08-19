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

// NewHTTPClientTransport instruments outbound HTTP requests with OpenTelemetry client spans
// and injects trace context using propagator. If propagator is nil, it defaults to W3C Trace Context.
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

// NewHTTPHandler instruments inbound HTTP requests with OpenTelemetry server spans
// and extracts trace context using propagator. If propagator is nil, it defaults to W3C Trace Context.
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

type debugHTTPClientRequestStateKey struct{}

// debugHTTPClientSpanTransport stores response finalization here through the request context.
// debugHTTPClientTransport runs it before otelhttp ends the span when the body closes before EOF.
type debugHTTPClientRequestState struct {
	finalizeResponse func()
}

type debugHTTPClientTransport struct {
	rt http.RoundTripper
}

var _ http.RoundTripper = (*debugHTTPClientTransport)(nil)

func (t *debugHTTPClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	state := &debugHTTPClientRequestState{}
	req = req.WithContext(context.WithValue(req.Context(), debugHTTPClientRequestStateKey{}, state))
	resp, err := t.rt.RoundTrip(req)
	if resp == nil || state.finalizeResponse == nil {
		return resp, err
	}
	resp.Body = newCloseFinalizingReadCloser(resp.Body, state.finalizeResponse)
	return resp, err
}

type debugHTTPSpan struct {
	trace.Span
}

func (s debugHTTPSpan) annotateHeaders(prefix string, headers http.Header) {
	// Debug mode is explicit opt-in for diagnostics, so all header values, including sensitive ones,
	// are recorded verbatim.
	for key, values := range headers {
		s.SetAttributes(attribute.StringSlice(prefix+strings.ToLower(key), values))
	}
}

func (s debugHTTPSpan) payloadAnnotator(key string) func(string) {
	return func(payload string) {
		s.SetAttributes(attribute.String(key, payload))
	}
}

type debugHTTPClientSpanTransport struct {
	rt http.RoundTripper
}

var _ http.RoundTripper = (*debugHTTPClientSpanTransport)(nil)

func (t *debugHTTPClientSpanTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := debugHTTPSpan{Span: trace.SpanFromContext(req.Context())}
	// Non-recording spans discard attributes, so skip the debug-capture work.
	if !span.IsRecording() {
		return t.rt.RoundTrip(req)
	}
	span.annotateHeaders("http.request.headers.", req.Header)
	req.Body, _ = newPayloadCapturingReadCloser(
		req.Body,
		req.ContentLength,
		span.payloadAnnotator("http.request.payload"),
	)

	resp, err := t.rt.RoundTrip(req)
	if resp == nil {
		return resp, err
	}

	span.annotateHeaders("http.response.headers.", resp.Header)
	var responseCapture *payloadCapture
	resp.Body, responseCapture = newPayloadCapturingReadCloser(
		resp.Body,
		resp.ContentLength,
		span.payloadAnnotator("http.response.payload"),
	)
	if state, ok := req.Context().Value(debugHTTPClientRequestStateKey{}).(*debugHTTPClientRequestState); ok && responseCapture != nil {
		state.finalizeResponse = responseCapture.finalize
	}
	return resp, err
}

type debugHTTPHandler struct {
	handler http.Handler
}

func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span := debugHTTPSpan{Span: trace.SpanFromContext(r.Context())}
	// Non-recording spans discard attributes, so skip the debug-capture work.
	if !span.IsRecording() {
		h.handler.ServeHTTP(w, r)
		return
	}
	span.annotateHeaders("http.request.headers.", r.Header)
	var requestCapture *payloadCapture
	r.Body, requestCapture = newPayloadCapturingReadCloser(
		r.Body,
		r.ContentLength,
		span.payloadAnnotator("http.request.payload"),
	)

	responseBody := payloadCapture{onFinalize: span.payloadAnnotator("http.response.payload")}
	w = newPayloadCapturingResponseWriter(w, &responseBody)

	h.handler.ServeHTTP(w, r)
	// Finalize after the handler returns because an unknown-length request may be fully consumed
	// without a read that returns EOF.
	if requestCapture != nil {
		requestCapture.finalize()
	}

	span.annotateHeaders("http.response.headers.", w.Header())
	responseBody.finalize()
}

// Use httpsnoop to intercept writes without dropping optional ResponseWriter interfaces.
func newPayloadCapturingResponseWriter(w http.ResponseWriter, capture *payloadCapture) http.ResponseWriter {
	return httpsnoop.Wrap(w, httpsnoop.Hooks{
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				n, err := next(p)
				if n > 0 {
					_, _ = capture.Write(p[:n])
				}
				return n, err
			}
		},
		ReadFrom: func(httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				// Prevent io.Copy from using ReaderFrom so bytes pass through Write, where the debug
				// response wrapper captures payloads and otelhttp counts response size.
				return io.Copy(struct{ io.Writer }{w}, src)
			}
		},
	})
}

// Debug mode favors complete diagnostics, so it buffers all observed payload bytes in memory
// without a size limit.
type payloadCapture struct {
	bytes.Buffer
	finalized  bool
	onFinalize func(string)
}

func (c *payloadCapture) finalize() {
	if c.finalized {
		return
	}
	c.finalized = true
	if c.Len() > 0 {
		c.onFinalize(c.String())
	}
}

type payloadCapturingReadCloser struct {
	io.ReadCloser
	capture       payloadCapture
	contentLength int64
}

func newPayloadCapturingReadCloser(
	body io.ReadCloser,
	contentLength int64,
	onFinalize func(string),
) (io.ReadCloser, *payloadCapture) {
	if body == nil || body == http.NoBody {
		return body, nil
	}
	capturingBody := &payloadCapturingReadCloser{
		ReadCloser:    body,
		capture:       payloadCapture{onFinalize: onFinalize},
		contentLength: contentLength,
	}
	return preserveBodyWriter(body, capturingBody), &capturingBody.capture
}

func (r *payloadCapturingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		_, _ = r.capture.Write(p[:n])
	}
	// Finalize after ContentLength bytes because callers may stop without another read that returns EOF.
	if err == io.EOF || r.contentLength > 0 && int64(r.capture.Len()) == r.contentLength {
		r.capture.finalize()
	}
	return n, err
}

func (r *payloadCapturingReadCloser) Close() error {
	r.capture.finalize()
	return r.ReadCloser.Close()
}

type closeFinalizingReadCloser struct {
	io.ReadCloser
	finalize func()
}

func newCloseFinalizingReadCloser(body io.ReadCloser, finalize func()) io.ReadCloser {
	finalizingBody := &closeFinalizingReadCloser{
		ReadCloser: body,
		finalize:   finalize,
	}
	return preserveBodyWriter(body, finalizingBody)
}

func (r *closeFinalizingReadCloser) Close() error {
	r.finalize()
	return r.ReadCloser.Close()
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
