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
	finishResponse func()
}

type debugHTTPClientTransport struct {
	rt http.RoundTripper
}

var _ http.RoundTripper = (*debugHTTPClientTransport)(nil)

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

type debugHTTPClientSpanTransport struct {
	rt http.RoundTripper
}

var _ http.RoundTripper = (*debugHTTPClientSpanTransport)(nil)

func (t *debugHTTPClientSpanTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(req.Context())
	// Non-recording spans discard attributes, so skip the debug-capture work.
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

type debugHTTPHandler struct {
	handler http.Handler
}

func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	// Non-recording spans discard attributes, so skip the debug-capture work.
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
	w = newPayloadCapturingResponseWriter(w, &responseBody)

	h.handler.ServeHTTP(w, r)
	// Finalize after the handler returns because an unknown-length request may be fully consumed
	// without a read that returns EOF.
	if requestCapture != nil {
		requestCapture.finish()
	}

	annotateHTTPHeaders(span, "http.response.headers.", w.Header())
	responseBody.finish()
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
	finished bool
	onFinish func(string)
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

type payloadCapturingReadCloser struct {
	io.ReadCloser
	capture       payloadCapture
	contentLength int64
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

func (r *payloadCapturingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		_, _ = r.capture.Write(p[:n])
	}
	// Finalize after ContentLength bytes because callers may stop without another read that returns EOF.
	if err == io.EOF || r.contentLength > 0 && int64(r.capture.Len()) == r.contentLength {
		r.capture.finish()
	}
	return n, err
}

func (r *payloadCapturingReadCloser) Close() error {
	r.capture.finish()
	return r.ReadCloser.Close()
}

type closeFinishingReadCloser struct {
	io.ReadCloser
	onClose func()
}

func newCloseFinishingReadCloser(body io.ReadCloser, onClose func()) io.ReadCloser {
	finishingBody := &closeFinishingReadCloser{
		ReadCloser: body,
		onClose:    onClose,
	}
	return preserveBodyWriter(body, finishingBody)
}

func (r *closeFinishingReadCloser) Close() error {
	r.onClose()
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

func annotateHTTPHeaders(span trace.Span, prefix string, headers http.Header) {
	// Debug mode is explicit opt-in for diagnostics, so all header values, including sensitive ones,
	// are recorded verbatim.
	for key, values := range headers {
		span.SetAttributes(attribute.StringSlice(prefix+strings.ToLower(key), values))
	}
}
