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

type httpClientTransport struct {
	rt      http.RoundTripper
	isDebug bool
}

type debugHTTPHandler struct {
	handler http.Handler
}

type nexusHTTPRequestKey struct{}

type nexusHTTPRequestAttrs struct {
	namespaceName       string
	targetNamespaceName string
}

// NewHTTPClientTransport wraps an HTTP RoundTripper with otelhttp so outbound requests
// carry TraceContext headers and produce a client span.
func NewHTTPClientTransport(
	rt http.RoundTripper,
	tracerProvider trace.TracerProvider,
	propagator propagation.TextMapPropagator,
) http.RoundTripper {
	if tracerProvider == nil {
		return rt
	}
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}
	return otelhttp.NewTransport(
		&httpClientTransport{
			rt:      rt,
			isDebug: DebugMode(),
		},
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
	if tracerProvider == nil {
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

func (t *httpClientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(req.Context())
	annotateNexusHTTPRequest(span, req)
	if t.isDebug {
		annotateHTTPHeaders(span, "http.request.headers.", req.Header)
		if payload, ok := captureBody(&req.Body, &req.GetBody); ok {
			span.SetAttributes(attribute.String("http.request.payload", string(payload)))
		}
	}

	rt := t.rt
	if rt == nil {
		rt = http.DefaultTransport
	}
	resp, err := rt.RoundTrip(req)
	if resp == nil {
		return resp, err
	}

	if t.isDebug {
		annotateHTTPHeaders(span, "http.response.headers.", resp.Header)
		if payload, ok := captureBody(&resp.Body, nil); ok {
			span.SetAttributes(attribute.String("http.response.payload", string(payload)))
		}
	}
	return resp, err
}

// MarkNexusHTTPRequest annotates the HTTP client span for a Nexus request.
func MarkNexusHTTPRequest(req *http.Request, namespaceName string, targetNamespaceName string) {
	*req = *req.WithContext(context.WithValue(req.Context(), nexusHTTPRequestKey{}, nexusHTTPRequestAttrs{
		namespaceName:       namespaceName,
		targetNamespaceName: targetNamespaceName,
	}))
}

func annotateNexusHTTPRequest(span trace.Span, req *http.Request) {
	nexusAttrs, ok := req.Context().Value(nexusHTTPRequestKey{}).(nexusHTTPRequestAttrs)
	if !ok {
		return
	}
	SetNexusSpanAttributes(span, NexusSpanAttributes{
		Request:             true,
		NamespaceName:       nexusAttrs.namespaceName,
		TargetNamespaceName: nexusAttrs.targetNamespaceName,
		RequestID:           req.Header.Get(NexusRequestIDHeader),
	})
}

func (h *debugHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	span := trace.SpanFromContext(r.Context())
	annotateHTTPHeaders(span, "http.request.headers.", r.Header)
	if payload, ok := captureBody(&r.Body, &r.GetBody); ok {
		span.SetAttributes(attribute.String("http.request.payload", string(payload)))
	}

	var responseBody bytes.Buffer
	w = httpsnoop.Wrap(w, httpsnoop.Hooks{
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				n, err := next(p)
				if n > 0 {
					responseBody.Write(p[:n])
				}
				return n, err
			}
		},
	})

	h.handler.ServeHTTP(w, r)

	annotateHTTPHeaders(span, "http.response.headers.", w.Header())
	if responseBody.Len() > 0 {
		span.SetAttributes(attribute.String("http.response.payload", responseBody.String()))
	}
}

func annotateHTTPHeaders(span trace.Span, prefix string, headers http.Header) {
	for key, values := range headers {
		span.SetAttributes(attribute.StringSlice(prefix+strings.ToLower(key), values))
	}
}

func captureBody(
	body *io.ReadCloser,
	getBody *func() (io.ReadCloser, error),
) ([]byte, bool) {
	if body == nil || *body == nil || *body == http.NoBody {
		return nil, false
	}

	payload, err := io.ReadAll(*body)
	_ = (*body).Close()
	*body = io.NopCloser(bytes.NewReader(payload))
	if getBody != nil {
		*getBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(payload)), nil
		}
	}
	return payload, err == nil
}
