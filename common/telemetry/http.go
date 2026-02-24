package telemetry

import (
	"bytes"
	"io"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const maxHTTPPayloadCapture = 2 * 1024 * 1024 // 2MB, matches rpc.MaxNexusAPIRequestBodyBytes

// DebugHTTPMiddleware wraps an http.Handler to capture request/response bodies and headers
// onto the active span, gated behind DebugMode(). This provides parity with the gRPC debug
// payload capture in customServerStatsHandler.
func DebugHTTPMiddleware(next http.Handler) http.Handler {
	if !DebugMode() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())

		// Capture request headers.
		for key, values := range r.Header {
			span.SetAttributes(attribute.StringSlice("http.request.headers."+key, values))
		}

		// Capture request body.
		if r.Body != nil {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPPayloadCapture))
			if err == nil {
				span.SetAttributes(attribute.Key("http.request.payload").String(string(body)))
			}
			// Restore the body so downstream handlers can read it.
			r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		}

		// Wrap the response writer to capture the response body.
		crw := &capturingResponseWriter{
			ResponseWriter: w,
			buf:            &bytes.Buffer{},
		}

		next.ServeHTTP(crw, r)

		// Capture response payload and status code.
		span.SetAttributes(attribute.Key("http.response.payload").String(crw.buf.String()))
		statusCode := crw.statusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		span.SetAttributes(attribute.Int("http.response.status_code", statusCode))
	})
}

// capturingResponseWriter wraps http.ResponseWriter to capture the response body.
type capturingResponseWriter struct {
	http.ResponseWriter
	buf        *bytes.Buffer
	statusCode int
}

func (w *capturingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *capturingResponseWriter) Write(b []byte) (int, error) {
	if w.buf.Len() < maxHTTPPayloadCapture {
		remaining := maxHTTPPayloadCapture - w.buf.Len()
		if len(b) <= remaining {
			w.buf.Write(b)
		} else {
			w.buf.Write(b[:remaining])
		}
	}
	return w.ResponseWriter.Write(b)
}

// Flush implements http.Flusher, delegating to the inner writer if it supports it.
func (w *capturingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap supports the http.ResponseController unwrapping convention.
func (w *capturingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
