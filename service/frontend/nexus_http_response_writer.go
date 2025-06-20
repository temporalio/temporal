package frontend

import (
	"net/http"
)

// nexusHTTPResponseWriter is a wrapper for http.ResponseWriter that appends headers set on a nexusContext
// before writing any response.
type nexusHTTPResponseWriter struct {
	writer http.ResponseWriter
	nc     *nexusContext
}

func newNexusHTTPResponseWriter(writer http.ResponseWriter, nc *nexusContext) http.ResponseWriter {
	return &nexusHTTPResponseWriter{
		writer: writer,
		nc:     nc,
	}
}

func (w *nexusHTTPResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *nexusHTTPResponseWriter) Write(data []byte) (int, error) {
	return w.writer.Write(data)
}

func (w *nexusHTTPResponseWriter) WriteHeader(statusCode int) {
	w.nc.responseHeadersMutex.Lock()
	headersCopy := make(map[string]string, len(w.nc.responseHeaders))
	for k, v := range w.nc.responseHeaders {
		headersCopy[k] = v
	}
	w.nc.responseHeadersMutex.Unlock()

	h := w.writer.Header()
	for key, val := range headersCopy {
		if val != "" {
			h.Set(key, val)
		}
	}
	w.writer.WriteHeader(statusCode)
}
