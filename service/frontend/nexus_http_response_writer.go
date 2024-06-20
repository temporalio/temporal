// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package frontend

import (
	"net/http"
)

const (
	// The Failure-Source header is used to indicate from where the Nexus failure originated.
	nexusFailureSourceHeaderName = "Temporal-Nexus-Failure-Source"
	// failureSourceWorker indicates the failure originated from outside the server (e.g. bad request or on the Nexus worker).
	failureSourceWorker = "worker"
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
	h := w.writer.Header()
	for key, val := range w.nc.responseHeaders {
		if val != "" {
			h.Set(key, val)
		}
	}

	w.writer.WriteHeader(statusCode)
}
