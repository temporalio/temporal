// Package httpfaults injects faults into inbound and outbound HTTP calls.
package httpfaults

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/rpc/faults"
)

// Scope identifies a namespace for fault matching.
type Scope = faults.Scope

// Request contains an HTTP request and its namespace scope.
type Request struct {
	Raw *http.Request
	Scope
}

func (r *Request) FaultScope() Scope { return r.Scope }

type (
	// Generators contains inbound and outbound fault hooks.
	Generators = faults.Generators[*Request, *http.Response]
	// Outcome defines the result of a matched HTTP fault.
	Outcome = faults.Outcome[*http.Response]
	// RequestCallback checks a request before the HTTP call.
	RequestCallback = faults.RequestCallback[*Request, *http.Response]
	// ResponseCallback checks a result after the HTTP call.
	ResponseCallback = faults.ResponseCallback[*Request, *http.Response]
	// Generator checks for faults before and after an HTTP call.
	Generator = faults.Generator[*Request, *http.Response]
	// Hooks installs callbacks outside the generator.
	Hooks = faults.Hooks[*Request, *http.Response]
	// CallbackGenerator stores HTTP callbacks in the shared fault registry.
	CallbackGenerator = faults.CallbackGenerator[*Request, *http.Response]
)

// NewCallbackGenerator returns a callback generator.
func NewCallbackGenerator() *CallbackGenerator {
	return faults.NewCallbackGenerator[*Request, *http.Response]()
}

// NewCallbackGeneratorWithHooks returns a callback generator that uses hooks.
func NewCallbackGeneratorWithHooks(hooks Hooks) *CallbackGenerator {
	return faults.NewCallbackGeneratorWithHooks[*Request, *http.Response](hooks)
}

// Wrap applies faults before and after an HTTP call. A nil generator returns inner as is.
func Wrap(
	generator Generator,
	scope Scope,
	inner func(*http.Request) (*http.Response, error),
) func(*http.Request) (*http.Response, error) {
	if generator == nil {
		return inner
	}
	return func(req *http.Request) (*http.Response, error) {
		callScope := scope
		if callScope == (Scope{}) {
			if contextScope, ok := req.Context().Value(scopeKey{}).(Scope); ok {
				callScope = contextScope
			}
		}
		var original *http.Response
		resp, err := faults.Invoke(req.Context(), generator, req.Method+" "+req.URL.Path, &Request{Raw: req, Scope: callScope}, func() (*http.Response, error) {
			var err error
			original, err = inner(req)
			return original, err
		})
		return applyOutcome(original, &Outcome{Response: resp, Error: err})
	}
}

func applyOutcome(original *http.Response, outcome *Outcome) (*http.Response, error) {
	if original == outcome.Response {
		return outcome.Response, outcome.Error
	}
	return outcome.Response, errors.Join(outcome.Error, closeResponse(original))
}

func closeResponse(resp *http.Response) error {
	if resp == nil || resp.Body == nil {
		return nil
	}
	return resp.Body.Close()
}

// NewResponse returns a synthetic HTTP response.
func NewResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func NewConfiguredGenerators(cfg *config.TransportFaultInjection, fallback Generators) (Generators, error) {
	return faults.NewConfiguredGenerators(cfg, configuredOutcome, fallback, func(resp *http.Response, err error) bool {
		return err == nil && resp != nil && resp.StatusCode < http.StatusBadRequest
	})
}

func configuredOutcome(name string) *Outcome {
	var status int
	switch name {
	case "Unavailable":
		status = http.StatusServiceUnavailable
	case "Internal":
		status = http.StatusInternalServerError
	case "ResourceExhausted":
		status = http.StatusTooManyRequests
	default:
		return nil
	}
	return &Outcome{Response: NewResponse(status, "fault injection: "+name)}
}

type scopeKey struct{}

// WithScope carries namespace scope to the HTTP transport without injecting faults twice.
func WithScope(req *http.Request, scope Scope) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), scopeKey{}, scope))
}

// TransportWrapper composes transport instrumentation and fault injection at client construction.
type TransportWrapper func(http.RoundTripper) http.RoundTripper

func (w TransportWrapper) Wrap(transport http.RoundTripper) http.RoundTripper {
	if w == nil {
		return transport
	}
	return w(transport)
}

type transport struct {
	http.RoundTripper
	call func(*http.Request) (*http.Response, error)
}

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) { return t.call(req) }

func (t transport) CloseIdleConnections() {
	if inner, ok := t.RoundTripper.(interface{ CloseIdleConnections() }); ok {
		inner.CloseIdleConnections()
	}
}

// WrapTransport applies faults at the transport, including calls made through http.Client.Do.
func WrapTransport(generator Generator, inner http.RoundTripper) http.RoundTripper {
	if generator == nil {
		return inner
	}
	return transport{RoundTripper: inner, call: Wrap(generator, Scope{}, inner.RoundTrip)}
}

// WrapHandler buffers responses so a response fault can replace the status and body.
// Flushing or hijacking commits a streaming response and prevents response injection.
func WrapHandler(generator Generator, inner http.Handler) http.Handler {
	if generator == nil {
		return inner
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		buffer := &responseBuffer{ResponseWriter: w, header: w.Header().Clone()}
		call := Wrap(generator, Scope{}, func(req *http.Request) (*http.Response, error) {
			writer := buffer.writer()
			inner.ServeHTTP(writer, req)
			if buffer.committed {
				return nil, nil
			}
			buffer.WriteHeader(http.StatusOK)
			return &http.Response{StatusCode: buffer.status, Header: buffer.responseHeader, Body: io.NopCloser(&buffer.body)}, nil
		})
		resp, _ := call(req)
		if buffer.committed {
			return
		}
		if resp == nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		writeResponse(w, resp)
	})
}

type responseBuffer struct {
	http.ResponseWriter
	header         http.Header
	responseHeader http.Header
	body           bytes.Buffer
	status         int
	committed      bool
}

func (b *responseBuffer) writer() http.ResponseWriter {
	// Preserve the underlying writer's optional interfaces while buffering writes.
	return httpsnoop.Wrap(b.ResponseWriter, httpsnoop.Hooks{
		Header:      func(httpsnoop.HeaderFunc) httpsnoop.HeaderFunc { return b.Header },
		WriteHeader: func(httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc { return b.WriteHeader },
		Write:       func(httpsnoop.WriteFunc) httpsnoop.WriteFunc { return b.Write },
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(r io.Reader) (int64, error) {
				if b.committed {
					return next(r)
				}
				b.WriteHeader(http.StatusOK)
				return b.body.ReadFrom(r)
			}
		},
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc { return func() { b.commit(); next() } },
		Hijack: func(next httpsnoop.HijackFunc) httpsnoop.HijackFunc {
			return func() (net.Conn, *bufio.ReadWriter, error) { b.committed = true; return next() }
		},
	})
}

func (b *responseBuffer) Header() http.Header {
	if b.committed {
		return b.ResponseWriter.Header()
	}
	return b.header
}

func (b *responseBuffer) WriteHeader(status int) {
	if b.committed {
		return
	}
	if status < 200 {
		for key, values := range b.header {
			b.ResponseWriter.Header()[key] = values
		}
		b.ResponseWriter.WriteHeader(status)
		b.committed = status == http.StatusSwitchingProtocols
		return
	}
	if b.status == 0 {
		b.status = status
		b.responseHeader = b.header.Clone()
	}
}

func (b *responseBuffer) Write(data []byte) (int, error) {
	if b.committed {
		return b.ResponseWriter.Write(data)
	}
	b.WriteHeader(http.StatusOK)
	return b.body.Write(data)
}

func (b *responseBuffer) commit() {
	if b.committed {
		return
	}
	b.WriteHeader(http.StatusOK)
	writeResponse(b.ResponseWriter, &http.Response{StatusCode: b.status, Header: b.responseHeader, Body: io.NopCloser(&b.body)})
	b.committed = true
}

func writeResponse(w http.ResponseWriter, resp *http.Response) {
	// A fault replaces the original response headers along with its status and body.
	clear(w.Header())
	for key, values := range resp.Header {
		w.Header()[key] = values
	}
	w.WriteHeader(resp.StatusCode)
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(w, resp.Body)
	}
}
