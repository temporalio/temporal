// Package httpfaults injects faults into HTTP client calls.
package httpfaults

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"go.temporal.io/server/common/rpc/faultinjection"
)

// Scope identifies a namespace for fault matching.
type Scope = faultinjection.Scope

// Request contains an HTTP request and its namespace scope.
type Request struct {
	Raw *http.Request
	Scope
}

func (r *Request) FaultScope() Scope { return r.Scope }

type (
	// Generator checks for faults before and after an HTTP call.
	Generator = faultinjection.Generator[*Request, *http.Response]
	// Result is the result of an HTTP call that a response fault checks.
	Result = faultinjection.Result[*Request, *http.Response]
)

// ResponseOverride is a fault that makes the HTTP call return Response without an error, instead of
// failing it. Before the call, the call is not sent; after the call, Response replaces its result.
// Both ResponseOverride and *ResponseOverride work. An override without a Response fails the call
// with the override as its error, since callers expect a response when there is no error.
type ResponseOverride struct {
	Response *http.Response
}

func (o ResponseOverride) Error() string {
	if o.Response == nil {
		return "httpfaults: response override without a response"
	}
	return "httpfaults: response override with status " + o.Response.Status
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
		wrapped := &Request{Raw: req, Scope: scope}
		operation := req.Method + " " + req.URL.Path

		if faultErr := generator.GenerateRequest(req.Context(), operation, wrapped); faultErr != nil {
			if replacement, ok := asResponseOverride(faultErr); ok {
				return replacement, nil
			}
			return nil, faultErr
		}

		resp, err := inner(req)
		if faultErr := generator.GenerateResponse(req.Context(), operation, wrapped, resp, err); faultErr != nil {
			return applyFault(resp, faultErr)
		}
		return resp, err
	}
}

// responseOverride is implemented by ResponseOverride and *ResponseOverride, so that
// asResponseOverride finds both forms with one errors.As.
type responseOverride interface {
	overrideResponse() *http.Response
}

func (o ResponseOverride) overrideResponse() *http.Response { return o.Response }

// asResponseOverride returns the response of a ResponseOverride or *ResponseOverride in err. It
// returns false for an override without a response.
func asResponseOverride(err error) (*http.Response, bool) {
	var override responseOverride
	if !errors.As(err, &override) {
		return nil, false
	}
	resp := override.overrideResponse()
	return resp, resp != nil
}

// applyFault replaces the result of the call whose response is original with the fault. It closes
// the original response, unless the fault overrides the response with one that shares its body.
func applyFault(original *http.Response, faultErr error) (*http.Response, error) {
	replacement, ok := asResponseOverride(faultErr)
	if !ok {
		return nil, errors.Join(faultErr, closeResponse(original))
	}
	if original != nil && !mayShareBody(original.Body, replacement.Body) {
		// The caller gets the replacement without an error, so it closes the replacement's body.
		// An error closing the original would make it drop the replacement and leak its body.
		_ = closeResponse(original)
	}
	return replacement, nil
}

// mayShareBody reports whether a and b may be the same body. Bodies of a type that == can't
// compare are treated as shared, so a body the replacement still uses is never closed.
func mayShareBody(a, b io.ReadCloser) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if t := reflect.TypeOf(a); t == reflect.TypeOf(b) && !t.Comparable() {
		return true
	}
	return a == b
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
