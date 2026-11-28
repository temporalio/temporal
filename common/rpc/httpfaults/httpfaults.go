// Package httpfaults injects faults into HTTP client calls.
package httpfaults

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"go.temporal.io/server/common/rpc/faultinjection"
)

// Scope identifies a namespace for fault matching.
type Scope = faultinjection.Scope

// Request contains an HTTP request and its namespace scope. A fault that answers the call with a
// response instead of sending the request calls Replace on the request.
type Request struct {
	Raw *http.Request
	Scope
	faultinjection.Replacement[*http.Response]
}

func (r *Request) FaultScope() Scope { return r.Scope }

type (
	// Generator checks for faults before and after an HTTP call.
	Generator = faultinjection.Generator[*Request, *http.Response]
	// Result is the result of an HTTP call that a response fault checks.
	Result = faultinjection.Result[*Request, *http.Response]
)

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

		replacement, faultErr := generator.GenerateRequest(req.Context(), operation, wrapped)
		if faultErr != nil {
			closeDiscarded(wrapped, nil, nil)
			return nil, faultErr
		}
		if replacement != nil {
			closeDiscarded(wrapped, replacement, nil)
			return replacement, nil
		}
		closeDiscarded(wrapped, nil, nil)

		resp, err := inner(req)
		replacement, faultErr = generator.GenerateResponse(req.Context(), operation, wrapped, resp, err)
		closeDiscarded(wrapped, replacement, resp)
		return applyFault(resp, err, replacement, faultErr)
	}
}

// closeDiscarded closes the body of each response that the faults of a stage discarded, which the
// caller never gets: a replacement that a later one replaced, or that an error dropped. It closes
// each response once, and leaves alone the replacement that the call returns and the original
// response of the call, which applyFault closes or keeps.
func closeDiscarded(request *Request, replacement, original *http.Response) {
	var closed []*http.Response
	for _, resp := range request.Discarded() {
		if resp == nil || resp == replacement || resp == original || slices.Contains(closed, resp) {
			continue
		}
		closed = append(closed, resp)
		// The call has a result already, so an error of closing a discarded body has no taker.
		_ = closeResponse(resp)
	}
}

// applyFault returns the result of the call with response original and error err, as the fault of
// the call decided: the error of the fault fails the call, a replacement replaces the result without
// an error, and neither keeps it. It closes the original response unless it is kept, or is the
// replacement itself.
func applyFault(original *http.Response, err error, replacement *http.Response, faultErr error) (*http.Response, error) {
	if faultErr != nil {
		return nil, errors.Join(faultErr, closeResponse(original))
	}
	if replacement == nil {
		return original, err
	}
	if replacement != original {
		// The caller gets the replacement without an error, so it closes the replacement's body.
		// An error closing the original would make it drop the replacement and leak its body.
		_ = closeResponse(original)
	}
	return replacement, nil
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
