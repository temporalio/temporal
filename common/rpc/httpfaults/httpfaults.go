// Package httpfaults injects faults into HTTP client calls.
package httpfaults

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.temporal.io/server/common/faultinjection"
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
	// Outcome defines the result of a matched HTTP fault.
	Outcome = faultinjection.Outcome[*http.Response]
	// RequestCallback checks a request before the HTTP call.
	RequestCallback = faultinjection.RequestCallback[*Request, *http.Response]
	// ResponseCallback checks a result after the HTTP call.
	ResponseCallback = faultinjection.ResponseCallback[*Request, *http.Response]
	// Generator checks for faults before and after an HTTP call.
	Generator = faultinjection.Generator[*Request, *http.Response]
	// Hooks installs callbacks outside the generator.
	Hooks = faultinjection.Hooks[*Request, *http.Response]
	// CallbackGenerator stores HTTP callbacks in the shared fault registry.
	CallbackGenerator = faultinjection.CallbackGenerator[*Request, *http.Response]
)

// NewCallbackGenerator returns a callback generator.
func NewCallbackGenerator() *CallbackGenerator {
	return faultinjection.NewCallbackGenerator[*Request, *http.Response]()
}

// NewCallbackGeneratorWithHooks returns a callback generator that uses hooks.
func NewCallbackGeneratorWithHooks(hooks Hooks) *CallbackGenerator {
	return faultinjection.NewCallbackGeneratorWithHooks[*Request, *http.Response](hooks)
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

		if outcome := generator.GenerateRequest(req.Context(), operation, wrapped); outcome != nil {
			return outcome.Response, outcome.Error
		}

		resp, err := inner(req)
		if outcome := generator.GenerateResponse(req.Context(), operation, wrapped, resp, err); outcome != nil {
			return applyOutcome(resp, outcome)
		}
		return resp, err
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
