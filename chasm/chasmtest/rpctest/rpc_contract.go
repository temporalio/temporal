// Package rpctest provides concurrency-safe RPC interaction contracts.
// An RPC contract deliberately does not impose a global call order: each invocation
// must match exactly one outstanding expectation.
package rpctest

import (
	"context"
	"fmt"
	"sync"
)

// Matcher decides whether an expectation accepts a request.
type Matcher func(request any) bool

// ContextMatcher decides whether an expectation accepts an RPC context and request.
type ContextMatcher func(context.Context, any) bool

// Responder supplies the contracted RPC outcome. It is always called after the
// contract mutex has been released.
type Responder func(context.Context, any) (any, error)

// Call is one observed invocation, ordered by invocation sequence rather than
// completion order.
type Call struct {
	Sequence uint64
	Method   string
	Request  any
}

// RPCContract matches calls to outstanding expectations and records invocations.
type RPCContract struct {
	mu           sync.Mutex
	expectations []*expectation
	calls        []Call
	nextSequence uint64
}

type expectation struct {
	method   string
	describe string
	match    ContextMatcher
	respond  Responder
	consumed bool
}

// Expect adds an expectation whose matcher does not inspect the RPC context.
func (c *RPCContract) Expect(method, describe string, match Matcher, respond func(any) (any, error)) {
	if match == nil {
		match = func(any) bool { return true }
	}
	if respond == nil {
		respond = func(any) (any, error) { return nil, nil }
	}
	c.ExpectContext(method, describe, func(_ context.Context, request any) bool {
		return match(request)
	}, func(_ context.Context, request any) (any, error) {
		return respond(request)
	})
}

// ExpectContext adds an expectation that may inspect the RPC context.
func (c *RPCContract) ExpectContext(method, describe string, match ContextMatcher, respond Responder) {
	if match == nil {
		match = func(context.Context, any) bool { return true }
	}
	if respond == nil {
		respond = func(context.Context, any) (any, error) { return nil, nil }
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expectations = append(c.expectations, &expectation{
		method:   method,
		describe: describe,
		match:    match,
		respond:  respond,
	})
}

// Invoke records and matches one RPC call. Unmatched and ambiguous calls are
// returned as errors with the complete outstanding-contract diagnostic.
func (c *RPCContract) Invoke(ctx context.Context, method string, request any) (any, error) {
	c.mu.Lock()
	c.nextSequence++
	call := Call{Sequence: c.nextSequence, Method: method, Request: request}
	c.calls = append(c.calls, call)

	var matches []*expectation
	for _, expectation := range c.expectations {
		if !expectation.consumed && expectation.method == method && expectation.match(ctx, request) {
			matches = append(matches, expectation)
		}
	}
	if len(matches) != 1 {
		diagnostic := c.diagnosticLocked(method, len(matches))
		c.mu.Unlock()
		return nil, fmt.Errorf("rpctest: %s", diagnostic)
	}
	selected := matches[0]
	selected.consumed = true
	responder := selected.respond
	c.mu.Unlock()

	return responder(ctx, request)
}

// Calls returns a snapshot of calls in invocation order.
func (c *RPCContract) Calls() []Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Call(nil), c.calls...)
}

// Pending returns the number of expectations that have not been consumed.
// Test harnesses can use this to choose an explicitly configured profile over
// their own deterministic fixture default.
func (c *RPCContract) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	remaining := 0
	for _, expectation := range c.expectations {
		if !expectation.consumed {
			remaining++
		}
	}
	return remaining
}

// Matches reports whether exactly one outstanding expectation accepts the call.
func (c *RPCContract) Matches(ctx context.Context, method string, request any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	matches := 0
	for _, expectation := range c.expectations {
		if !expectation.consumed && expectation.method == method && expectation.match(ctx, request) {
			matches++
		}
	}
	return matches == 1
}

// AssertSatisfied reports every expectation that was not invoked.
func (c *RPCContract) AssertSatisfied() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var remaining []string
	for _, expectation := range c.expectations {
		if !expectation.consumed {
			remaining = append(remaining, expectation.description())
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return fmt.Errorf("rpctest: %d unmatched expectation(s): %v", len(remaining), remaining)
}

func (c *RPCContract) diagnosticLocked(method string, matches int) string {
	var outstanding []string
	for _, expectation := range c.expectations {
		if !expectation.consumed {
			outstanding = append(outstanding, expectation.description())
		}
	}
	if matches == 0 {
		return fmt.Sprintf("unmatched call %q; outstanding expectations: %v", method, outstanding)
	}
	return fmt.Sprintf("ambiguous call %q matched %d expectations; outstanding expectations: %v", method, matches, outstanding)
}

func (e *expectation) description() string {
	if e.describe != "" {
		return fmt.Sprintf("%s (%s)", e.method, e.describe)
	}
	return e.method
}

// MatchType builds a typed matcher without exposing type assertions at each
// scenario call site.
func MatchType[T any](predicate func(T) bool) Matcher {
	return func(request any) bool {
		typed, ok := request.(T)
		return ok && predicate(typed)
	}
}

// RespondType builds a typed responder without exposing type assertions at
// each contract call site. A wrong request type is a test-contract invariant.
func RespondType[Request, Response any](responder func(context.Context, Request) (Response, error)) Responder {
	return func(ctx context.Context, request any) (any, error) {
		typed, ok := request.(Request)
		if !ok {
			return nil, fmt.Errorf("rpctest: responder received %T, expected %T", request, *new(Request))
		}
		return responder(ctx, typed)
	}
}
