// Package rpctest provides a small, concurrency-safe RPC scenario script.
// It deliberately does not impose a global call order: each invocation must
// match exactly one outstanding expectation.
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

// Responder supplies the scripted RPC outcome. It is always called after the
// script mutex has been released.
type Responder func(context.Context, any) (any, error)

// Call is one observed invocation, ordered by invocation sequence rather than
// completion order.
type Call struct {
	Sequence uint64
	Method   string
	Request  any
}

// Script matches calls to outstanding expectations and records invocations.
type Script struct {
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
func (s *Script) Expect(method, describe string, match Matcher, respond func(any) (any, error)) {
	if match == nil {
		match = func(any) bool { return true }
	}
	if respond == nil {
		respond = func(any) (any, error) { return nil, nil }
	}
	s.ExpectContext(method, describe, func(_ context.Context, request any) bool {
		return match(request)
	}, func(_ context.Context, request any) (any, error) {
		return respond(request)
	})
}

// ExpectContext adds an expectation that may inspect the RPC context.
func (s *Script) ExpectContext(method, describe string, match ContextMatcher, respond Responder) {
	if match == nil {
		match = func(context.Context, any) bool { return true }
	}
	if respond == nil {
		respond = func(context.Context, any) (any, error) { return nil, nil }
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expectations = append(s.expectations, &expectation{
		method:   method,
		describe: describe,
		match:    match,
		respond:  respond,
	})
}

// Invoke records and matches one RPC call. Unmatched and ambiguous calls are
// returned as errors with the complete outstanding-script diagnostic.
func (s *Script) Invoke(ctx context.Context, method string, request any) (any, error) {
	s.mu.Lock()
	s.nextSequence++
	call := Call{Sequence: s.nextSequence, Method: method, Request: request}
	s.calls = append(s.calls, call)

	var matches []*expectation
	for _, expectation := range s.expectations {
		if !expectation.consumed && expectation.method == method && expectation.match(ctx, request) {
			matches = append(matches, expectation)
		}
	}
	if len(matches) != 1 {
		diagnostic := s.diagnosticLocked(method, len(matches))
		s.mu.Unlock()
		return nil, fmt.Errorf("rpctest: %s", diagnostic)
	}
	selected := matches[0]
	selected.consumed = true
	responder := selected.respond
	s.mu.Unlock()

	return responder(ctx, request)
}

// Calls returns a snapshot of calls in invocation order.
func (s *Script) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// AssertExhausted reports every expectation that was not invoked.
func (s *Script) AssertExhausted() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var remaining []string
	for _, expectation := range s.expectations {
		if !expectation.consumed {
			remaining = append(remaining, expectation.description())
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	return fmt.Errorf("rpctest: %d unmatched expectation(s): %v", len(remaining), remaining)
}

func (s *Script) diagnosticLocked(method string, matches int) string {
	var outstanding []string
	for _, expectation := range s.expectations {
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
// each scenario call site. A wrong request type is a test-script invariant.
func RespondType[Request, Response any](responder func(context.Context, Request) (Response, error)) Responder {
	return func(ctx context.Context, request any) (any, error) {
		typed, ok := request.(Request)
		if !ok {
			return nil, fmt.Errorf("rpctest: responder received %T, expected %T", request, *new(Request))
		}
		return responder(ctx, typed)
	}
}
