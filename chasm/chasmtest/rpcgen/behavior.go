package rpcgen

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/server/chasm/chasmtest/rpctest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

// Behavior is a replayable outbound unary RPC outcome. Profiles choose which
// behavior is legal for a method; this package does not infer that from a
// descriptor.
type Behavior[Request, Response proto.Message] struct {
	Label     string
	Response  Response
	Err       error
	Delay     time.Duration
	Committed bool
	respond   func(context.Context, Request) (Response, error)
}

// Expect installs one explicit, typed matcher-based expectation. Scripts have
// no fallback behavior: every outbound call in a property must be intentional.
func (b Behavior[Request, Response]) Expect(
	contract *rpctest.RPCContract,
	method string,
	describe string,
	match func(Request) bool,
) {
	if match == nil {
		match = func(Request) bool { return true }
	}
	contract.ExpectContext(method, fmt.Sprintf("%s: %s", b.Label, describe), func(_ context.Context, request any) bool {
		typed, ok := request.(Request)
		return ok && match(typed)
	}, rpctest.RespondType(func(ctx context.Context, request Request) (Response, error) {
		return b.respondTo(ctx, request)
	}))
}

func (b Behavior[Request, Response]) respondTo(ctx context.Context, request Request) (Response, error) {
	if b.Delay > 0 {
		timer := time.NewTimer(b.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			var response Response
			return response, ctx.Err()
		case <-timer.C:
		}
	}
	if b.respond != nil {
		return b.respond(ctx, request)
	}
	return b.Response, b.Err
}

// Derived returns a successful behavior derived from the real outbound request.
func Derived[Request, Response proto.Message](label string, response func(Request) Response) Behavior[Request, Response] {
	return Behavior[Request, Response]{
		Label: label,
		respond: func(_ context.Context, request Request) (Response, error) {
			return response(request), nil
		},
	}
}

func Success[Request, Response proto.Message](response Response) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "success", Response: response}
}

func Retryable[Request, Response proto.Message](code codes.Code) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "retryable-" + code.String(), Err: status.Error(code, "injected retryable failure")}
}

func Terminal[Request, Response proto.Message](code codes.Code) Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "terminal-" + code.String(), Err: status.Error(code, "injected terminal failure")}
}

func Timeout[Request, Response proto.Message]() Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "timeout", Delay: time.Hour}
}

func Cancellation[Request, Response proto.Message]() Behavior[Request, Response] {
	return Behavior[Request, Response]{Label: "cancellation", Err: context.Canceled}
}

func AmbiguousCommit[Request, Response proto.Message](response Response) Behavior[Request, Response] {
	return Behavior[Request, Response]{
		Label:     "ambiguous-commit",
		Response:  response,
		Err:       status.Error(codes.Unavailable, "response lost after commit"),
		Committed: true,
	}
}

// Draw selects a behavior on the property-test goroutine, retaining the choice
// in Rapid's replay trace.
func Draw[Request, Response proto.Message](t *rapid.T, label string, behaviors ...Behavior[Request, Response]) Behavior[Request, Response] {
	if len(behaviors) == 0 {
		t.Fatal("rpcgen: Draw requires at least one behavior")
	}
	return behaviors[rapid.IntRange(0, len(behaviors)-1).Draw(t, label)]
}
