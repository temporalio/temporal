package umpire

import (
	"context"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type RPCRegistryStrategyOption func(*rpcRegistryStrategyConfig)

type rpcRegistryStrategyConfig struct {
	methodFilter                 func(string) bool
	recordUnregisteredViolations bool
}

func WithRPCRegistryMethodFilter(filter func(string) bool) RPCRegistryStrategyOption {
	return func(config *rpcRegistryStrategyConfig) {
		config.methodFilter = filter
	}
}

func WithUnregisteredRPCViolations() RPCRegistryStrategyOption {
	return func(config *rpcRegistryStrategyConfig) {
		config.recordUnregisteredViolations = true
	}
}

func RPCRegistryStrategy(u *Umpire, registry *RPCRegistry, options ...RPCRegistryStrategyOption) Strategy {
	var config rpcRegistryStrategyConfig
	for _, option := range options {
		option(&config)
	}
	var currentT atomic.Pointer[T]
	recordIssue := func(t *T, issue *RPCRegistryIssue) {
		if t != nil {
			t.Logf("rpc registry: %s method=%s expected=%s actual=%s", issue.Message, issue.Method, issue.Expected, issue.Actual)
		}
		if u != nil {
			u.Record(issue)
		}
	}
	validateMessage := func(t *T, method string, role string, expected protoreflect.FullName, value any) {
		if expected == "" {
			return
		}
		actual, ok := rpcValueMessageName(value)
		if !ok {
			recordIssue(t, &RPCRegistryIssue{
				Method:   method,
				Message:  role + " is not a proto message",
				Expected: string(expected),
				Actual:   fmt.Sprintf("%T", value),
			})
			return
		}
		if actual != expected {
			recordIssue(t, &RPCRegistryIssue{
				Method:   method,
				Message:  role + " type mismatch",
				Expected: string(expected),
				Actual:   string(actual),
			})
		}
	}
	return Strategy{
		Name: "rpc-registry",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			t := currentT.Load()
			if config.methodFilter != nil && !config.methodFilter(method) {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			spec, ok := registry.Lookup(method)
			if !ok {
				if config.recordUnregisteredViolations {
					recordIssue(t, &RPCRegistryIssue{
						Method:  method,
						Message: "unregistered RPC",
						Actual:  fmt.Sprintf("%T", req),
					})
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			validateMessage(t, method, "request", spec.Request, req)
			if t != nil && !skipRPCFieldAssertions(ctx) {
				spec.assertFields(t, method, "request", req, spec.RequestFields)
			}
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				validateMessage(t, method, "response", spec.Response, reply)
				if t != nil && !skipRPCFieldAssertions(ctx) {
					spec.assertFields(t, method, "response", reply, spec.ResponseFields)
				}
			}
			return err
		},
		SetT: func(t *T) { currentT.Store(t) },
	}
}

type RPCRegistryIssue struct {
	Method   string
	Message  string
	Expected string
	Actual   string
}

func (i *RPCRegistryIssue) Key() string {
	if i == nil {
		return ""
	}
	return i.Method
}

func RPCRegistryRule() SafetyRule {
	return SafetyRule{
		Name: "rpc-registry",
		Check: func(ctx *RuleContext, history []*Record) {
			for _, rec := range history {
				issue, ok := rec.Fact.(*RPCRegistryIssue)
				if !ok {
					continue
				}
				ctx.Violate(issue.Message, map[string]string{
					"method":   issue.Method,
					"expected": issue.Expected,
					"actual":   issue.Actual,
				})
			}
		},
	}
}
