// Package failpointadapter runs failpoints at the gRPC and HTTP fault injection points, which only
// run if failpoints are enabled.
package failpointadapter

import (
	"context"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/faultinjection"
	"go.temporal.io/testx/failpoint"
)

// NamespaceIDResolver resolves the ID of a namespace by its name. It reports false if it cannot.
type NamespaceIDResolver func(name namespace.Name) (namespace.ID, bool)

// NamespaceNameResolver resolves the name of a namespace by its ID. It reports false if it cannot.
type NamespaceNameResolver func(id namespace.ID) (namespace.Name, bool)

// Namespaces resolves the namespaces of the requests, in memory, without reading through to
// persistence. The zero value resolves nothing.
type Namespaces struct {
	// ID resolves the ID of a request that only carries a namespace name, such as most frontend
	// requests, so the failpoints of the namespace ID fire for it. It can be nil.
	ID NamespaceIDResolver
	// Name resolves the name of a request that only carries a namespace ID, such as the requests
	// between services, so failpoints scoped to NameScope(name) can fire for it. It can be nil.
	Name NamespaceNameResolver
}

// RegistryNamespaces returns the Namespaces of a namespace registry, which look up the in-memory
// state of the registry only (see namespace.CachedID). A nil registry resolves nothing.
func RegistryNamespaces(registry namespace.Registry) Namespaces {
	if registry == nil {
		return Namespaces{}
	}
	return Namespaces{
		ID:   func(name namespace.Name) (namespace.ID, bool) { return namespace.CachedID(registry, name) },
		Name: func(id namespace.ID) (namespace.Name, bool) { return namespace.CachedName(registry, id) },
	}
}

// Keys are the failpoint keys of a transport boundary.
type Keys struct {
	// Request and Response return the keys of the failpoints that fire before and after the
	// handler of an operation.
	Request  func(operation string) failpoint.Key[error]
	Response func(operation string) failpoint.Key[error]
	// AnyRequest and AnyResponse are the keys of the failpoints that fire before and after the
	// handler of every operation, if the failpoint of the operation did not fire.
	AnyRequest  failpoint.Key[error]
	AnyResponse failpoint.Key[error]
	// PolicyOperation returns the name of the operation that the Policy of the registry, such as
	// BUGGIFY, decides for. It can be coarser than the operation, to keep the number of names that
	// the policy sees bounded when operations contain text that callers choose, such as the path of
	// an HTTP request. Registered failpoints still match the full operation. It can be nil, then
	// the policy decides for the operation.
	PolicyOperation func(operation string) string
	// ExplicitOnly keeps the Policy of the registry, such as BUGGIFY, from injecting errors at this
	// boundary, for example into the requests that a server receives from clients. Registered
	// failpoints still fire.
	ExplicitOnly bool
}

// Adapter executes error failpoints for a transport boundary, scoped to the namespace ID or
// NameScope(name) of the request if it has one. Of the failpoints of one key, the one for the ID
// runs first if registered, then NameScope(name), then unscoped.
//
// Before the handler, it executes the failpoint of the operation with the request as the argument
// of the execution, then the failpoint of any operation, if the first did not fire. An error fails
// the call. After the handler, it does the same with a *faultinjection.Result as the argument. An
// error replaces the result of the handler; nil keeps it. A fault that does not fail the call can
// replace the response instead, by calling Replace on its argument if that is a
// faultinjection.Replacement.
type Adapter[Req, Resp any] struct {
	registry   *failpoint.Registry
	keys       Keys
	namespaces Namespaces
}

// NewAdapter returns an adapter that executes the failpoints for keys in registry.
func NewAdapter[Req, Resp any](registry *failpoint.Registry, keys Keys, namespaces Namespaces) *Adapter[Req, Resp] {
	return &Adapter[Req, Resp]{registry: registry, keys: keys, namespaces: namespaces}
}

// GenerateRequest executes the request failpoints for the operation with req as the argument.
func (a *Adapter[Req, Resp]) GenerateRequest(_ context.Context, operation string, req Req) (Resp, error) {
	if a.registry == nil || !a.registry.MayFire() {
		var none Resp
		return none, nil
	}
	return a.execute(a.registry, operation, false, req, req)
}

// GenerateResponse executes the response failpoints for the operation with a
// *faultinjection.Result of the handler as the argument.
func (a *Adapter[Req, Resp]) GenerateResponse(_ context.Context, operation string, req Req, resp Resp, err error) (Resp, error) {
	if a.registry == nil || !a.registry.MayFire() {
		var none Resp
		return none, nil
	}
	result := &faultinjection.Result[Req, Resp]{Operation: operation, Request: req, Response: resp, Error: err}
	return a.execute(a.registry, operation, true, req, result)
}

// replacer is implemented by the arguments that carry a replacement, see faultinjection.Replacement.
type replacer[Resp any] interface {
	Replaced() Resp
	Drop()
	Discarded() []Resp
}

// discarder is implemented by the requests that take the responses that faults discarded, see
// faultinjection.Replacement.
type discarder[Resp any] interface {
	Discard(responses ...Resp)
}

// execute executes the failpoints of the request or response stage of an operation with arg in
// the registry. It returns the error of the failpoint that fired, else the response that the
// argument carries, if any.
func (a *Adapter[Req, Resp]) execute(registry *failpoint.Registry, operation string, response bool, req Req, arg any) (Resp, error) {
	var none Resp
	keyOf, anyKey := a.keys.Request, a.keys.AnyRequest
	if response {
		keyOf, anyKey = a.keys.Response, a.keys.AnyResponse
	}
	opKey := keyOf(operation)
	scopes, policyScope := a.scopes(req)
	var keyBuf [4]failpoint.Key[error]
	keys := appendScopedKeys(keyBuf[:0], opKey, scopes)
	keys = appendScopedKeys(keys, anyKey, scopes)
	opts := failpoint.ExecuteOptions{
		Arg:          arg,
		ExplicitOnly: a.keys.ExplicitOnly,
	}
	if !a.keys.ExplicitOnly {
		policyOperation := operation
		if a.keys.PolicyOperation != nil {
			policyOperation = a.keys.PolicyOperation(operation)
		}
		opts.PolicyKey = keyOf(policyOperation).Name()
		opts.PolicyScope = policyScope
	}

	err := failpoint.ExecuteInOrder(registry, opts, keys...).Value
	carrier, ok := arg.(replacer[Resp])
	if !ok {
		return none, err
	}
	if err != nil {
		// The error wins: the response that a fault replaced is discarded.
		carrier.Drop()
	}
	if response {
		// The argument of the response stage is the adapter's own, so hand the discarded responses
		// to the request, which the caller of the generator keeps.
		if sink, ok := any(req).(discarder[Resp]); ok {
			sink.Discard(carrier.Discarded()...)
		}
	}
	if err != nil {
		return none, err
	}
	return carrier.Replaced(), nil
}

func appendScopedKeys(dst []failpoint.Key[error], base failpoint.Key[error], scopes []string) []failpoint.Key[error] {
	if len(scopes) == 0 {
		return append(dst, base)
	}
	for _, s := range scopes {
		dst = append(dst, base.ForScope(s))
	}
	return dst
}

// NameScope returns the scope of failpoints targeted by namespace name.
func NameScope(name namespace.Name) string {
	return "name:" + name.String()
}

// scopes returns the failpoint scopes that apply to req in order of precedence (namespace ID
// first, then NameScope(name)), and the scope for the policy (namespace name if known, else ID).
func (a *Adapter[Req, Resp]) scopes(req Req) ([]string, string) {
	ns := faultinjection.ScopeFromRequest(req)
	var out [2]string
	n := 0
	var policyScope string
	switch {
	case ns.NamespaceID != "":
		out[n] = ns.NamespaceID.String()
		n++
		policyScope = ns.NamespaceID.String()
		name := ns.NamespaceName
		if name == "" && a.namespaces.Name != nil {
			if resolved, ok := a.namespaces.Name(ns.NamespaceID); ok {
				name = resolved
			}
		}
		if name != "" {
			out[n] = NameScope(name)
			n++
			policyScope = name.String()
		}
	case ns.NamespaceName != "":
		policyScope = ns.NamespaceName.String()
		if a.namespaces.ID != nil {
			if id, ok := a.namespaces.ID(ns.NamespaceName); ok {
				out[n] = id.String()
				n++
			}
		}
		out[n] = NameScope(ns.NamespaceName)
		n++
	default:
	}
	return out[:n], policyScope
}
