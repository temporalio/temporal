// Package faultinjection defines the seam that injects request and response faults for multiple
// transports.
package faultinjection

import (
	"context"

	"go.temporal.io/server/common/namespace"
)

type (
	faultScopeGetter interface {
		FaultScope() Scope
	}

	namespaceNameGetter interface {
		GetNamespace() string
	}

	namespaceIDGetter interface {
		GetNamespaceId() string
	}
)

// Generator checks for faults before and after a handler runs.
type Generator[Req, Resp any] interface {
	// GenerateRequest returns the error to fail the call with before the handler runs, or nil to
	// run the handler.
	GenerateRequest(ctx context.Context, operation string, req Req) error
	// GenerateResponse returns the error that replaces the result of the handler, or nil to keep
	// the result.
	GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) error
}

// Result is the result of a handler that a response fault checks.
type Result[Req, Resp any] struct {
	Operation string
	Request   Req
	Response  Resp
	Error     error
}

// Scope identifies a namespace by ID, name, or both. An empty scope applies globally.
type Scope struct {
	NamespaceID   namespace.ID
	NamespaceName namespace.Name
}

// ScopeFromRequest returns the namespace scope from req.
func ScopeFromRequest(req any) Scope {
	if request, ok := req.(faultScopeGetter); ok {
		return request.FaultScope()
	}
	if namespaceID := namespaceIDFromRequest(req); namespaceID != "" {
		return Scope{NamespaceID: namespace.ID(namespaceID)}
	}
	if namespaceName := namespaceNameFromRequest(req); namespaceName != "" {
		return Scope{NamespaceName: namespace.Name(namespaceName)}
	}
	return Scope{}
}

func namespaceIDFromRequest(req any) string {
	if request, ok := req.(namespaceIDGetter); ok {
		return request.GetNamespaceId()
	}
	return ""
}

func namespaceNameFromRequest(req any) string {
	if request, ok := req.(namespaceNameGetter); ok {
		return request.GetNamespace()
	}
	return ""
}
