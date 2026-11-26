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

// Generator checks for faults before and after a handler runs. A check has three outcomes: it
// returns an error to fail the call, a response to answer the call with that response and no
// error, or neither to leave the call as it is.
type Generator[Req, Resp any] interface {
	// GenerateRequest returns the error to fail the call with before the handler runs, or the
	// response to return without running the handler, or neither (a nil response and error) to run
	// the handler. An error wins over a response.
	GenerateRequest(ctx context.Context, operation string, req Req) (Resp, error)
	// GenerateResponse returns the error that replaces the result of the handler, or the response
	// that replaces it without an error, or neither (a nil response and error) to keep the result.
	// An error wins over a response. Returning resp itself keeps the response, without an error.
	GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) (Resp, error)
}

// Scope identifies a namespace by ID, name, or both. An empty scope applies globally.
type Scope struct {
	NamespaceID   namespace.ID
	NamespaceName namespace.Name
}

// ScopeFromRequest returns the namespace scope from req. It has the ID and the name of the
// namespace if the request carries both, such as the ListTaskQueuePartitions request that the
// frontend sends to matching, so a failpoint of either can fire for it, the ID first.
func ScopeFromRequest(req any) Scope {
	if request, ok := req.(faultScopeGetter); ok {
		return request.FaultScope()
	}
	return Scope{
		NamespaceID:   namespace.ID(namespaceIDFromRequest(req)),
		NamespaceName: namespace.Name(namespaceNameFromRequest(req)),
	}
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
