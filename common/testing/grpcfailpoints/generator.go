package grpcfailpoints

import (
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/server/common/testing/serverfailpoints"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator creates a Generator that executes the gRPC failpoints of source, scoped to the
// namespace ID of the request, for the gRPC server of a service. namespaces resolves the ID of a
// request that only carries a namespace name, such as most frontend requests, and the name of one
// that only carries an ID, for the scope of BUGGIFY; its fields can be nil. It
// returns nil if failpoints are not enabled, so no fault interceptor is installed. With
// explicitOnly, the Policy of the registry, such as BUGGIFY, doesn't inject errors into the
// requests of the service; registered failpoints still fire.
func NewGenerator(source *failpoint.Registry, namespaces failpointadapter.Namespaces, explicitOnly bool) grpcfaults.Generator {
	if source == nil {
		return nil
	}
	return failpointadapter.NewAdapter[any, any](source, failpointadapter.Keys{
		Request:      serverfailpoints.GRPCKey,
		Response:     serverfailpoints.GRPCResponseKey,
		AnyRequest:   serverfailpoints.AnyGRPCKey,
		AnyResponse:  serverfailpoints.AnyGRPCResponseKey,
		ExplicitOnly: explicitOnly,
	}, namespaces)
}
