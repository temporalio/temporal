package grpcfaultstest

import (
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator creates a Generator that executes the gRPC failpoints of source, scoped to the
// namespace ID of the request, for the gRPC server of service. namespaces resolves the ID of a
// request that only carries a namespace name, such as most frontend requests; it can be nil. It
// returns nil if failpoints are not enabled, so no fault interceptor is installed. The service
// parameter lets a fallback of the registry, such as BUGGIFY, decide per service where it may inject errors.
func NewGenerator(source *failpoints.Source, namespaces faultinjectiontest.NamespaceIDs, _ primitives.ServiceName) grpcfaults.Generator {
	if !source.Enabled() {
		return nil
	}
	return faultinjectiontest.NewAdapter[any, any](source, faultinjectiontest.Keys{
		Request:     failpoint.GRPCKey,
		Response:    failpoint.GRPCResponseKey,
		AnyRequest:  failpoint.AnyGRPCKey,
		AnyResponse: failpoint.AnyGRPCResponseKey,
	}, namespaces)
}
