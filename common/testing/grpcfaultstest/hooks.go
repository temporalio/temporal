package grpcfaultstest

import (
	"strings"

	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/buggify"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator creates a Generator that executes the gRPC failpoints of source, scoped to the
// namespace ID of the request, for the gRPC server of service. namespaces resolves the ID of a
// request that only carries a namespace name, such as most frontend requests; it can be nil. It
// returns nil if failpoints are not enabled, so no fault interceptor is installed. BUGGIFY only injects
// errors into the requests of services for which buggify.InjectsInboundGRPCErrors reports true.
func NewGenerator(source *failpoints.Source, namespaces faultinjectiontest.NamespaceIDs, service primitives.ServiceName) grpcfaults.Generator {
	if !source.Enabled() {
		return nil
	}
	return faultinjectiontest.NewAdapter[any, any](source, faultinjectiontest.Keys{
		Request:     failpoint.GRPCKey,
		Response:    failpoint.GRPCResponseKey,
		AnyRequest:  failpoint.AnyGRPCKey,
		AnyResponse: failpoint.AnyGRPCResponseKey,
		Exempt:      isFailpointService,
		NoBuggify:   !buggify.InjectsInboundGRPCErrors(service),
	}, namespaces)
}

// FailpointServicePrefix is the prefix of the full method names of the FailpointService. It is a
// constant rather than derived from failpointpb, so that servers don't link the
// service in this package.
const FailpointServicePrefix = "/temporal.testx.failpoint.v1.FailpointService/"

// isFailpointService reports whether fullMethod belongs to the FailpointService. Its methods run no
// failpoints, so a failpoint for every method doesn't block the requests that remove it.
func isFailpointService(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, FailpointServicePrefix)
}
