package grpcfaultstest

import (
	"strings"

	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/faultinjectiontest"
	commontesthooks "go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint"
)

// NewGenerator creates a Generator that executes the gRPC failpoints of test hooks, scoped to the
// namespace ID of the request. namespaces resolves the ID of a request that only carries a
// namespace name, such as most frontend requests; it can be nil. It returns nil without test
// hooks, so no fault interceptor is installed.
func NewGenerator(testHooks commontesthooks.TestHooks, namespaces faultinjectiontest.NamespaceIDs) grpcfaults.Generator {
	if !testHooks.Enabled() {
		return nil
	}
	return faultinjectiontest.NewAdapter[any, any](testHooks, faultinjectiontest.Keys{
		Request:     failpoint.GRPCKey,
		Response:    failpoint.GRPCResponseKey,
		AnyRequest:  failpoint.AnyGRPCKey,
		AnyResponse: failpoint.AnyGRPCResponseKey,
		Exempt:      isFailpointService,
	}, namespaces)
}

// FailpointServicePrefix is the prefix of the full method names of the FailpointService. It is a
// constant rather than derived from failpointpb, so that builds without test hooks don't link the
// service.
const FailpointServicePrefix = "/temporal.testx.failpoint.v1.FailpointService/"

// isFailpointService reports whether fullMethod belongs to the FailpointService. Its methods run no
// failpoints, so a failpoint for every method doesn't block the requests that remove it.
func isFailpointService(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, FailpointServicePrefix)
}
