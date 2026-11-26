package failpointgrpc

import (
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
)

// NewServerWithListLimit is NewServer with a limit, in bytes, on the response of ListFailpoints.
func NewServerWithListLimit(registry *failpoint.Registry, limit int, opts ...ServerOption) failpointpb.FailpointServiceServer {
	s := NewServer(registry, opts...).(*server)
	s.listLimit = limit
	return s
}
