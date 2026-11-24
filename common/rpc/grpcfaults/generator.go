package grpcfaults

import "go.temporal.io/server/common/rpc/faultinjection"

type (
	// Generator checks for gRPC faults before and after a handler runs.
	Generator = faultinjection.Generator[any, any]
	// Result is the result of a gRPC handler that a response fault checks.
	Result = faultinjection.Result[any, any]
)
