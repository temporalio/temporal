package temporalfs

import "go.temporal.io/api/serviceerror"

var errNotImplemented = serviceerror.NewUnimplemented("TemporalFS operation not yet implemented")
