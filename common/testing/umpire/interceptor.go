package umpire

import (
	"context"
)

// FactRecorder records gRPC request facts.
type FactRecorder interface {
	RecordFact(context.Context, any)
}

// FaultInjector injects faults into gRPC calls.
type FaultInjector interface {
	Inject(ctx context.Context, info any, request any) error
}

// ResponseRecorder is an optional extension of FactRecorder that also
// receives the handler response. Implement it alongside FactRecorder to
// observe response data (e.g. whether a poll returned a task).
type ResponseRecorder interface {
	RecordResponse(ctx context.Context, req, resp any)
}

// RejectionRecorder is an optional extension of FactRecorder that receives a
// handler's error outcome — the request was rejected rather than served. It lets
// the observer model a synchronous rejection (an invalid request that produced no
// entity and no telemetry) as a fact. See UMPIRE.md.
type RejectionRecorder interface {
	RecordRejection(ctx context.Context, req any, err error)
}
