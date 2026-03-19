package umpire

import (
	"context"

	"google.golang.org/grpc"
)

// FactRecorder records gRPC request events.
type FactRecorder interface {
	RecordFact(context.Context, any)
}

// FaultInjector injects faults into gRPC calls.
type FaultInjector interface {
	MakePlay(ctx context.Context, targetType any, request any) error
}

// ResponseRecorder is an optional extension of FactRecorder that also
// receives the handler response. Implement it alongside FactRecorder to
// observe response data (e.g. whether a poll returned a task).
type ResponseRecorder interface {
	RecordResponse(ctx context.Context, req, resp any)
}

// NewUnaryServerInterceptor returns a gRPC interceptor that records events via rec
// and optionally injects faults via inj. Either may be nil.
func NewUnaryServerInterceptor(rec FactRecorder, inj FaultInjector) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if rec != nil {
			rec.RecordFact(ctx, req)
		}
		if inj != nil {
			if err := inj.MakePlay(ctx, req, req); err != nil {
				return nil, err
			}
		}
		resp, err := handler(ctx, req)
		if err == nil {
			if rr, ok := rec.(ResponseRecorder); ok {
				rr.RecordResponse(ctx, req, resp)
			}
		}
		return resp, err
	}
}
