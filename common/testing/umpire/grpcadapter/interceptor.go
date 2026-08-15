// Package grpcadapter connects Umpire observation and injection contracts to gRPC.
package grpcadapter

import (
	"context"

	"go.temporal.io/server/common/testing/umpire"
	"google.golang.org/grpc"
)

// NewUnaryServerInterceptor returns a gRPC interceptor that records events via rec
// and optionally injects faults via inj. Either may be nil.
func NewUnaryServerInterceptor(rec umpire.FactRecorder, inj umpire.FaultInjector) grpc.UnaryServerInterceptor {
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
			if err := inj.Inject(ctx, req, req); err != nil {
				return nil, err
			}
		}
		resp, err := handler(ctx, req)
		if err == nil {
			if rr, ok := rec.(umpire.ResponseRecorder); ok {
				rr.RecordResponse(ctx, req, resp)
			}
		} else if rr, ok := rec.(umpire.RejectionRecorder); ok {
			rr.RecordRejection(ctx, req, err)
		}
		return resp, err
	}
}
