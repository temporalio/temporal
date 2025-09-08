package metrics

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc/stats"
)

type ServerStatsHandler stats.Handler

var _ stats.Handler = (*grpcStatsHandler)(nil)

type grpcStatsHandler struct {
	mh          Handler
	activeConns atomic.Int64
}

func NewServerStatsHandler(mh Handler) ServerStatsHandler {
	return &grpcStatsHandler{
		mh: mh,
	}
}

func (h *grpcStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *grpcStatsHandler) HandleConn(_ context.Context, stat stats.ConnStats) {
	switch stat.(type) {
	case *stats.ConnBegin:
		ServiceConnAccepted.With(h.mh).Record(1)
		newVal := h.activeConns.Add(1)
		ServiceConnActive.With(h.mh).Record(float64(newVal))
	case *stats.ConnEnd:
		ServiceConnClosed.With(h.mh).Record(1)
		newVal := h.activeConns.Add(-1)
		if newVal < 0 { // should never happen, but just in case
			h.activeConns.Store(0)
			newVal = 0
		}
		ServiceConnActive.With(h.mh).Record(float64(newVal))
	}
}

func (h *grpcStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *grpcStatsHandler) HandleRPC(ctx context.Context, stat stats.RPCStats) {
	// nothing to do here
}
