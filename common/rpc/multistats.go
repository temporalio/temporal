package rpc

import (
	"context"

	"google.golang.org/grpc/stats"
)

// MultiStatsHandler forwards stats to multiple stats.Handlers.
type MultiStatsHandler []stats.Handler

func (m MultiStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	for _, h := range m {
		ctx = h.TagConn(ctx, info)
	}
	return ctx
}

func (m MultiStatsHandler) HandleConn(ctx context.Context, cs stats.ConnStats) {
	for _, h := range m {
		h.HandleConn(ctx, cs)
	}
}

func (m MultiStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	for _, h := range m {
		ctx = h.TagRPC(ctx, info)
	}
	return ctx
}

func (m MultiStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	for _, h := range m {
		h.HandleRPC(ctx, rs)
	}
}
