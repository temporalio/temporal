package interceptor

import (
	"context"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/expguard"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// ExperimentalGuardInterceptor checks inbound requests for unknown proto fields
// when no experimental API variant is active. An unknown field on an inbound
// request means an experimental client sent a field this binary doesn't know
// about. When a variant IS active the server expects experimental traffic, so
// the check is skipped.
type ExperimentalGuardInterceptor struct {
	apiVariant dynamicconfig.StringPropertyFn
	logger     log.Logger
}

func NewExperimentalGuardInterceptor(
	apiVariant dynamicconfig.StringPropertyFn,
	logger log.Logger,
) *ExperimentalGuardInterceptor {
	return &ExperimentalGuardInterceptor{apiVariant: apiVariant, logger: logger}
}

func (g *ExperimentalGuardInterceptor) UnaryIntercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if g.apiVariant() == "" {
		if msg, ok := req.(proto.Message); ok {
			expguard.Check(g.logger, msg, "frontend.inbound")
		}
	}
	return handler(ctx, req)
}
