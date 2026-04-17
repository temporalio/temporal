package interceptor

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ContextMetadataInterceptor struct {
	setTrailer      bool
	logger          log.Logger
	throttledLogger log.ThrottledLogger
}

// NewContextMetadataInterceptor creates a new ContextMetadataInterceptor
func NewContextMetadataInterceptor(setTrailer bool, logger log.Logger) *ContextMetadataInterceptor {
	cmi := &ContextMetadataInterceptor{
		setTrailer: setTrailer,
	}
	if setTrailer {
		cmi.logger = logger
		cmi.throttledLogger = log.NewThrottledLogger(logger, func() float64 {
			return 0.1 // 1 log per 10 seconds
		})
	}
	return cmi
}

// Intercept adds metadata context to all incoming gRPC requests
func (c *ContextMetadataInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	ctx = contextutil.WithMetadataContext(ctx)

	resp, err := handler(ctx, req)

	if c.setTrailer {
		c.appendContextMetadataToTrailer(ctx, info)
	}

	return resp, err
}

func (c *ContextMetadataInterceptor) appendContextMetadataToTrailer(ctx context.Context, info *grpc.UnaryServerInfo) {
	var trailerPairs []string

	for _, key := range contextutil.PropagatedMetadataKeys() {
		if value, ok := contextutil.ContextMetadataGet(ctx, key); ok {
			trailerPairs = append(trailerPairs, key, fmt.Sprint(value))
		}
	}

	if len(trailerPairs) == 0 {
		c.throttledLogger.Info("ContextMetadataInterceptor: No metadata in context, not setting trailer",
			tag.NewStringTag("fullMethod", info.FullMethod),
		)
		return
	}

	trailer := metadata.Pairs(trailerPairs...)
	c.throttledLogger.Info("ContextMetadataInterceptor: Setting trailer",
		tag.NewAnyTag("trailer", trailer),
		tag.NewStringTag("fullMethod", info.FullMethod),
	)

	if err := grpc.SetTrailer(ctx, trailer); err != nil {
		c.logger.Error("ContextMetadataInterceptor: Failed to set trailer",
			tag.Error(err))
	}
}
