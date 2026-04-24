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

// trailerKeyPrefix namespaces context metadata keys in gRPC trailers so the client-side
// interceptor can distinguish them from gRPC-internal and other interceptor trailer keys.
const trailerKeyPrefix = "contextmetadata-"

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
			return 1.0 / 30.0 // 1 log per 30 seconds
		})
	}
	return cmi
}

// Intercept wraps the request context with metadata storage and invokes the handler.
// When setTrailer is enabled, accumulated metadata is written to gRPC response trailers.
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
	// If the context is done, the gRPC stream may already be in streamDone state,
	// and SetTrailer would return ErrIllegalHeaderWrite ("SendHeader called multiple times").
	select {
	case <-ctx.Done():
		return
	default:
	}

	var trailerPairs []string

	for key, value := range contextutil.ContextMetadataGetAll(ctx) {
		valStr := fmt.Sprint(value)
		trailerPairs = append(trailerPairs, trailerKeyPrefix+key, valStr)
		// Backward compatibility: also emit unprefixed keys for older readers.
		if key == contextutil.MetadataKeyWorkflowType || key == contextutil.MetadataKeyWorkflowTaskQueue {
			trailerPairs = append(trailerPairs, key, valStr)
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
			tag.Error(err),
			tag.NewStringTag("fullMethod", info.FullMethod))
	}
}
