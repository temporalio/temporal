package interceptor

import (
	"context"
	"strings"

	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TrailerToContextMetadataInterceptor reads metadata from gRPC response trailers
// and propagates it to the calling context using contextutil.ContextMetadataSet.
//
// Requires the context to be pre-wrapped with contextutil.WithMetadataContext() before the RPC call.
// This is typically done by server-side interceptors (e.g., ContextMetadataInterceptor).
func TrailerToContextMetadataInterceptor(logger log.Logger) grpc.UnaryClientInterceptor {
	throttledLogger := log.NewThrottledLogger(logger, func() float64 {
		return 1.0 / 30.0 // 1 log per 30 seconds
	})
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var trailer metadata.MD
		opts = append(opts, grpc.Trailer(&trailer))

		err := invoker(ctx, method, req, reply, cc, opts...)

		trailerMetadata := make(map[string]string)
		propagatedMetadata := make(map[string]string)

		for prefixedKey, values := range trailer {
			key, ok := strings.CutPrefix(prefixedKey, trailerKeyPrefix)
			if !ok {
				// Backward compatibility: accept unprefixed keys from older writers.
				if prefixedKey != contextutil.MetadataKeyWorkflowType && prefixedKey != contextutil.MetadataKeyWorkflowTaskQueue {
					continue
				}
				key = prefixedKey
			}
			if len(values) == 0 {
				continue
			}

			trailerMetadata[key] = values[0]
			if contextutil.ContextMetadataSet(ctx, key, values[0]) {
				propagatedMetadata[key] = values[0]
			}
		}

		logMetadataPropagationStatus(ctx, method, trailerMetadata, propagatedMetadata, throttledLogger)

		return err
	}
}

func logMetadataPropagationStatus(
	ctx context.Context,
	method string,
	trailerMetadata map[string]string,
	propagatedMetadata map[string]string,
	throttledLogger log.ThrottledLogger,
) {
	contextWrapped := contextutil.ContextHasMetadata(ctx)

	if len(trailerMetadata) == 0 {
		throttledLogger.Info("TrailerToContextMetadataInterceptor: No metadata in trailer",
			tag.NewBoolTag("contextWrapped", contextWrapped),
			tag.NewStringTag("method", method))
		return
	}

	if !contextWrapped {
		throttledLogger.Warn("TrailerToContextMetadataInterceptor: Trailer had metadata but context not wrapped",
			tag.NewAnyTag("trailer", trailerMetadata),
			tag.NewStringTag("method", method))
		return
	}

	if len(propagatedMetadata) < len(trailerMetadata) {
		throttledLogger.Warn("TrailerToContextMetadataInterceptor: Failed to propagate some metadata from trailer",
			tag.NewAnyTag("trailer", trailerMetadata),
			tag.NewAnyTag("propagated", propagatedMetadata),
			tag.NewStringTag("method", method))
		return
	}

	throttledLogger.Info("TrailerToContextMetadataInterceptor: Propagated metadata from trailer",
		tag.NewAnyTag("trailer", propagatedMetadata),
		tag.NewStringTag("method", method))
}
