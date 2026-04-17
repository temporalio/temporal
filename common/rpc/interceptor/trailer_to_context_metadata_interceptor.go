package interceptor

import (
	"context"

	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TrailerToContextMetadataInterceptor reads workflow metadata from gRPC response trailers
// and propagates it to the calling context using contextutil.ContextMetadataSet.
//
// Requires the context to be pre-wrapped with contextutil.WithMetadataContext() before the RPC call.
// This is typically done by server-side interceptors (e.g., ContextMetadataInterceptor).
func TrailerToContextMetadataInterceptor(logger log.Logger) grpc.UnaryClientInterceptor {
	throttledLogger := log.NewThrottledLogger(logger, func() float64 {
		return 0.1 // 1 log per 10 seconds
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

		for _, key := range contextutil.PropagatedMetadataKeys() {
			trailerValues := trailer.Get(key)
			if len(trailerValues) == 0 {
				continue
			}

			// NOTE: ContextMetadataSet overwrites activity metadata rather than accumulating.
			// This is safe today because only History populates activity metadata, and each
			// Frontend request calls one downstream service, not both. If a future flow has
			// multiple downstream RPCs independently contributing activity metadata in the
			// same request, switch to ContextMetadataAddActivity to accumulate.
			trailerMetadata[key] = trailerValues[0]
			if contextutil.ContextMetadataSet(ctx, key, trailerValues[0]) {
				propagatedMetadata[key] = trailerValues[0]
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
