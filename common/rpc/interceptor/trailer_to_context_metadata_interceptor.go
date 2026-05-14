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

		setMetadata := func(key, value string) {
			trailerMetadata[key] = value
			if contextutil.ContextMetadataSet(ctx, key, value) {
				propagatedMetadata[key] = value
			}
		}

		// Pass 1: -bin keys from new writers (highest priority).
		// These are authoritative and take precedence over plain-text keys.
		binKeys := make(map[string]struct{})
		for trailerKey, values := range trailer {
			if !strings.HasPrefix(trailerKey, trailerKeyPrefix) || !strings.HasSuffix(trailerKey, trailerBinSuffix) {
				continue
			}
			key := trailerKey[len(trailerKeyPrefix) : len(trailerKey)-len(trailerBinSuffix)]
			if key == "" {
				continue
			}
			if len(values) == 0 {
				throttledLogger.Warn("TrailerToContextMetadataInterceptor: -bin trailer key has empty values",
					tag.NewStringTag("trailerKey", trailerKey),
					tag.NewStringTag("method", method),
				)
				continue
			}
			binKeys[key] = struct{}{}
			setMetadata(key, values[0])
		}

		// Pass 2: plain prefixed and unprefixed keys from old writers.
		// Skip keys already set by a -bin entry.
		for trailerKey, values := range trailer {
			if strings.HasPrefix(trailerKey, trailerKeyPrefix) && strings.HasSuffix(trailerKey, trailerBinSuffix) {
				continue
			}
			key, ok := extractContextMetadataKey(trailerKey)
			if !ok || key == "" || len(values) == 0 {
				continue
			}
			if _, hasBin := binKeys[key]; hasBin {
				continue
			}
			setMetadata(key, values[0])
		}

		logMetadataPropagationStatus(ctx, method, trailerMetadata, propagatedMetadata, throttledLogger)

		return err
	}
}

// extractContextMetadataKey extracts the context metadata key from a plain-text
// gRPC trailer key (formats 2 and 3 only — binary "-bin" keys are handled
// separately by the caller's first pass).
//
// Recognized formats (format 1, binary "-bin" keys, is handled by the caller's first pass):
//  2. Plain-text prefixed: "contextmetadata-<key>" (old writers)
//  3. Unprefixed well-known: "workflow-type" / "workflow-task-queue" (very old writers)
func extractContextMetadataKey(trailerKey string) (string, bool) {
	if after, ok := strings.CutPrefix(trailerKey, trailerKeyPrefix); ok {
		if after == "" {
			return "", false
		}
		return after, true
	}
	if trailerKey == contextutil.MetadataKeyWorkflowType || trailerKey == contextutil.MetadataKeyWorkflowTaskQueue {
		return trailerKey, true
	}
	return "", false
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
