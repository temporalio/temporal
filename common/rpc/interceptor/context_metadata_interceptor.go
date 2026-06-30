package interceptor

import (
	"context"
	"fmt"

	contextpropagationspb "go.temporal.io/server/api/contextpropagation/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// trailerKeyPrefix namespaces context metadata keys in gRPC trailers so the client-side
// interceptor can distinguish them from gRPC-internal and other interceptor trailer keys.
const trailerKeyPrefix = "contextmetadata-"

// protoTrailerKey is the gRPC trailer key that carries all context metadata as a single
// serialized protobuf. The "-bin" suffix tells gRPC to base64-encode the value on the
// wire, making it safe for arbitrary byte sequences (including HTTP/2-unsafe control
// characters in workflow type names).
const protoTrailerKey = "contextmetadata-bin"

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

	allMetadata := contextutil.ContextMetadataGetAll(ctx)
	if len(allMetadata) == 0 {
		c.throttledLogger.Info("ContextMetadataInterceptor: No metadata in context, not setting trailer",
			tag.NewStringTag("fullMethod", info.FullMethod),
		)
		return
	}

	trailerPairs := c.buildTrailerPairs(allMetadata)

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

// buildTrailerPairs constructs the gRPC trailer key-value pairs from context metadata.
//
// It emits metadata in two formats:
//  1. Proto format: all entries serialized into a single ContextMetadata protobuf under
//     the "contextmetadata-bin" key. The "-bin" suffix causes gRPC to base64-encode the
//     value, making it safe for arbitrary bytes (including HTTP/2-unsafe control chars).
//  2. Legacy format (backward compatibility during rolling deploys): individual
//     "contextmetadata-<key>" entries plus unprefixed well-known keys. Old readers that
//     don't understand the proto key will fall back to these.
func (c *ContextMetadataInterceptor) buildTrailerPairs(allMetadata map[string]any) []string {
	var trailerPairs []string

	// Proto format: serialize all metadata into a single protobuf message.
	protoMsg := &contextpropagationspb.ContextMetadata{
		Entries: make(map[string]string, len(allMetadata)),
	}
	for key, value := range allMetadata {
		protoMsg.Entries[key] = fmt.Sprint(value)
	}
	if protoBytes, err := proto.Marshal(protoMsg); err != nil {
		c.throttledLogger.Warn("ContextMetadataInterceptor: Failed to marshal proto metadata, falling back to legacy-only",
			tag.Error(err),
		)
	} else {
		trailerPairs = append(trailerPairs, protoTrailerKey, string(protoBytes))
	}

	// Legacy format: emit individual keys for backward compatibility with older readers.
	// Skip entries with HTTP/2-unsafe values (the proto key handles those).
	for key, value := range allMetadata {
		valStr := fmt.Sprint(value)
		if !isHTTP2SafeValue(valStr) {
			continue
		}
		trailerPairs = append(trailerPairs, trailerKeyPrefix+key, valStr)
		// Backward compatibility: also emit unprefixed keys for older readers.
		if key == contextutil.MetadataKeyWorkflowType || key == contextutil.MetadataKeyWorkflowTaskQueue {
			trailerPairs = append(trailerPairs, key, valStr)
		}
	}

	return trailerPairs
}

// isHTTP2SafeValue returns true if the string can be used as an HTTP/2 header value.
// Per RFC 9113 section 8.2.1, header field values must not contain NUL (0x00),
// CR (0x0D), or LF (0x0A). Additionally, Go's HTTP/2 framer rejects C0 control
// characters (0x00-0x1F except HTAB 0x09) and DEL (0x7F).
func isHTTP2SafeValue(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0x09 { // HTAB is allowed
			continue
		}
		if b < 0x20 || b == 0x7f { // C0 controls and DEL
			return false
		}
	}
	return true
}
