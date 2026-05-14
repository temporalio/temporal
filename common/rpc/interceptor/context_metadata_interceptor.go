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

// trailerBinSuffix is appended to trailer keys so that gRPC automatically base64-encodes
// values on the wire per the gRPC over HTTP/2 spec (PROTOCOL-HTTP2.md §Requests, "Binary-Header"):
//
//	https://github.com/grpc/grpc/blob/cf61c7d62a1a7f43b9d2ea6488186bc14fc41a8c/doc/PROTOCOL-HTTP2.md
//
// Binary headers use Base64 encoding (RFC 4648 §4) because the gRPC HTTP/2 framer rejects
// header values containing certain control bytes. Without this, values containing C0 control bytes
// (0x00–0x1F except HTAB) or DEL (0x7F) cause the HTTP/2 framer to reject the response.
//
// Constraint: context metadata keys (in contextutil) must NOT end in "-bin", or the reader
// cannot distinguish "contextmetadata-<key>-bin" (binary) from "contextmetadata-<key>" (plain)
// when <key> itself ends in "-bin".
const trailerBinSuffix = "-bin"

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

// isHTTP2HeaderSafe reports whether s can be sent through Go's gRPC HTTP/2 framer
// without rejection. The framer forbids C0 control bytes 0x00–0x1F (except HTAB 0x09)
// and DEL 0x7F. Bytes >= 0x80 (UTF-8, emoji, accented chars) are allowed by Go's
// framer but are technically outside strict RFC 9113 visible-ASCII range — if a non-Go
// proxy or gRPC implementation sits between endpoints, the -bin path handles those.
// Iterates bytes, not runes — this is intentional because HTTP/2 validation is octet-based.
func isHTTP2HeaderSafe(s string) bool {
	for i := range len(s) {
		b := s[i]
		if b == 0x7f || (b < 0x20 && b != 0x09) {
			return false
		}
	}
	return true
}

// buildTrailerPairs constructs gRPC trailer key-value pairs for all context metadata.
// Each key is emitted in up to three formats for backward compatibility:
// (1) binary-safe with "-bin" suffix (always), (2) plain-text with prefix (when value
// is HTTP/2-safe), (3) unprefixed well-known keys (when value is HTTP/2-safe).
func (c *ContextMetadataInterceptor) buildTrailerPairs(ctx context.Context, info *grpc.UnaryServerInfo) []string {
	var trailerPairs []string
	for key, value := range contextutil.ContextMetadataGetAll(ctx) {
		valStr := fmt.Sprint(value)
		trailerPairs = append(trailerPairs, trailerKeyPrefix+key+trailerBinSuffix, valStr)
		if isHTTP2HeaderSafe(valStr) {
			trailerPairs = append(trailerPairs, trailerKeyPrefix+key, valStr)
			if key == contextutil.MetadataKeyWorkflowType || key == contextutil.MetadataKeyWorkflowTaskQueue {
				trailerPairs = append(trailerPairs, key, valStr)
			}
		} else {
			valueSample := valStr
			if len(valueSample) > 128 {
				valueSample = valueSample[:128]
			}
			c.throttledLogger.Warn("ContextMetadataInterceptor: Skipping plain-text trailer key for HTTP/2-unsafe value (only -bin key emitted)",
				tag.NewStringTag("key", key),
				tag.NewStringTag("fullMethod", info.FullMethod),
				tag.NewStringTag("valueSample", fmt.Sprintf("%q", valueSample)),
			)
		}
	}
	return trailerPairs
}

func (c *ContextMetadataInterceptor) appendContextMetadataToTrailer(ctx context.Context, info *grpc.UnaryServerInfo) {
	// If the context is done, the gRPC stream may already be in streamDone state,
	// and SetTrailer would return ErrIllegalHeaderWrite ("SendHeader called multiple times").
	select {
	case <-ctx.Done():
		return
	default:
	}

	trailerPairs := c.buildTrailerPairs(ctx, info)

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
