package interceptor

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	contextpropagationspb "go.temporal.io/server/api/contextpropagation/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestContextMetadataInterceptor_Intercept(t *testing.T) {
	testCases := []struct {
		name             string
		setTrailer       bool
		setupContext     func() context.Context
		handler          func(context.Context, any) (any, error)
		wantErr          bool
		expectTrailerErr bool
		wantResponse     any
		validateResult   func(*testing.T, context.Context)
	}{
		{
			name:       "AddsMetadataContext",
			setTrailer: false,
			setupContext: func() context.Context {
				return t.Context()
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return "response", nil
			},
			wantErr:      false,
			wantResponse: "response",
			validateResult: func(t *testing.T, ctx context.Context) {
				ok := contextutil.ContextMetadataSet(ctx, "test-key", "test-value")
				require.True(t, ok, "context should be wrapped with metadata context")
			},
		},
		{
			name:       "WithSetTrailer_NoMetadata",
			setTrailer: true,
			setupContext: func() context.Context {
				return t.Context()
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return "response", nil
			},
			wantErr:      false,
			wantResponse: "response",
		},
		{
			name:             "WithSetTrailer_WithMetadata",
			setTrailer:       true,
			expectTrailerErr: true,
			setupContext: func() context.Context {
				return metadata.NewOutgoingContext(t.Context(), metadata.New(map[string]string{}))
			},
			handler: func(ctx context.Context, req any) (any, error) {
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow-type")
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue")
				contextutil.ContextMetadataSet(ctx, "other-key", "other-value")
				return "response", nil
			},
			wantErr:      false,
			wantResponse: "response",
		},
		{
			name:       "WithoutSetTrailer",
			setTrailer: false,
			setupContext: func() context.Context {
				return t.Context()
			},
			handler: func(ctx context.Context, req any) (any, error) {
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow-type")
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue")
				return "response", nil
			},
			wantErr:      false,
			wantResponse: "response",
		},
		{
			name:       "HandlerReturnsError",
			setTrailer: false,
			setupContext: func() context.Context {
				return t.Context()
			},
			handler: func(ctx context.Context, req any) (any, error) {
				return nil, grpc.ErrServerStopped
			},
			wantErr:      true,
			wantResponse: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			if tc.expectTrailerErr {
				tl.Expect(testlogger.Error, "ContextMetadataInterceptor: Failed to set trailer")
			}
			interceptor := NewContextMetadataInterceptor(tc.setTrailer, tl)

			var capturedCtx context.Context
			wrappedHandler := func(ctx context.Context, req any) (any, error) {
				capturedCtx = ctx
				return tc.handler(ctx, req)
			}

			ctx := tc.setupContext()
			info := &grpc.UnaryServerInfo{
				FullMethod: "/test.Service/TestMethod",
			}
			resp, err := interceptor.Intercept(ctx, "request", info, wrappedHandler)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantResponse, resp)

			if tc.validateResult != nil {
				tc.validateResult(t, capturedCtx)
			}
		})
	}
}

func TestContextMetadataInterceptor_appendContextMetadataToTrailer(t *testing.T) {
	testCases := []struct {
		name             string
		setupContext     func() context.Context
		expectTrailerErr bool
	}{
		{
			name: "AllPropagatedKeys",
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-queue")
				return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{}))
			},
			expectTrailerErr: true,
		},
		{
			name: "PartialMetadata",
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
				return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{}))
			},
			expectTrailerErr: true,
		},
		{
			name: "NoMetadata",
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{}))
			},
			expectTrailerErr: false,
		},
		{
			name: "NoOutgoingContext",
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
				return ctx
			},
			expectTrailerErr: true,
		},
		{
			name: "NonStringValues",
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, 12345)
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, struct{ name string }{name: "queue"})
				return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{}))
			},
			expectTrailerErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			if tc.expectTrailerErr {
				tl.Expect(testlogger.Error, "ContextMetadataInterceptor: Failed to set trailer")
			}
			interceptor := NewContextMetadataInterceptor(true, tl)

			ctx := tc.setupContext()
			info := &grpc.UnaryServerInfo{
				FullMethod: "/test.Service/TestMethod",
			}
			interceptor.appendContextMetadataToTrailer(ctx, info)
		})
	}
}

func TestContextMetadataInterceptor_backwardCompatTrailerKeys(t *testing.T) {
	ctx := contextutil.WithMetadataContext(t.Context())
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-queue")
	contextutil.ContextMetadataSet(ctx, "other-key", "other-value")

	allMetadata := contextutil.ContextMetadataGetAll(ctx)
	var trailerPairs []string
	for key, value := range allMetadata {
		valStr := fmt.Sprint(value)
		trailerPairs = append(trailerPairs, trailerKeyPrefix+key, valStr)
		if key == contextutil.MetadataKeyWorkflowType || key == contextutil.MetadataKeyWorkflowTaskQueue {
			trailerPairs = append(trailerPairs, key, valStr)
		}
	}

	trailer := metadata.Pairs(trailerPairs...)

	// Well-known keys appear both with and without prefix
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowTaskQueue)

	// Other keys only appear with prefix
	require.Contains(t, trailer, trailerKeyPrefix+"other-key")
	require.NotContains(t, trailer, "other-key")
}

func TestNewContextMetadataInterceptor(t *testing.T) {
	testCases := []struct {
		name       string
		setTrailer bool
	}{
		{
			name:       "WithSetTrailerTrue",
			setTrailer: true,
		},
		{
			name:       "WithSetTrailerFalse",
			setTrailer: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			interceptor := NewContextMetadataInterceptor(tc.setTrailer, tl)

			require.NotNil(t, interceptor)
			require.Equal(t, tc.setTrailer, interceptor.setTrailer)
			if tc.setTrailer {
				require.Equal(t, tl, interceptor.logger)
				require.NotNil(t, interceptor.throttledLogger)
			} else {
				require.Nil(t, interceptor.logger)
				require.Nil(t, interceptor.throttledLogger)
			}
		})
	}
}

func TestBuildTrailerPairs_ProtoRoundTrip(t *testing.T) {
	allMetadata := map[string]any{
		contextutil.MetadataKeyWorkflowType:      "test-workflow",
		contextutil.MetadataKeyWorkflowTaskQueue: "test-queue",
		"other-key":                              "other-value",
	}

	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	trailer := metadata.Pairs(pairs...)

	// Proto key must be present.
	protoValues := trailer[protoTrailerKey]
	require.Len(t, protoValues, 1, "expected exactly one proto trailer value")

	// Deserialize and verify round-trip.
	protoMsg := &contextpropagationspb.ContextMetadata{}
	err := proto.Unmarshal([]byte(protoValues[0]), protoMsg)
	require.NoError(t, err)
	require.Equal(t, "test-workflow", protoMsg.Entries[contextutil.MetadataKeyWorkflowType])
	require.Equal(t, "test-queue", protoMsg.Entries[contextutil.MetadataKeyWorkflowTaskQueue])
	require.Equal(t, "other-value", protoMsg.Entries["other-key"])
}

func TestBuildTrailerPairs_EmitsBothProtoAndLegacyKeys(t *testing.T) {
	allMetadata := map[string]any{
		contextutil.MetadataKeyWorkflowType:      "test-workflow",
		contextutil.MetadataKeyWorkflowTaskQueue: "test-queue",
		"other-key":                              "other-value",
	}

	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	trailer := metadata.Pairs(pairs...)

	// Proto key present.
	require.Contains(t, trailer, protoTrailerKey)

	// Legacy prefixed keys present.
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, trailerKeyPrefix+"other-key")

	// Well-known unprefixed keys also present (backward compat).
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowTaskQueue)

	// Non-well-known keys only appear with prefix (no unprefixed "other-key").
	require.NotContains(t, trailer, "other-key")
}

func TestBuildTrailerPairs_EmptyMetadata(t *testing.T) {
	allMetadata := map[string]any{}
	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	// Even with empty metadata, proto is serialized (valid empty message).
	trailer := metadata.Pairs(pairs...)
	require.Contains(t, trailer, protoTrailerKey)
}

func TestBuildTrailerPairs_NonStringValues(t *testing.T) {
	allMetadata := map[string]any{
		contextutil.MetadataKeyWorkflowType:      12345,
		contextutil.MetadataKeyWorkflowTaskQueue: struct{ name string }{name: "queue"},
	}

	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	trailer := metadata.Pairs(pairs...)

	// Proto key present with fmt.Sprint values.
	protoValues := trailer[protoTrailerKey]
	require.Len(t, protoValues, 1)

	protoMsg := &contextpropagationspb.ContextMetadata{}
	err := proto.Unmarshal([]byte(protoValues[0]), protoMsg)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(12345), protoMsg.Entries[contextutil.MetadataKeyWorkflowType])
}

func TestBuildTrailerPairs_ControlCharsInValues(t *testing.T) {
	allMetadata := map[string]any{
		contextutil.MetadataKeyWorkflowType: "workflow\nwith\x00control\rchars",
	}

	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	trailer := metadata.Pairs(pairs...)

	// Proto key must be present and correctly round-trip.
	protoValues := trailer[protoTrailerKey]
	require.Len(t, protoValues, 1)

	protoMsg := &contextpropagationspb.ContextMetadata{}
	err := proto.Unmarshal([]byte(protoValues[0]), protoMsg)
	require.NoError(t, err)
	require.Equal(t, "workflow\nwith\x00control\rchars", protoMsg.Entries[contextutil.MetadataKeyWorkflowType])

	// Legacy keys must NOT be present for HTTP/2-unsafe values.
	require.NotContains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.NotContains(t, trailer, contextutil.MetadataKeyWorkflowType)
}

func TestBuildTrailerPairs_MixedSafeAndUnsafeValues(t *testing.T) {
	allMetadata := map[string]any{
		contextutil.MetadataKeyWorkflowType:      "workflow\nwith\nnewlines",
		contextutil.MetadataKeyWorkflowTaskQueue: "safe-queue-name",
	}

	cmi := NewContextMetadataInterceptor(true, testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	pairs := cmi.buildTrailerPairs(allMetadata)
	trailer := metadata.Pairs(pairs...)

	// Proto key carries both entries.
	protoValues := trailer[protoTrailerKey]
	require.Len(t, protoValues, 1)
	protoMsg := &contextpropagationspb.ContextMetadata{}
	err := proto.Unmarshal([]byte(protoValues[0]), protoMsg)
	require.NoError(t, err)
	require.Equal(t, "workflow\nwith\nnewlines", protoMsg.Entries[contextutil.MetadataKeyWorkflowType])
	require.Equal(t, "safe-queue-name", protoMsg.Entries[contextutil.MetadataKeyWorkflowTaskQueue])

	// Unsafe workflow type: legacy keys skipped.
	require.NotContains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)

	// Safe task queue: legacy keys present.
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowTaskQueue)
}

func TestIsHTTP2SafeValue(t *testing.T) {
	require.True(t, isHTTP2SafeValue("hello world"))
	require.True(t, isHTTP2SafeValue("value with\ttab"))
	require.True(t, isHTTP2SafeValue(""))
	require.False(t, isHTTP2SafeValue("has\nnewline"))
	require.False(t, isHTTP2SafeValue("has\x00null"))
	require.False(t, isHTTP2SafeValue("has\rcarriage"))
	require.False(t, isHTTP2SafeValue("has\x07bell"))
	require.False(t, isHTTP2SafeValue("has\x7fDEL"))
}
