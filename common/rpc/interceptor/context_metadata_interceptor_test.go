package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
		expectUnsafeWarn bool
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
		{
			name:             "UnsafeValueEmitsBinKeyOnly",
			expectUnsafeWarn: true,
			setupContext: func() context.Context {
				ctx := contextutil.WithMetadataContext(t.Context())
				contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "Foo\nBar")
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
			if tc.expectUnsafeWarn {
				tl.Expect(testlogger.Warn, "ContextMetadataInterceptor: Skipping plain-text trailer key for HTTP/2-unsafe value (only -bin key emitted)")
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
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	interceptor := NewContextMetadataInterceptor(true, tl)

	ctx := contextutil.WithMetadataContext(t.Context())
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-queue")
	contextutil.ContextMetadataSet(ctx, "other-key", "other-value")

	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/TestMethod"}
	trailerPairs := interceptor.buildTrailerPairs(ctx, info)
	trailer := metadata.Pairs(trailerPairs...)

	// Level 1 (newest): all keys get -bin suffixed entries
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType+trailerBinSuffix)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue+trailerBinSuffix)
	require.Contains(t, trailer, trailerKeyPrefix+"other-key"+trailerBinSuffix)

	// Level 2: all keys get plain prefixed entries (for safe values)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, trailerKeyPrefix+"other-key")

	// Level 3 (oldest): well-known keys also get unprefixed entries
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowTaskQueue)
	require.NotContains(t, trailer, "other-key")
}

func TestContextMetadataInterceptor_binTrailerKeys_SafeValues(t *testing.T) {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	interceptor := NewContextMetadataInterceptor(true, tl)

	ctx := contextutil.WithMetadataContext(t.Context())
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "test-workflow")
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "test-queue")
	contextutil.ContextMetadataSet(ctx, "other-key", "other-value")

	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/TestMethod"}
	trailerPairs := interceptor.buildTrailerPairs(ctx, info)
	trailer := metadata.Pairs(trailerPairs...)

	// Binary-safe keys always present
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType+trailerBinSuffix)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue+trailerBinSuffix)
	require.Contains(t, trailer, trailerKeyPrefix+"other-key"+trailerBinSuffix)

	// Plain-text keys also present for safe values (backward compat during rolling deploy)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, trailerKeyPrefix+"other-key")
}

func TestContextMetadataInterceptor_binTrailerKeys_UnsafeValues(t *testing.T) {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	tl.Expect(testlogger.Warn, "ContextMetadataInterceptor: Skipping plain-text trailer key for HTTP/2-unsafe value (only -bin key emitted)")
	interceptor := NewContextMetadataInterceptor(true, tl)

	ctx := contextutil.WithMetadataContext(t.Context())
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, "Foo\nBar")
	contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, "safe-queue")

	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/TestMethod"}
	trailerPairs := interceptor.buildTrailerPairs(ctx, info)
	trailer := metadata.Pairs(trailerPairs...)

	// Binary-safe keys present for all values
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType+trailerBinSuffix)
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue+trailerBinSuffix)

	// Plain-text key suppressed for the unsafe value (newline)
	require.NotContains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowType)
	require.NotContains(t, trailer, contextutil.MetadataKeyWorkflowType)

	// Safe value still gets plain-text keys
	require.Contains(t, trailer, trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue)
	require.Contains(t, trailer, contextutil.MetadataKeyWorkflowTaskQueue)
}

func TestIsHTTP2HeaderSafe(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{"empty", "", true},
		{"plain ASCII", "hello-world", true},
		{"with HTAB", "hello\tworld", true},
		{"with space", "hello world", true},
		{"UTF-8 CJK", "Workflow-日本語", true},
		{"emoji", "🚀-workflow", true},
		{"accented chars", "café-résumé", true},
		{"mixed Unicode", "workflow-über-naïve-🎉", true},
		{"newline", "Foo\nBar", false},
		{"carriage return", "Foo\rBar", false},
		{"NUL byte", "Foo\x00Bar", false},
		{"DEL", "Foo\x7fBar", false},
		{"bell", "Foo\x07Bar", false},
		{"escape", "Foo\x1bBar", false},
		{"unit separator 0x1F boundary", "\x1f", false},
		{"space 0x20 boundary", "\x20", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isHTTP2HeaderSafe(tt.s))
		})
	}
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
