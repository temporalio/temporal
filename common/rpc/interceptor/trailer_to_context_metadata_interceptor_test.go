package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestTrailerToContextMetadataInterceptor(t *testing.T) {
	testCases := []struct {
		name             string
		setupInvoker     func() grpc.UnaryInvoker
		contextWrapped   bool
		wantErr          bool
		validateMetadata func(*testing.T, context.Context)
	}{
		{
			name: "NoTrailerMetadata",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
		},
		{
			name: "WithWorkflowType",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(trailerKeyPrefix+contextutil.MetadataKeyWorkflowType, "test-workflow-type")
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				value, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "test-workflow-type", value)
			},
		},
		{
			name: "WithWorkflowTaskQueue",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue")
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				value, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
				require.True(t, ok)
				require.Equal(t, "test-task-queue", value)
			},
		},
		{
			name: "WithAllMetadata",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(
								trailerKeyPrefix+contextutil.MetadataKeyWorkflowType, "test-workflow-type",
								trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue",
							)
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				workflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "test-workflow-type", workflowType)

				taskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
				require.True(t, ok)
				require.Equal(t, "test-task-queue", taskQueue)
			},
		},
		{
			name: "WithMultipleValues",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.MD{
								trailerKeyPrefix + contextutil.MetadataKeyWorkflowType: []string{"first-value", "second-value", "third-value"},
							}
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				value, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "first-value", value)
			},
		},
		{
			name: "InvokerReturnsError",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					return errors.New("invoker error")
				}
			},
			contextWrapped: true,
			wantErr:        true,
		},
		{
			name: "InvokerErrorWithMetadata",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(trailerKeyPrefix+contextutil.MetadataKeyWorkflowType, "test-workflow-type")
						}
					}
					return errors.New("invoker error")
				}
			},
			contextWrapped: true,
			wantErr:        true,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				value, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "test-workflow-type", value)
			},
		},
		{
			name: "ContextNotWrapped",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(trailerKeyPrefix+contextutil.MetadataKeyWorkflowType, "test-workflow-type")
						}
					}
					return nil
				}
			},
			contextWrapped: false,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.False(t, ok)
			},
		},
		{
			name: "UnprefixedWellKnownKeysAccepted",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(
								contextutil.MetadataKeyWorkflowType, "test-workflow-type",
								contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue",
							)
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				workflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "test-workflow-type", workflowType)

				taskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
				require.True(t, ok)
				require.Equal(t, "test-task-queue", taskQueue)
			},
		},
		{
			name: "UnprefixedKeysIgnored",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(
								"some-other-key", "some-value",
								"another-key", "another-value",
							)
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				_, ok := contextutil.ContextMetadataGet(ctx, "some-other-key")
				require.False(t, ok)
				_, ok = contextutil.ContextMetadataGet(ctx, "another-key")
				require.False(t, ok)
			},
		},
		{
			name: "MixedPrefixedAndUnprefixed",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.Pairs(
								trailerKeyPrefix+contextutil.MetadataKeyWorkflowType, "test-workflow-type",
								"some-other-key", "some-value",
								trailerKeyPrefix+contextutil.MetadataKeyWorkflowTaskQueue, "test-task-queue",
								"another-key", "another-value",
							)
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				workflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.True(t, ok)
				require.Equal(t, "test-workflow-type", workflowType)

				taskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
				require.True(t, ok)
				require.Equal(t, "test-task-queue", taskQueue)

				_, ok = contextutil.ContextMetadataGet(ctx, "some-other-key")
				require.False(t, ok)
				_, ok = contextutil.ContextMetadataGet(ctx, "another-key")
				require.False(t, ok)
			},
		},
		{
			name: "EmptyTrailerValues",
			setupInvoker: func() grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					for _, opt := range opts {
						if trailer, ok := opt.(grpc.TrailerCallOption); ok {
							md := trailer.TrailerAddr
							*md = metadata.MD{
								trailerKeyPrefix + contextutil.MetadataKeyWorkflowType: []string{},
							}
						}
					}
					return nil
				}
			},
			contextWrapped: true,
			wantErr:        false,
			validateMetadata: func(t *testing.T, ctx context.Context) {
				_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
				require.False(t, ok)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			interceptor := TrailerToContextMetadataInterceptor(tl)

			invoker := tc.setupInvoker()

			var ctx context.Context
			if tc.contextWrapped {
				ctx = contextutil.WithMetadataContext(t.Context())
			} else {
				ctx = t.Context()
			}

			err := interceptor(ctx, "/test.Service/Method", "request", "reply", nil, invoker)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.validateMetadata != nil {
				tc.validateMetadata(t, ctx)
			}
		})
	}
}

func TestTrailerToContextMetadataInterceptor_PassesThroughCallOptions(t *testing.T) {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	interceptor := TrailerToContextMetadataInterceptor(tl)

	var receivedOpts []grpc.CallOption
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		receivedOpts = opts
		return nil
	}

	ctx := contextutil.WithMetadataContext(t.Context())
	err := interceptor(ctx, "/test.Service/Method", "request", "reply", nil, invoker)

	require.NoError(t, err)
	require.NotEmpty(t, receivedOpts)

	hasTrailerOption := false
	for _, opt := range receivedOpts {
		if _, ok := opt.(grpc.TrailerCallOption); ok {
			hasTrailerOption = true
			break
		}
	}
	require.True(t, hasTrailerOption)
}

func TestLogMetadataPropagationStatus(t *testing.T) {
	testCases := []struct {
		name               string
		setupContext       func() context.Context
		trailerMetadata    map[string]string
		propagatedMetadata map[string]string
	}{
		{
			name: "NoMetadata_ContextWrapped",
			setupContext: func() context.Context {
				return contextutil.WithMetadataContext(t.Context())
			},
			trailerMetadata:    make(map[string]string),
			propagatedMetadata: make(map[string]string),
		},
		{
			name: "WithMetadata_ContextNotWrapped",
			setupContext: func() context.Context {
				return t.Context()
			},
			trailerMetadata: map[string]string{
				contextutil.MetadataKeyWorkflowType: "test-workflow-type",
			},
			propagatedMetadata: make(map[string]string),
		},
		{
			name: "WithMetadata_ContextWrapped",
			setupContext: func() context.Context {
				return contextutil.WithMetadataContext(t.Context())
			},
			trailerMetadata: map[string]string{
				contextutil.MetadataKeyWorkflowType:      "test-workflow-type",
				contextutil.MetadataKeyWorkflowTaskQueue: "test-task-queue",
			},
			propagatedMetadata: map[string]string{
				contextutil.MetadataKeyWorkflowType:      "test-workflow-type",
				contextutil.MetadataKeyWorkflowTaskQueue: "test-task-queue",
			},
		},
		{
			name: "PartialPropagation_ContextWrapped",
			setupContext: func() context.Context {
				return contextutil.WithMetadataContext(t.Context())
			},
			trailerMetadata: map[string]string{
				contextutil.MetadataKeyWorkflowType:      "test-workflow-type",
				contextutil.MetadataKeyWorkflowTaskQueue: "test-task-queue",
			},
			propagatedMetadata: map[string]string{
				contextutil.MetadataKeyWorkflowType: "test-workflow-type",
			},
		},
		{
			name: "EmptyMetadata_ContextNotWrapped",
			setupContext: func() context.Context {
				return t.Context()
			},
			trailerMetadata:    make(map[string]string),
			propagatedMetadata: make(map[string]string),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tl := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			throttledLogger := log.NewThrottledLogger(tl, func() float64 {
				return 1.0
			})

			ctx := tc.setupContext()
			logMetadataPropagationStatus(ctx, "/test.Service/Method", tc.trailerMetadata, tc.propagatedMetadata, throttledLogger)
		})
	}
}
