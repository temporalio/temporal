package interceptor

import (
	"context"

	"google.golang.org/grpc"

	"go.temporal.io/server/common/contextutil"
)

type MetadataContextInterceptor struct{}

// NewMetadataContextInterceptor creates a new MetadataContextInterceptor
func NewMetadataContextInterceptor() *MetadataContextInterceptor {
	return &MetadataContextInterceptor{}
}

// Intercept adds metadata context to all incoming gRPC requests
func (m *MetadataContextInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	ctx = contextutil.AddMetadataContext(ctx)
	return handler(ctx, req)
}
