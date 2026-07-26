//go:build !test_dep

package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAddNonNilResponseInterceptorNoop(t *testing.T) {
	handlerResp := &emptypb.Empty{}
	sentinelCalled := false
	sentinel := func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		sentinelCalled = true
		return handler(ctx, req)
	}

	interceptors := AddNonNilResponseInterceptor(
		[]grpc.UnaryServerInterceptor{sentinel},
		log.NewNoopLogger(),
	)
	require.Len(t, interceptors, 1)

	resp, err := interceptors[0](t.Context(), nil, nil, func(context.Context, any) (any, error) {
		return handlerResp, nil
	})
	require.NoError(t, err)
	require.True(t, sentinelCalled)
	require.Same(t, handlerResp, resp)
}
