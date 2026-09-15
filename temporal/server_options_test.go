package temporal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.temporal.io/server/service/frontend"
	"google.golang.org/grpc"
)

type testFrontendInterceptor struct{}

func (testFrontendInterceptor) Intercept(
	context.Context,
	any,
	*grpc.UnaryServerInfo,
	grpc.UnaryHandler,
) (any, error) {
	return nil, nil
}

func (testFrontendInterceptor) InterceptNexus(
	context.Context,
	nexus.InterceptorInput,
	nexus.HandlerFunc,
) (any, error) {
	return nil, nil
}

var _ frontend.Interceptor = testFrontendInterceptor{}

func TestServerOptionsRejectsBothFrontendInterceptorOptions(t *testing.T) {
	options := serverOptions{
		customFrontendInterceptors: []grpc.UnaryServerInterceptor{
			func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				return nil, nil
			},
		},
		customFrontendUnifiedInterceptors: []frontend.Interceptor{testFrontendInterceptor{}},
	}

	err := options.validateConfig()
	require.EqualError(t, err, "WithChainedFrontendGrpcInterceptors is deprecated in favor of WithChainedFrontendInterceptors- they cannot both be set")
}
