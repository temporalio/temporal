package rpc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestErrorInterceptorPreservesResourceExhaustedRetryDelay(t *testing.T) {
	t.Parallel()

	serverStatus, err := status.New(codes.ResourceExhausted, "namespace rate limit exceeded").WithDetails(
		&errordetailspb.ResourceExhaustedFailure{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		},
		&errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)},
	)
	require.NoError(t, err)

	gotErr := errorInterceptor(context.Background(), "/test/method", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return serverStatus.Err()
		},
	)
	resourceExhaustedErr, ok := errors.AsType[*serviceerror.ResourceExhausted](gotErr)
	require.True(t, ok)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhaustedErr.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhaustedErr.Scope)
	retryInfo := retryInfoDetail(gotErr)
	require.NotNil(t, retryInfo)
	require.Equal(t, 2*time.Second, retryInfo.GetRetryDelay().AsDuration())
}

func retryInfoDetail(err error) *errdetails.RetryInfo {
	for _, detail := range serviceerror.ToStatus(err).Details() {
		if retryInfo, ok := detail.(*errdetails.RetryInfo); ok {
			return retryInfo
		}
	}
	return nil
}

func TestCreateLocalFrontendGRPCConnectionIsCached(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)
	first := f.CreateLocalFrontendGRPCConnection()
	second := f.CreateLocalFrontendGRPCConnection()
	require.NotNil(t, first)
	require.Same(t, first, second, "expected the internal frontend connection to be reused")
	t.Cleanup(func() { _ = first.Close() })
}

func TestCreateRemoteFrontendGRPCConnection_CachedPerAddress(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)

	firstA := f.CreateRemoteFrontendGRPCConnection("remote-a:7233")
	secondA := f.CreateRemoteFrontendGRPCConnection("remote-a:7233")
	require.NotNil(t, firstA)
	require.Same(t, firstA, secondA, "expected repeated calls for the same address to reuse the connection")

	connB := f.CreateRemoteFrontendGRPCConnection("remote-b:7233")
	require.NotNil(t, connB)
	require.NotSame(t, firstA, connB, "expected different addresses to get distinct connections")

	t.Cleanup(func() {
		_ = firstA.Close()
		_ = connB.Close()
	})
}

// Concurrent first callers for one rpcAddress converge on a single connection.
func TestCreateRemoteFrontendGRPCConnection_ConcurrentSameAddressConverges(t *testing.T) {
	f := NewFactory(nil, primitives.FrontendService, log.NewNoopLogger(),
		metrics.NoopMetricsHandler, nil, "localhost:7233", "", 0, nil, nil, nil, nil, nil, nil)

	const goroutines = 20
	conns := make([]*grpc.ClientConn, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			conns[i] = f.CreateRemoteFrontendGRPCConnection("remote-concurrent:7233")
		}(i)
	}
	wg.Wait()

	require.NotNil(t, conns[0])
	for i := 1; i < goroutines; i++ {
		require.Same(t, conns[0], conns[i], "expected all concurrent callers to converge on the same cached connection")
	}
	t.Cleanup(func() { _ = conns[0].Close() })
}
