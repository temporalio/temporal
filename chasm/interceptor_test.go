package chasm

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ServiceHandler struct {
	testspb.UnimplementedTestServiceServer
}

func (h ServiceHandler) Test(
	ctx context.Context,
	req *testspb.TestRequest,
) (resp *testspb.TestResponse, err error) {
	hasEngineCtx := engineFromContext(ctx) != nil

	return &testspb.TestResponse{
		RequestId:    req.RequestId,
		HasEngineCtx: hasEngineCtx,
	}, nil
}

type ServiceLibrary struct {
	UnimplementedLibrary
}

func NewServiceLibrary() *ServiceLibrary {
	return &ServiceLibrary{}
}

func (l *ServiceLibrary) RegisterServices(server *grpc.Server) {
	testspb.RegisterTestServiceServer(server, ServiceHandler{})
}

func TestChasmRequestInterceptor_ShouldRespond(t *testing.T) {
	mockEngine := NewMockEngine(gomock.NewController(t))
	requestInterceptor := ChasmRequestInterceptorProvider(mockEngine, log.NewNoopLogger(), metrics.NoopMetricsHandler)

	server, address := startTestServer(t, grpc.UnaryInterceptor(requestInterceptor.Intercept))
	defer server.Stop()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		require.NoError(t, err)
	}(conn)

	client := testspb.NewTestServiceClient(conn)

	var response *testspb.TestResponse
	response, err = client.Test(context.Background(), &testspb.TestRequest{
		RequestId: "test-request-id",
	})
	require.NoError(t, err)

	require.Equal(t, "test-request-id", response.GetRequestId())
	require.True(t, response.HasEngineCtx)
}

func startTestServer(t *testing.T, opt ...grpc.ServerOption) (*grpc.Server, string) {
	server := grpc.NewServer(opt...)
	listener, err := net.Listen("tcp", "localhost:0") // :0 picks a random available port
	if err != nil {
		panic(err)
	}

	go func() {
		err := server.Serve(listener)
		require.NoError(t, err)
	}()

	lib := NewServiceLibrary()
	lib.RegisterServices(server)

	return server, listener.Addr().String()
}
