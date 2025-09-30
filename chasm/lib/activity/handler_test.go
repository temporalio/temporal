package activity

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStartActivity_ShouldSucceed(t *testing.T) {
	mockEngine := chasm.NewMockEngine(gomock.NewController(t))
	requestInterceptor := chasm.ChasmRequestInterceptorProvider(mockEngine, log.NewNoopLogger(), metrics.NoopMetricsHandler)

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

	client := activitypb.NewActivityServiceClient(conn)

	var response *activitypb.StartActivityExecutionResponse
	response, err = client.StartActivityExecution(context.Background(), &activitypb.StartActivityExecutionRequest{
		FrontendRequest: &workflowservice.StartActivityExecutionRequest{
			Namespace: "default",
			RequestId: "my-request-id",
		},
	})
	require.NoError(t, err)

	frontendResponse := response.GetFrontendResponse()
	require.NotNil(t, frontendResponse)
	require.True(t, frontendResponse.GetStarted())
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

	lib := NewLibrary()
	err = lib.RegisterServices(server)
	require.NoError(t, err)

	return server, listener.Addr().String()
}
