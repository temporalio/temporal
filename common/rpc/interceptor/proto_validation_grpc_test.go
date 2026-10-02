package interceptor

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type protoValidationTestServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	calls atomic.Int64
}

func (s *protoValidationTestServer) StartNexusOperationExecution(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	s.calls.Add(1)
	return &workflowservice.StartNexusOperationExecutionResponse{}, nil
}

func TestProtoValidationInterceptorGRPC(t *testing.T) {
	t.Parallel()

	core, entries := observer.New(zap.ErrorLevel)
	validation, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)))
	require.NoError(t, err)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		NewServiceErrorInterceptor(func() int { return 1000 }).Intercept,
		validation.Intercept,
	))
	service := &protoValidationTestServer{}
	workflowservice.RegisterWorkflowServiceServer(server, service)
	listener := bufconn.Listen(1024 * 1024)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveResult)
	})
	connection, err := grpc.NewClient("passthrough:///proto-validation",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := workflowservice.NewWorkflowServiceClient(connection)
	_, err = client.StartNexusOperationExecution(t.Context(), &workflowservice.StartNexusOperationExecutionRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, service.calls.Load())
	require.Empty(t, entries.All())

	response, err := client.StartNexusOperationExecution(t.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace: "ns", OperationId: "id", Endpoint: "endpoint", Service: "service", Operation: "operation",
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &workflowservice.StartNexusOperationExecutionResponse{}, response)
	require.EqualValues(t, 1, service.calls.Load())
	require.Len(t, entries.All(), 1)
	require.Equal(t, true, entries.All()[0].ContextMap()["failed-assertion"])
}
