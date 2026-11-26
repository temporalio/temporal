package failpointgrpc_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestFailpointIntegration_InjectAndExecutePanic(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointpb.NewFailpointServiceClient(conn)

	// Inject panic failpoint
	_, err = client.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
		PanicValue: "integration test panic",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// Verify it was registered
	fp := registry.Get(key1)
	require.NotNil(t, fp)

	// Verify it panics
	typedFP, ok := fp.(*failpoint.Failpoint[error])
	require.True(t, ok, "expected *Failpoint[error]")
	require.Panics(t, func() {
		_, _ = typedFP.Execute()
	})
}

func TestFailpointIntegration_InjectAndExecuteReturn(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointpb.NewFailpointServiceClient(conn)

	// Inject return failpoint
	_, err = client.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "integration test error",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	// Execute it multiple times (Always recurrence)
	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok, "expected *Failpoint[error]")
	for range 3 {
		result, executed := fp.Execute()
		require.True(t, executed)
		require.Error(t, result)
		require.Equal(t, "integration test error", result.Error())
	}
}

func TestFailpointIntegration_InjectBlockAndWait(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointpb.NewFailpointServiceClient(conn)

	_, err = client.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Name:       "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	waitDone := make(chan error, 1)
	go func() {
		_, err := client.WaitForBlock(t.Context(), &failpointpb.WaitForBlockRequest{Name: "1"})
		waitDone <- err
	}()

	// Verify WaitForBlock hasn't completed yet (still waiting)
	require.Never(t, func() bool {
		select {
		case <-waitDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	executeDone := make(chan struct{})
	go func() {
		defer close(executeDone)
		_, _ = fp.Execute()
	}()

	var waitErr error
	await.RequireTrue(t, func() bool {
		select {
		case waitErr = <-waitDone:
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, waitErr)

	fp.Disable()
	<-executeDone
}

func TestFailpointIntegration_ListFailpoints(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointpb.NewFailpointServiceClient(conn)

	// Inject some failpoints
	_, err = client.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
		PanicValue: "test",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = client.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Name:       "2",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	_, err = client.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "3",
		ErrorMessage: "error",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// List them
	resp, err := client.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 3)

	// Verify structure
	for _, fp := range resp.Failpoints {
		require.NotEqual(t, int32(0), fp.Name)
		require.NotEmpty(t, fp.Action)
	}
}

func TestFailpointIntegration_DisableFailpoint(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointpb.RegisterFailpointServiceServer(grpcServer, failpointgrpc.NewServer(registry))
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointpb.NewFailpointServiceClient(conn)

	// Inject a failpoint
	_, err = client.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "will be disabled",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	// Verify it works
	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok, "expected *Failpoint[error]")
	_, executed := fp.Execute()
	require.True(t, executed)

	// Disable it
	_, err = client.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)

	// Verify it's disabled
	_, executed = fp.Execute()
	require.False(t, executed)
}

// TestFailpointIntegration_Execute tests that a call site that executes the key with the registry
// as its source sees the failpoint that the service configured.
func TestFailpointIntegration_Execute(t *testing.T) {
	registry := failpoint.NewRegistry()

	// Inject a failpoint via registry
	_, err := failpointgrpc.NewServer(registry).InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "context test",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	result, executed := failpoint.Execute(registry, key1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "context test", result.Error())

	// Second execution should not trigger (Once)
	result, executed = failpoint.Execute(registry, key1)
	require.False(t, executed)
	require.NoError(t, result)
}

// TestFailpointIntegration_MultipleServerInstances ensures that different
// registries can coexist (simulating multiple server instances in tests).
func TestFailpointIntegration_MultipleServerInstances(t *testing.T) {
	registry1 := failpoint.NewRegistry()
	registry2 := failpoint.NewRegistry()

	// Configure different failpoints in each registry
	_, err := failpointgrpc.NewServer(registry1).InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "from registry 1",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = failpointgrpc.NewServer(registry2).InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "from registry 2",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	result1, executed1 := failpoint.Execute(registry1, key1)
	require.True(t, executed1)
	require.Equal(t, "from registry 1", result1.Error())

	result2, executed2 := failpoint.Execute(registry2, key1)
	require.True(t, executed2)
	require.Equal(t, "from registry 2", result2.Error())
}
