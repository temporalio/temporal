//go:build test_dep

package failpoint_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/failpoint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestFailpointIntegration_InjectAndExecutePanic(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointservice.RegisterFailpointServiceServer(grpcServer, registry)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointservice.NewFailpointServiceClient(conn)

	// Inject panic failpoint
	_, err = client.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "integration test panic",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// Verify it was registered
	fp := registry.Get(1)
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
	failpointservice.RegisterFailpointServiceServer(grpcServer, registry)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointservice.NewFailpointServiceClient(conn)

	// Inject return failpoint
	_, err = client.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "integration test error",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	// Execute it multiple times (Always recurrence)
	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
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
	failpointservice.RegisterFailpointServiceServer(grpcServer, registry)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointservice.NewFailpointServiceClient(conn)

	_, err = client.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        1,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	waitDone := make(chan error, 1)
	go func() {
		_, err := client.WaitForBlock(t.Context(), &failpointservice.WaitForBlockRequest{Key: 1})
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
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, waitErr)

	fp.UnblockOne()
	<-executeDone
}

func TestFailpointIntegration_ListFailpoints(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointservice.RegisterFailpointServiceServer(grpcServer, registry)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointservice.NewFailpointServiceClient(conn)

	// Inject some failpoints
	_, err = client.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "test",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = client.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        2,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	_, err = client.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          3,
		ErrorMessage: "error",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// List them
	resp, err := client.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 3)

	// Verify structure
	for _, fp := range resp.Failpoints {
		require.NotEqual(t, int32(0), fp.Key)
		require.NotEmpty(t, fp.Action)
	}
}

func TestFailpointIntegration_DisableFailpoint(t *testing.T) {
	registry := failpoint.NewRegistry()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	grpcServer := grpc.NewServer()
	failpointservice.RegisterFailpointServiceServer(grpcServer, registry)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := failpointservice.NewFailpointServiceClient(conn)

	// Inject a failpoint
	_, err = client.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "will be disabled",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	// Verify it works
	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok, "expected *Failpoint[error]")
	_, executed := fp.Execute()
	require.True(t, executed)

	// Disable it
	_, err = client.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)

	// Verify it's disabled
	_, executed = fp.Execute()
	require.False(t, executed)
}

// TestFailpointIntegration_ContextPropagation tests that registry is properly
// injected into contexts and can be used by ExecuteCtx.
func TestFailpointIntegration_ContextPropagation(t *testing.T) {
	// Simulate what a server instance does
	registry := failpoint.NewRegistry()

	// Inject a failpoint via registry
	_, err := registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "context test",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// Simulate what the interceptor does
	ctx := failpoint.InjectRegistry(t.Context(), registry)

	// Now code using ExecuteCtx should see the failpoint
	result, executed := failpoint.ExecuteCtx[error](ctx, 1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "context test", result.Error())

	// Second execution should not trigger (Once)
	result, executed = failpoint.ExecuteCtx[error](ctx, 1)
	require.False(t, executed)
	require.NoError(t, result)
}

// TestFailpointIntegration_MultipleServerInstances ensures that different
// registries can coexist (simulating multiple server instances in tests).
func TestFailpointIntegration_MultipleServerInstances(t *testing.T) {
	registry1 := failpoint.NewRegistry()
	registry2 := failpoint.NewRegistry()

	// Configure different failpoints in each registry
	_, err := registry1.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "from registry 1",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = registry2.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "from registry 2",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// Create contexts for each
	ctx1 := failpoint.InjectRegistry(t.Context(), registry1)
	ctx2 := failpoint.InjectRegistry(t.Context(), registry2)

	// Execute with each context
	result1, executed1 := failpoint.ExecuteCtx[error](ctx1, 1)
	require.True(t, executed1)
	require.Equal(t, "from registry 1", result1.Error())

	result2, executed2 := failpoint.ExecuteCtx[error](ctx2, 1)
	require.True(t, executed2)
	require.Equal(t, "from registry 2", result2.Error())
}
