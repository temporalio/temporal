//go:build test_dep

package failpointgrpc_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Low-level registry operations

func TestServer_List(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key2)

	registry.Set(key1, fp1)
	registry.Set(key2, fp2)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 2)

	keys := map[string]bool{}
	for _, info := range resp.Failpoints {
		keys[info.Key] = true
	}
	require.True(t, keys["1"] && keys["2"])
}

// gRPC service methods

func TestServer_InjectPanic(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Key:        "1",
		PanicValue: "test panic",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	require.Panics(t, func() { _, _ = fp.Execute() })
}

func TestServer_InjectBlock(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Key:        "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	blocked := make(chan struct{})
	go func() {
		_, _ = fp.Execute()
		close(blocked)
	}()

	// Verify it's blocked
	require.Never(t, func() bool {
		select {
		case <-blocked:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)

	fp.UnblockOne()

	// Verify it unblocked
	await.RequireTrue(t, func() bool {
		select {
		case <-blocked:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestServer_InjectReturn(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "1",
		ErrorMessage: "test error",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	result, executed := fp.Execute()
	require.True(t, executed)
	require.EqualError(t, result, "test error")
}

func TestServer_Disable(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "1",
		ErrorMessage: "test error",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	_, executed := fp.Execute()
	require.True(t, executed)

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)

	// After disable, the failpoint should be removed from registry
	require.Nil(t, registry.Get(key1))
}

func TestServer_DisableAll(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	// Inject multiple failpoints
	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Key:        "1",
		PanicValue: "panic1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "2",
		ErrorMessage: "error1",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 2)

	// Disable all
	_, err = server.DisableAllFailpoints(t.Context(), &failpointpb.DisableAllFailpointsRequest{})
	require.NoError(t, err)

	// Verify all are gone
	resp, err = server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Failpoints)
}

func TestServer_WaitForBlock(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Key:        "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	waitDone := make(chan error, 1)
	go func() {
		_, err := server.WaitForBlock(t.Context(), &failpointpb.WaitForBlockRequest{Key: "1"})
		waitDone <- err
	}()

	// Verify WaitForBlock hasn't completed yet
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

func TestServer_ListFailpoints(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Key:        "1",
		PanicValue: "panic1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Key:        "2",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "3",
		ErrorMessage: "error1",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 3)

	keys := map[string]bool{}
	for _, fp := range resp.Failpoints {
		keys[fp.Key] = true
		require.True(t, fp.Enabled)
	}
	require.True(t, keys["1"] && keys["2"] && keys["3"])
}

func TestServer_RecurrenceConversion(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	tests := []struct {
		proto    failpointpb.Recurrence
		expected failpointpb.Recurrence
	}{
		{failpointpb.Recurrence_RECURRENCE_ONCE, failpointpb.Recurrence_RECURRENCE_ONCE},
		{failpointpb.Recurrence_RECURRENCE_ALWAYS, failpointpb.Recurrence_RECURRENCE_ALWAYS},
		{failpointpb.Recurrence_RECURRENCE_UNSPECIFIED, failpointpb.Recurrence_RECURRENCE_ALWAYS},
	}

	for _, tt := range tests {
		_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
			Key:        "100",
			PanicValue: "test",
			Recurrence: tt.proto,
		})
		require.NoError(t, err)

		resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Failpoints, 1)
		require.Equal(t, tt.expected, resp.Failpoints[0].Recurrence)

		_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "100"})
		require.NoError(t, err)
	}
}

func TestServer_GetInfo(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Key:        "1",
		PanicValue: "test",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 1)
	info := resp.Failpoints[0]
	require.Equal(t, "1", info.Key)
	require.True(t, info.Enabled)
	require.Equal(t, failpointpb.Action_ACTION_PANIC, info.Action)
	require.Equal(t, failpointpb.Recurrence_RECURRENCE_ONCE, info.Recurrence)

	// Disable and check again - it should be removed from the registry
	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)

	resp, err = server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Failpoints)
}

// Context injection functionality

// TestRegistry_DisableFailpoint_Idempotent verifies that disabling a
// non-existent or already-disabled failpoint is a no-op rather than an error.
// Without this, cleanup hooks that disable defensively flap with NotFound.
func TestServer_DisableFailpoint_Idempotent(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "1",
		ErrorMessage: "x",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)

	// Second disable on the same key must also succeed.
	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)
}

// TestRegistry_Inject_RejectsUnsetKey verifies that requests omitting the key
// (proto zero value) are rejected with InvalidArgument. Otherwise the registry
// would happily store a failpoint at key 0 that no ExecuteCtx call could match.
func TestServer_Inject_RejectsUnsetKey(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	ctx := t.Context()

	_, err := server.InjectPanic(ctx, &failpointpb.InjectPanicRequest{
		PanicValue: "x",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = server.InjectBlock(ctx, &failpointpb.InjectBlockRequest{
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{
		ErrorMessage: "x",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestRegistry_WaitForBlock_DisabledMidWait verifies that the gRPC WaitForBlock
// path tolerates a concurrent Disable: the wait completes (via the closed
// arrivedCh that disableLocked produces) instead of panicking inside WaitCh.
func TestServer_WaitForBlock_DisabledMidWait(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Key:        "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	waitStarted := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		close(waitStarted)
		_, err := server.WaitForBlock(t.Context(), &failpointpb.WaitForBlockRequest{Key: "1"})
		waitDone <- err
	}()

	<-waitStarted
	// The waiter is in WaitForBlock; assert it stays parked, then disable.
	require.Never(t, func() bool {
		select {
		case <-waitDone:
			return true
		default:
			return false
		}
	}, 50*time.Millisecond, 10*time.Millisecond)

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Key: "1"})
	require.NoError(t, err)

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
}

func TestServer_NamespaceScopedRequest(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "1",
		NamespaceId:  "ns-1",
		ErrorMessage: "scoped",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	got, fired := failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key1.ForNamespace("ns-1"))
	require.True(t, fired)
	require.EqualError(t, got, "scoped")

	_, fired = failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key1.ForNamespace("ns-2"))
	require.False(t, fired)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 1)
	require.Equal(t, "1", resp.Failpoints[0].Key)
	require.Equal(t, "ns-1", resp.Failpoints[0].NamespaceId)
}

func TestServer_InjectReturnErrorName(t *testing.T) {
	sentinel := errors.New("sentinel")
	failpoint.RegisterError("TestServer_InjectReturnErrorName", func(msg string) error {
		return fmt.Errorf("%s: %w", msg, sentinel)
	})
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Key:          "1",
		ErrorName:    "TestServer_InjectReturnErrorName",
		ErrorMessage: "injected",
	})
	require.NoError(t, err)
	got, fired := failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key1)
	require.True(t, fired)
	require.ErrorIs(t, got, sentinel)
	require.EqualError(t, got, "injected: sentinel")

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{Key: "1", ErrorName: "NoSuchError"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
