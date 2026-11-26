package failpointgrpc_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Low-level registry operations

func TestServer_List(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key2)

	registry.Set(fp1)
	registry.Set(fp2)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 2)

	keys := map[string]bool{}
	for _, info := range resp.Failpoints {
		keys[info.Name] = true
	}
	require.True(t, keys["1"] && keys["2"])
}

// gRPC service methods

func TestServer_InjectPanic(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
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
		Name:       "1",
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

	fp.Disable()

	// Verify it unblocked
	await.RequireTrue(t, func() bool {
		select {
		case <-blocked:
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
}

func TestServer_InjectReturn(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
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
		Name:         "1",
		ErrorMessage: "test error",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	_, executed := fp.Execute()
	require.True(t, executed)

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)

	// After disable, the failpoint should be removed from registry
	require.Nil(t, registry.Get(key1))
}

func TestServer_DisableAll(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	// Inject multiple failpoints
	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
		PanicValue: "panic1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "2",
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
		Name:       "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	waitDone := make(chan error, 1)
	go func() {
		_, err := server.WaitForBlock(t.Context(), &failpointpb.WaitForBlockRequest{Name: "1"})
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
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, waitErr)

	fp.Disable()
	<-executeDone
}

// An injection into a failpoint that a test registered replaces its trigger and counts, so a
// failpoint whose MaxFires is spent blocks the next execution.
func TestServer_InjectReplacesTheTriggerOfAnUnmanagedFailpoint(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	fp := failpoint.NewFailpoint[error](key1)
	fp.Return(errors.New("registered by a test"), failpoint.Trigger{MaxFires: 1})
	registry.Set(fp)
	_, fired := fp.Execute()
	require.True(t, fired)

	_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{Name: "1"})
	require.NoError(t, err)

	executeDone := make(chan struct{})
	go func() {
		defer close(executeDone)
		_, _ = fp.Execute()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = server.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: "1"})
	require.NoError(t, err)

	info := fp.Info()
	require.Equal(t, failpoint.ActionBlock, info.Action)
	require.Equal(t, failpoint.Trigger{}, info.Trigger)

	fp.Disable()
	<-executeDone
}

func TestServer_ListFailpoints(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
		PanicValue: "panic1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Name:       "2",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "3",
		ErrorMessage: "error1",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 3)

	keys := map[string]bool{}
	for _, fp := range resp.Failpoints {
		keys[fp.Name] = true
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
		trigger  failpoint.Trigger
	}{
		{failpointpb.Recurrence_RECURRENCE_ONCE, failpointpb.Recurrence_RECURRENCE_ONCE, failpoint.Trigger{MaxFires: 1}},
		{failpointpb.Recurrence_RECURRENCE_ALWAYS, failpointpb.Recurrence_RECURRENCE_ALWAYS, failpoint.Trigger{}},
		{failpointpb.Recurrence_RECURRENCE_UNSPECIFIED, failpointpb.Recurrence_RECURRENCE_ALWAYS, failpoint.Trigger{}},
	}

	for _, tt := range tests {
		_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
			Name:       "100",
			PanicValue: "test",
			Recurrence: tt.proto,
		})
		require.NoError(t, err)

		resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Failpoints, 1)
		require.Equal(t, tt.expected, resp.Failpoints[0].Recurrence)
		require.Equal(t, tt.trigger, registry.Infos()[0].Trigger, "the recurrence is the trigger of the failpoint")

		_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "100"})
		require.NoError(t, err)
	}
}

func TestServer_GetInfo(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{
		Name:       "1",
		PanicValue: "test",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 1)
	info := resp.Failpoints[0]
	require.Equal(t, "1", info.Name)
	require.True(t, info.Enabled)
	require.Equal(t, failpointpb.Action_ACTION_PANIC, info.Action)
	require.Equal(t, failpointpb.Recurrence_RECURRENCE_ONCE, info.Recurrence)

	// Disable and check again - it should be removed from the registry
	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
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

	_, err := server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorMessage: "x",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)

	// Second disable on the same key must also succeed.
	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
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
// arrivedCh that disableLocked produces) instead of panicking, and reports that
// no execution reached the block.
func TestServer_WaitForBlock_DisabledMidWait(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{
		Name:       "1",
		Recurrence: failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	waitStarted := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		close(waitStarted)
		_, err := server.WaitForBlock(t.Context(), &failpointpb.WaitForBlockRequest{Name: "1"})
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

	_, err = server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)

	var waitErr error
	await.RequireTrue(t, func() bool {
		select {
		case waitErr = <-waitDone:
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, codes.FailedPrecondition, status.Code(waitErr))
}

func TestServer_WaitForBlock_NotBlocking(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := server.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: "1"})
	require.Equal(t, codes.NotFound, status.Code(err))

	_, err = server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Name: "1", ErrorMessage: "x"})
	require.NoError(t, err)
	_, err = server.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: "1"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestServer_SharedRegistry covers servers that serve the same registry, as the services of one
// process do: an injection through the second server, after an execution reached the block that
// the first server injected, configures the same failpoint, so it reaches the blocked execution
// instead of stranding it on a failpoint that is no longer registered.
func TestServer_SharedRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	servers := []failpointpb.FailpointServiceServer{failpointgrpc.NewServer(registry), failpointgrpc.NewServer(registry)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := servers[0].InjectBlock(ctx, &failpointpb.InjectBlockRequest{Name: "1"})
	require.NoError(t, err)
	installed, ok := registry.Get(key1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	executeDone := make(chan struct{})
	t.Cleanup(func() {
		installed.Disable()
		<-executeDone
	})
	go func() {
		defer close(executeDone)
		_, _ = installed.Execute()
	}()
	_, err = servers[0].WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: "1"})
	require.NoError(t, err)

	// Re-arming the installed failpoint releases the execution that is blocked on it.
	_, err = servers[1].InjectBlock(ctx, &failpointpb.InjectBlockRequest{Name: "1"})
	require.NoError(t, err)
	require.Same(t, installed, registry.Get(key1))
	select {
	case <-executeDone:
	case <-ctx.Done():
		require.FailNow(t, "the second injection did not reach the blocked execution")
	}

	_, err = servers[1].DisableFailpoint(ctx, &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)
	require.Empty(t, registry.Infos())
}

// TestServer_SharedRegistry_CompetingFirstInjections covers the first injections of a key through
// two servers of one registry at the same time: both must configure the one failpoint that gets
// installed.
func TestServer_SharedRegistry_CompetingFirstInjections(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range 20 {
		registry := failpoint.NewRegistry()
		servers := []failpointpb.FailpointServiceServer{failpointgrpc.NewServer(registry), failpointgrpc.NewServer(registry)}

		start := make(chan struct{})
		errs := make([]error, len(servers))
		var wg sync.WaitGroup
		for i, server := range servers {
			wg.Go(func() {
				<-start
				_, errs[i] = server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Name: "1", ErrorMessage: "injected"})
			})
		}
		close(start)
		wg.Wait()
		require.NoError(t, errors.Join(errs...))

		require.Equal(t, []failpoint.Info{{Key: key1, Action: failpoint.ActionReturn}}, registry.Infos())
	}
}

func TestServer_DescribeRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	resp1, err := failpointgrpc.NewServer(registry).DescribeRegistry(t.Context(), &failpointpb.DescribeRegistryRequest{})
	require.NoError(t, err)
	resp2, err := failpointgrpc.NewServer(registry).DescribeRegistry(t.Context(), &failpointpb.DescribeRegistryRequest{})
	require.NoError(t, err)
	resp3, err := failpointgrpc.NewServer(failpoint.NewRegistry()).DescribeRegistry(t.Context(), &failpointpb.DescribeRegistryRequest{})
	require.NoError(t, err)

	require.NotEmpty(t, resp1.RegistryId)
	require.Equal(t, resp1.RegistryId, resp2.RegistryId)
	require.NotEqual(t, resp1.RegistryId, resp3.RegistryId)
}

func TestServer_ScopedRequest(t *testing.T) {
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		Scope:        "ns-1",
		ErrorMessage: "scoped",
		Recurrence:   failpointpb.Recurrence_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	got, fired := failpoint.Execute(registry, key1.ForScope("ns-1"))
	require.True(t, fired)
	require.EqualError(t, got, "scoped")

	_, fired = failpoint.Execute(registry, key1.ForScope("ns-2"))
	require.False(t, fired)

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 1)
	require.Equal(t, "1", resp.Failpoints[0].Name)
	require.Equal(t, "ns-1", resp.Failpoints[0].Scope)
}

func TestServer_InjectReturnErrorName(t *testing.T) {
	sentinel := errors.New("sentinel")
	// The catalog is global, so each run registers a name of its own.
	name := "TestServer_InjectReturnErrorName_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return fmt.Errorf("%s: %w", msg, sentinel)
	})
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)

	_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{
		Name:         "1",
		ErrorName:    name,
		ErrorMessage: "injected",
	})
	require.NoError(t, err)
	got, fired := failpoint.Execute(registry, key1)
	require.True(t, fired)
	require.ErrorIs(t, got, sentinel)
	require.EqualError(t, got, "injected: sentinel")

	_, err = server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{Name: "1", ErrorName: "NoSuchError"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServer_WithoutRegistry(t *testing.T) {
	t.Parallel()
	server := failpointgrpc.NewServer(nil)
	ctx := t.Context()

	_, err := server.DescribeRegistry(ctx, &failpointpb.DescribeRegistryRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Name: "1"})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = server.ListFailpoints(ctx, &failpointpb.ListFailpointsRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = server.DisableAllFailpoints(ctx, &failpointpb.DisableAllFailpointsRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServer_DisableReleasesTheBlockedExecution(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	fp := failpoint.NewFailpoint[error](key1)
	fp.Block()
	registry.Set(fp)
	executeDone := make(chan struct{})
	t.Cleanup(func() {
		fp.Disable()
		<-executeDone
	})
	go func() {
		defer close(executeDone)
		_, _ = fp.Execute()
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, fp.Wait(ctx))

	_, err := server.DisableFailpoint(ctx, &failpointpb.DisableFailpointRequest{Name: "1"})
	require.NoError(t, err)
	select {
	case <-executeDone: // The blocked execution is released.
	case <-ctx.Done():
		require.FailNow(t, "disable did not release the blocked execution")
	}
	require.Nil(t, registry.Get(key1))
}

func TestServer_RejectsKeysThatAreNotErrorKeys(t *testing.T) {
	t.Parallel()
	server := failpointgrpc.NewServer(failpoint.NewRegistry())
	ctx := t.Context()

	for name, wantMessage := range map[string]string{
		intKey.Name():    `has the value type int, not error`,
		"test.nosuchkey": `unknown key "test.nosuchkey"`,
	} {
		_, err := server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Name: name})
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
		require.ErrorContains(t, err, wantMessage)
		_, err = server.DisableFailpoint(ctx, &failpointpb.DisableFailpointRequest{Name: name})
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
		_, err = server.WaitForBlock(ctx, &failpointpb.WaitForBlockRequest{Name: name})
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
}

func TestServer_RejectsUnknownRecurrence(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	ctx := t.Context()
	unknown := failpointpb.Recurrence(99)

	_, err := server.InjectPanic(ctx, &failpointpb.InjectPanicRequest{Name: "1", Recurrence: unknown})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.InjectBlock(ctx, &failpointpb.InjectBlockRequest{Name: "1", Recurrence: unknown})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = server.InjectReturn(ctx, &failpointpb.InjectReturnRequest{Name: "1", Recurrence: unknown})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, registry.Infos(), "a rejected request registers nothing")
}

func TestServer_LeavesRuleFailpoints(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	server := failpointgrpc.NewServer(registry)
	set := failpoint.NewRuleSet(registry)
	t.Cleanup(set.Close)
	_, err := set.Apply([]failpoint.Rule{{Key: "rule", Action: failpoint.ActionOn}})
	require.NoError(t, err)
	ruleFP := registry.Get(ruleKey)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "InjectReturn", call: func() error {
			_, err := server.InjectReturn(t.Context(), &failpointpb.InjectReturnRequest{Name: "rule", ErrorMessage: "injected"})
			return err
		}},
		{name: "InjectPanic", call: func() error {
			_, err := server.InjectPanic(t.Context(), &failpointpb.InjectPanicRequest{Name: "rule", PanicValue: "injected"})
			return err
		}},
		{name: "InjectBlock", call: func() error {
			_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{Name: "rule"})
			return err
		}},
		{name: "DisableFailpoint", call: func() error {
			_, err := server.DisableFailpoint(t.Context(), &failpointpb.DisableFailpointRequest{Name: "rule"})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.ErrorContains(t, err, "managed by failpoint rules")
			require.Same(t, ruleFP, registry.Get(ruleKey))
			require.True(t, failpoint.ExecuteSwitch(registry, ruleKey))
		})
	}

	t.Run("DisableAllFailpoints", func(t *testing.T) {
		_, err := server.InjectBlock(t.Context(), &failpointpb.InjectBlockRequest{Name: "1"})
		require.NoError(t, err)
		_, err = server.DisableAllFailpoints(t.Context(), &failpointpb.DisableAllFailpointsRequest{})
		require.NoError(t, err)
		require.Equal(t, []string{ruleKey.String()}, infoKeys(registry))
		require.True(t, failpoint.ExecuteSwitch(registry, ruleKey))
	})
}

// statsKeys are the keys of the BUGGIFY counters that the tests below make.
var statsKeys = failpoint.ReserveKeyPrefix[error]("stats.")

// useStatsBuggify configures BUGGIFY in registry, activating the names with the prefix "stats.on"
// and no others.
func useStatsBuggify(t *testing.T, registry *failpoint.Registry) *buggify.Buggify {
	t.Helper()
	b := buggify.New(registry)
	require.NoError(t, b.Set(buggify.Config{
		Seed: 1,
		Keys: []buggify.Key{{Match: "stats.on*", ActivationProbability: 1, FireProbability: 1}, {Match: "stats.*"}},
	}))
	return b
}

// buggifyRegistry returns a registry with BUGGIFY from useStatsBuggify.
func buggifyRegistry(t *testing.T) (*failpoint.Registry, *buggify.Buggify) {
	t.Helper()
	registry := failpoint.NewRegistry()
	return registry, useStatsBuggify(t, registry)
}

func TestServer_ListFailpointsFallbackKeys(t *testing.T) {
	t.Parallel()
	registry, b := buggifyRegistry(t)
	server := failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b))
	require.False(t, failpoint.ExecuteSwitch(registry, statsKeys.Key("off")))
	require.True(t, failpoint.ExecuteSwitch(registry, statsKeys.Key("on")))
	require.True(t, failpoint.ExecuteSwitch(registry, statsKeys.Key("on")))

	resp, err := server.ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Failpoints)
	require.Len(t, resp.FallbackKeys, 2)
	require.True(t, proto.Equal(&failpointpb.FallbackKeyInfo{Name: "stats.off", Activated: false, Executions: 1}, resp.FallbackKeys[0]), resp.FallbackKeys[0])
	require.True(t, proto.Equal(&failpointpb.FallbackKeyInfo{Name: "stats.on", Activated: true, Executions: 2, Fires: 2}, resp.FallbackKeys[1]), resp.FallbackKeys[1])

	// A registry without BUGGIFY reports nothing.
	resp, err = failpointgrpc.NewServer(failpoint.NewRegistry()).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.FallbackKeys)
}

// A registry can hold failpoints and counters with arbitrarily many or long names, so the listing
// is kept within a limit and says what it leaves out.
func TestServer_ListFailpointsIsBounded(t *testing.T) {
	t.Parallel()

	t.Run("the default limit keeps the response below what a client receives", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		const names = 6
		for i := range names {
			registry.Set(failpoint.NewFailpoint(statsKeys.Key(fmt.Sprintf("%d%s", i, strings.Repeat("x", 1<<20)))))
		}

		resp, err := failpointgrpc.NewServer(registry).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Less(t, proto.Size(resp), 4<<20, "gRPC receives 4 MiB by default")
		require.NotEmpty(t, resp.Failpoints)
		require.Equal(t, int64(names), int64(len(resp.Failpoints))+resp.FailpointsOmitted)
		require.Positive(t, resp.FailpointsOmitted)
		require.Zero(t, resp.FallbackKeysOmitted)
	})

	t.Run("failpoints keep their place before the counters", func(t *testing.T) {
		t.Parallel()
		registry, b := buggifyRegistry(t)
		for i := range 10 {
			registry.Set(failpoint.NewFailpoint(statsKeys.Key(fmt.Sprintf("fp.%02d", i))))
			require.False(t, failpoint.ExecuteSwitch(registry, statsKeys.Key(fmt.Sprintf("count.%02d", i))))
		}
		all, err := failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b)).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Len(t, all.Failpoints, 10)
		require.Len(t, all.FallbackKeys, 10)
		require.Zero(t, all.FailpointsOmitted+all.FallbackKeysOmitted)

		// The limit fits all failpoints and a part of the counters.
		limit := proto.Size(&failpointpb.ListFailpointsResponse{Failpoints: all.Failpoints}) + 10*6 + proto.Size(all.FallbackKeys[0])*3
		resp, err := failpointgrpc.NewServerWithListLimit(registry, limit, failpointgrpc.WithBuggify(b)).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Failpoints, 10)
		require.Zero(t, resp.FailpointsOmitted)
		require.NotEmpty(t, resp.FallbackKeys)
		require.Less(t, len(resp.FallbackKeys), 10)
		require.Equal(t, int64(10-len(resp.FallbackKeys)), resp.FallbackKeysOmitted)
		for i, info := range resp.FallbackKeys {
			require.True(t, proto.Equal(all.FallbackKeys[i], info), "the entries kept are a prefix of the list")
		}

		// A limit below the failpoints omits counters and some failpoints.
		resp, err = failpointgrpc.NewServerWithListLimit(registry, 100, failpointgrpc.WithBuggify(b)).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Empty(t, resp.FallbackKeys)
		require.Equal(t, int64(10), resp.FallbackKeysOmitted)
		require.Equal(t, int64(10), int64(len(resp.Failpoints))+resp.FailpointsOmitted)
		require.Positive(t, resp.FailpointsOmitted)
	})
}

func TestServer_ListFailpointsSanitizesNames(t *testing.T) {
	t.Parallel()
	registry, b := buggifyRegistry(t)
	// An HTTP path such as /%FF unescapes to invalid UTF-8.
	name := "\xff"
	require.True(t, failpoint.ExecuteSwitch(registry, statsKeys.Key("on"+name)))
	fp := failpoint.NewFailpoint(statsKeys.Key(name).ForScope("scope\xfe"))
	registry.Set(fp)

	resp, err := failpointgrpc.NewServer(registry, failpointgrpc.WithBuggify(b)).ListFailpoints(t.Context(), &failpointpb.ListFailpointsRequest{})
	require.NoError(t, err)
	_, err = proto.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, "stats.on�", resp.FallbackKeys[0].Name)
	require.Equal(t, "stats.\ufffd", resp.Failpoints[0].Name)
	require.Equal(t, "scope�", resp.Failpoints[0].Scope)
}

// infoKeys returns the keys of the failpoints of the registry as strings.
func infoKeys(registry *failpoint.Registry) []string {
	var keys []string
	for _, info := range registry.Infos() {
		keys = append(keys, info.Key.String())
	}
	return keys
}

// TestProtoEnumsMatchTheFailpointConstants checks that every failpoint.Action has a matching
// failpointpb.Action (plus ACTION_UNSPECIFIED for proto3) so actionProto stays in sync.
func TestProtoEnumsMatchTheFailpointConstants(t *testing.T) {
	t.Parallel()

	t.Run("action", func(t *testing.T) {
		t.Parallel()
		// Every action that has a name is in the proto enum under the same name, and every proto
		// enum value except ACTION_UNSPECIFIED corresponds to a failpoint.Action.
		var actions int
		for action := failpoint.Action(0); !strings.HasPrefix(action.String(), "Action("); action++ {
			actions++
			want := "ACTION_" + strings.ToUpper(action.String())
			_, ok := failpointpb.Action_value[want]
			require.True(t, ok, "%s (%s) is not in the proto enum", action, want)
		}
		require.Len(t, failpointpb.Action_name, actions+1) // +1 for ACTION_UNSPECIFIED
	})

	t.Run("recurrence", func(t *testing.T) {
		t.Parallel()
		// The recurrences of the protocol stand for triggers, see TestServer_RecurrenceConversion: a
		// new one needs a mapping.
		require.Len(t, failpointpb.Recurrence_name, 3)
	})
}
