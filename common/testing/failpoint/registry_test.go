//go:build test_dep

package failpoint_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/failpoint"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Low-level registry operations

func TestRegistry_SetAndGet(t *testing.T) {
	registry := failpoint.NewRegistry()

	fp := new(failpoint.Failpoint[error])
	registry.Set(1, fp)

	retrieved := registry.Get(1)
	require.NotNil(t, retrieved)
	require.Equal(t, fp, retrieved)
}

func TestRegistry_Delete(t *testing.T) {
	registry := failpoint.NewRegistry()

	fp := new(failpoint.Failpoint[error])
	registry.Set(1, fp)
	require.NotNil(t, registry.Get(1))

	registry.Delete(1)
	require.Nil(t, registry.Get(1))
}

func TestRegistry_List(t *testing.T) {
	registry := failpoint.NewRegistry()

	fp1 := failpoint.NewFailpoint[error](1)
	fp2 := failpoint.NewFailpoint[error](2)

	registry.Set(1, fp1)
	registry.Set(2, fp2)

	resp, err := registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 2)

	keys := map[enumsspb.FailpointKey]bool{}
	for _, info := range resp.Failpoints {
		keys[info.Key] = true
	}
	require.True(t, keys[1] && keys[2])
}

// gRPC service methods

func TestRegistry_InjectPanic(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "test panic",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	require.Panics(t, func() { _, _ = fp.Execute() })
}

func TestRegistry_InjectBlock(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        1,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
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

func TestRegistry_InjectReturn(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "test error",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	result, executed := fp.Execute()
	require.True(t, executed)
	require.EqualError(t, result, "test error")
}

func TestRegistry_Disable(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "test error",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok)
	_, executed := fp.Execute()
	require.True(t, executed)

	_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)

	// After disable, the failpoint should be removed from registry
	require.Nil(t, registry.Get(1))
}

func TestRegistry_DisableAll(t *testing.T) {
	registry := failpoint.NewRegistry()

	// Inject multiple failpoints
	_, err := registry.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "panic1",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          2,
		ErrorMessage: "error1",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	resp, err := registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 2)

	// Disable all
	_, err = registry.DisableAllFailpoints(t.Context(), &failpointservice.DisableAllFailpointsRequest{})
	require.NoError(t, err)

	// Verify all are gone
	resp, err = registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Failpoints)
}

func TestRegistry_WaitForBlock(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        1,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	fp, ok := registry.Get(1).(*failpoint.Failpoint[error])
	require.True(t, ok)

	waitDone := make(chan error, 1)
	go func() {
		_, err := registry.WaitForBlock(t.Context(), &failpointservice.WaitForBlockRequest{Key: 1})
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

func TestRegistry_ListFailpoints(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "panic1",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = registry.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        2,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	_, err = registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          3,
		ErrorMessage: "error1",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 3)

	keys := map[enumsspb.FailpointKey]bool{}
	for _, fp := range resp.Failpoints {
		keys[fp.Key] = true
		require.True(t, fp.Enabled)
	}
	require.True(t, keys[1] && keys[2] && keys[3])
}

func TestRegistry_RecurrenceConversion(t *testing.T) {
	registry := failpoint.NewRegistry()

	tests := []struct {
		proto    enumsspb.FailpointRecurrence
		expected enumsspb.FailpointRecurrence
	}{
		{enumsspb.FAILPOINT_RECURRENCE_ONCE, enumsspb.FAILPOINT_RECURRENCE_ONCE},
		{enumsspb.FAILPOINT_RECURRENCE_ALWAYS, enumsspb.FAILPOINT_RECURRENCE_ALWAYS},
		{enumsspb.FAILPOINT_RECURRENCE_UNSPECIFIED, enumsspb.FAILPOINT_RECURRENCE_ALWAYS},
	}

	for _, tt := range tests {
		_, err := registry.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
			Key:        100,
			PanicValue: "test",
			Recurrence: tt.proto,
		})
		require.NoError(t, err)

		resp, err := registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Failpoints, 1)
		require.Equal(t, tt.expected, resp.Failpoints[0].Recurrence)

		_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 100})
		require.NoError(t, err)
	}
}

func TestRegistry_GetInfo(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectPanic(t.Context(), &failpointservice.InjectPanicRequest{
		Key:        1,
		PanicValue: "test",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	resp, err := registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Failpoints, 1)
	info := resp.Failpoints[0]
	require.Equal(t, enumsspb.FailpointKey(1), info.Key)
	require.True(t, info.Enabled)
	require.Equal(t, enumsspb.FAILPOINT_ACTION_PANIC, info.Action)
	require.Equal(t, enumsspb.FAILPOINT_RECURRENCE_ONCE, info.Recurrence)

	// Disable and check again - it should be removed from the registry
	_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)

	resp, err = registry.ListFailpoints(t.Context(), &failpointservice.ListFailpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Failpoints)
}

// Context injection functionality

func TestRegistry_ContextInjection(t *testing.T) {
	registry := failpoint.NewRegistry()
	ctx := t.Context()

	// Inject registry into context
	ctx = failpoint.InjectRegistry(ctx, registry)

	// Retrieve it
	retrieved, ok := failpoint.GetRegistry(ctx)
	require.True(t, ok)
	require.Equal(t, registry, retrieved)
}

func TestRegistry_ExecuteCtxWithRegistry(t *testing.T) {
	registry := failpoint.NewRegistry()
	_, err := registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "test error",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	ctx := failpoint.InjectRegistry(t.Context(), registry)

	result, executed := failpoint.ExecuteCtx[error](ctx, 1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "test error", result.Error())
}

func TestRegistry_ExecuteCtxFallbackToDirectInjection(t *testing.T) {
	// Create context with directly injected failpoint (no registry)
	ctx, fp := failpoint.InjectReturn(
		t.Context(),
		1,
		errors.New("direct injection"),
		failpoint.Once,
	)

	result, executed := failpoint.ExecuteCtx[error](ctx, 1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "direct injection", result.Error())

	// Verify it was executed
	_, executed = fp.Execute()
	require.False(t, executed)
}

func TestRegistry_ExecuteCtxRegistryTakesPrecedence(t *testing.T) {
	registry := failpoint.NewRegistry()
	_, err := registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "from registry",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	// Create context with both registry and direct injection
	ctx := failpoint.InjectRegistry(t.Context(), registry)
	ctx, _ = failpoint.InjectReturn(
		ctx,
		1,
		errors.New("direct injection"),
		failpoint.Once,
	)

	// Registry should take precedence
	result, executed := failpoint.ExecuteCtx[error](ctx, 1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "from registry", result.Error())
}

// TestRegistry_DisableFailpoint_Idempotent verifies that disabling a
// non-existent or already-disabled failpoint is a no-op rather than an error.
// Without this, cleanup hooks that disable defensively flap with NotFound.
func TestRegistry_DisableFailpoint_Idempotent(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)

	_, err = registry.InjectReturn(t.Context(), &failpointservice.InjectReturnRequest{
		Key:          1,
		ErrorMessage: "x",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.NoError(t, err)

	_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)

	// Second disable on the same key must also succeed.
	_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
	require.NoError(t, err)
}

// TestRegistry_Inject_RejectsUnsetKey verifies that requests omitting the key
// (proto zero value) are rejected with InvalidArgument. Otherwise the registry
// would happily store a failpoint at key 0 that no ExecuteCtx call could match.
func TestRegistry_Inject_RejectsUnsetKey(t *testing.T) {
	registry := failpoint.NewRegistry()
	ctx := t.Context()

	_, err := registry.InjectPanic(ctx, &failpointservice.InjectPanicRequest{
		PanicValue: "x",
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = registry.InjectBlock(ctx, &failpointservice.InjectBlockRequest{
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = registry.InjectReturn(ctx, &failpointservice.InjectReturnRequest{
		ErrorMessage: "x",
		Recurrence:   enumsspb.FAILPOINT_RECURRENCE_ONCE,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestRegistry_WaitForBlock_DisabledMidWait verifies that the gRPC WaitForBlock
// path tolerates a concurrent Disable: the wait completes (via the closed
// arrivedCh that disableLocked produces) instead of panicking inside WaitCh.
func TestRegistry_WaitForBlock_DisabledMidWait(t *testing.T) {
	registry := failpoint.NewRegistry()

	_, err := registry.InjectBlock(t.Context(), &failpointservice.InjectBlockRequest{
		Key:        1,
		Recurrence: enumsspb.FAILPOINT_RECURRENCE_ALWAYS,
	})
	require.NoError(t, err)

	waitStarted := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		close(waitStarted)
		_, err := registry.WaitForBlock(t.Context(), &failpointservice.WaitForBlockRequest{Key: 1})
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

	_, err = registry.DisableFailpoint(t.Context(), &failpointservice.DisableFailpointRequest{Key: 1})
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

// TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic guards against a prior
// race where UnblockOne snapshotted blockCh outside the lock and then sent on
// it after a concurrent Disable closed the channel.
func TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic(t *testing.T) {
	for range 100 {
		fp := failpoint.NewFailpoint[error](1)
		fp.Block(failpoint.Always)

		var wg sync.WaitGroup
		wg.Go(fp.Disable)
		wg.Go(fp.UnblockOne)
		wg.Wait()
	}
}

// TestFailpoint_OnceBlock_ConcurrentReinject guards against a prior race where
// the deferred disableLocked in Execute would clobber state belonging to a
// fresh Block() that landed during the wait.
func TestFailpoint_OnceBlock_ConcurrentReinject(t *testing.T) {
	fp := failpoint.NewFailpoint[error](1)
	fp.Block(failpoint.Once)

	executed := make(chan struct{})
	go func() {
		defer close(executed)
		_, _ = fp.Execute()
	}()

	// Wait for the goroutine to park inside Execute.
	await.RequireTrue(t, func() bool {
		select {
		case <-fp.WaitCh():
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)

	// Re-inject Block(Always) while the original goroutine is parked. The
	// original goroutine's Once cleanup must not close the new channels.
	fp.Block(failpoint.Always)

	// Release the original goroutine via Disable on the OLD blockCh. We do
	// this by issuing a UnblockOne on the new state... but the new state has
	// a fresh blockCh. The original is parked on the old (now-closed via
	// disableLocked invoked from the new Block()). So it should already have
	// returned.
	select {
	case <-executed:
	case <-time.After(time.Second):
		t.Fatal("original Once goroutine did not return after re-inject")
	}

	// The new Always failpoint must still be usable: a fresh goroutine should
	// be parkable on it, and UnblockOne should release it.
	parked := make(chan struct{})
	go func() {
		defer close(parked)
		_, _ = fp.Execute()
	}()
	await.RequireTrue(t, func() bool {
		select {
		case <-fp.WaitCh():
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
	fp.UnblockOne()
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("new Always failpoint did not unblock — state was clobbered")
	}
}
