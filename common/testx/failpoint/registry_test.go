//go:build test_dep

package failpoint_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/await"
	"go.temporal.io/testx/failpoint"
)

// Low-level registry operations

func TestRegistry_SetAndGet(t *testing.T) {
	registry := failpoint.NewRegistry()

	fp := new(failpoint.Failpoint[error])
	registry.Set(key1, fp)

	retrieved := registry.Get(key1)
	require.NotNil(t, retrieved)
	require.Equal(t, fp, retrieved)
}

func TestRegistry_Delete(t *testing.T) {
	registry := failpoint.NewRegistry()

	fp := new(failpoint.Failpoint[error])
	registry.Set(key1, fp)
	require.NotNil(t, registry.Get(key1))

	registry.Delete(key1)
	require.Nil(t, registry.Get(key1))
}

// gRPC service methods

// Context injection functionality

func TestRegistry_Keys(t *testing.T) {
	registry := failpoint.NewRegistry()
	registry.Set(key1, failpoint.NewFailpoint[error](key1))
	registry.Set(key2.ForNamespace("ns"), failpoint.NewFailpoint[error](key2))

	require.ElementsMatch(t, []failpoint.Key{key1, key2.ForNamespace("ns")}, registry.Keys())
}

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
	fp := failpoint.NewFailpoint[error](key1)
	fp.Return(errors.New("test error"), failpoint.Once)
	registry.Set(key1, fp)

	ctx := failpoint.InjectRegistry(t.Context(), registry)

	result, executed := failpoint.ExecuteCtx[error](ctx, key1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "test error", result.Error())
}

func TestRegistry_ExecuteCtxFallbackToDirectInjection(t *testing.T) {
	// Create context with directly injected failpoint (no registry)
	ctx, fp := failpoint.InjectReturn(
		t.Context(),
		key1,
		errors.New("direct injection"),
		failpoint.Once,
	)

	result, executed := failpoint.ExecuteCtx[error](ctx, key1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "direct injection", result.Error())

	// Verify it was executed
	_, executed = fp.Execute()
	require.False(t, executed)
}

func TestRegistry_ExecuteCtxRegistryTakesPrecedence(t *testing.T) {
	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint[error](key1)
	fp.Return(errors.New("from registry"), failpoint.Once)
	registry.Set(key1, fp)

	// Create context with both registry and direct injection
	ctx := failpoint.InjectRegistry(t.Context(), registry)
	ctx, _ = failpoint.InjectReturn(
		ctx,
		key1,
		errors.New("direct injection"),
		failpoint.Once,
	)

	// Registry should take precedence
	result, executed := failpoint.ExecuteCtx[error](ctx, key1)
	require.True(t, executed)
	require.Error(t, result)
	require.Equal(t, "from registry", result.Error())
}

// TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic guards against a prior
// race where UnblockOne snapshotted blockCh outside the lock and then sent on
// it after a concurrent Disable closed the channel.
func TestFailpoint_UnblockOne_AfterDisable_DoesNotPanic(t *testing.T) {
	for range 100 {
		fp := failpoint.NewFailpoint[error](key1)
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
	fp := failpoint.NewFailpoint[error](key1)
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

func TestRegistry_GetOrSet(t *testing.T) {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)

	require.Same(t, fp1, registry.GetOrSet(key1, fp1))
	require.Same(t, fp1, registry.GetOrSet(key1, fp2))
	require.Same(t, fp1, registry.Get(key1))
}

func TestRegistry_CompareAndDelete(t *testing.T) {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)
	registry.Set(key1, fp1)

	require.False(t, registry.CompareAndDelete(key1, fp2))
	require.Same(t, fp1, registry.Get(key1))
	require.True(t, registry.CompareAndDelete(key1, fp1))
	require.Nil(t, registry.Get(key1))
	require.False(t, registry.CompareAndDelete(key1, fp1))
}

func TestRegistry_CompareAndDelete_NotComparable(t *testing.T) {
	registry := failpoint.NewRegistry()
	entry := []string{"not", "comparable"}
	registry.Set(key1, entry)

	require.False(t, registry.CompareAndDelete(key1, entry))
	require.False(t, registry.CompareAndDelete(key1, struct{ s []string }{}))
	require.NotNil(t, registry.Get(key1))
}

func TestRegistry_DeleteAll(t *testing.T) {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[int](key2)
	registry.Set(key1, fp1)
	registry.Set(key2, fp2)

	require.ElementsMatch(t, []any{fp1, fp2}, registry.DeleteAll())
	require.Empty(t, registry.Keys())
	require.Empty(t, registry.DeleteAll())
}

func TestRegistry_ID(t *testing.T) {
	registry := failpoint.NewRegistry()
	id := registry.ID()
	require.NotEmpty(t, id)
	registry.Set(key1, failpoint.NewFailpoint[error](key1))
	require.Equal(t, id, registry.ID())
	require.NotEqual(t, id, failpoint.NewRegistry().ID())
}
