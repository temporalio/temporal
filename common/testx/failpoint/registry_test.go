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

func TestRegistry_Len(t *testing.T) {
	registry := failpoint.NewRegistry()
	require.Zero(t, registry.Len())

	registry.Set(key1, failpoint.NewFailpoint[error](key1))
	registry.Set(key1.ForScope("ns"), failpoint.NewFailpoint[error](key1))
	require.Equal(t, 2, registry.Len())

	registry.Delete(key1)
	require.Equal(t, 1, registry.Len())
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
	registry.Set(key2.ForScope("ns"), failpoint.NewFailpoint[error](key2))

	require.ElementsMatch(t, []failpoint.Key{key1, key2.ForScope("ns")}, registry.Keys())
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

func TestRegistry_ExecuteCtxKeepsDirectInjectionOfParent(t *testing.T) {
	ctx, _ := failpoint.InjectReturn(t.Context(), key1, errors.New("outer"), failpoint.Always)
	ctx = failpoint.InjectRegistry(ctx, failpoint.NewRegistry())
	ctx, _ = failpoint.InjectReturn(ctx, key2, errors.New("inner"), failpoint.Always)

	result, executed := failpoint.ExecuteCtx[error](ctx, key1)
	require.True(t, executed)
	require.EqualError(t, result, "outer")
	result, executed = failpoint.ExecuteCtx[error](ctx, key2)
	require.True(t, executed)
	require.EqualError(t, result, "inner")

	// A later injection for the same key replaces the earlier one.
	ctx, _ = failpoint.InjectReturn(ctx, key1, errors.New("replaced"), failpoint.Always)
	result, executed = failpoint.ExecuteCtx[error](ctx, key1)
	require.True(t, executed)
	require.EqualError(t, result, "replaced")
}

func TestRegistry_ExecuteCtxWithoutFailpoints(t *testing.T) {
	_, executed := failpoint.ExecuteCtx[error](t.Context(), key1)
	require.False(t, executed)

	_, executed = failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), failpoint.NewRegistry()), key1)
	require.False(t, executed)

	_, ok := failpoint.GetRegistry(failpoint.InjectRegistry(t.Context(), nil))
	require.False(t, ok)
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
	require.Nil(t, registry.Get(key1))
	require.Empty(t, registry.DeleteAll())

	registry.Set(key1, fp1)
	require.Same(t, fp1, registry.Get(key1))
}

func TestRegistry_DeleteFunc(t *testing.T) {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[int](key2)
	registry.Set(key1, fp1)
	registry.Set(key2, fp2)
	registry.Set(key3, []string{"not comparable"})

	removed := registry.DeleteFunc(func(key failpoint.Key, _ any) bool { return key == key1 })
	require.ElementsMatch(t, []any{fp2, []string{"not comparable"}}, removed)
	require.Equal(t, []failpoint.Key{key1}, registry.Keys())
	require.Same(t, fp1, registry.Get(key1))
}

func TestRegistry_DeleteFunc_PredicateCallsRegistry(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key2)
	replacement := failpoint.NewFailpoint[error](key2)
	registry.Set(key1, fp1)
	registry.Set(key2, fp2)

	removed := registry.DeleteFunc(func(key failpoint.Key, _ any) bool {
		require.Len(t, registry.Keys(), 2)
		if key == key2 {
			// The entry changes after the predicate saw it, so it is kept.
			registry.Set(key2, replacement)
		}
		return false
	})
	require.Equal(t, []any{fp1}, removed)
	require.Equal(t, []failpoint.Key{key2}, registry.Keys())
	require.Same(t, replacement, registry.Get(key2))
}

func TestRegistry_ID(t *testing.T) {
	registry := failpoint.NewRegistry()
	id := registry.ID()
	require.NotEmpty(t, id)
	registry.Set(key1, failpoint.NewFailpoint[error](key1))
	require.Equal(t, id, registry.ID())
	require.NotEqual(t, id, failpoint.NewRegistry().ID())
}

func TestRegistry_Subscribe(t *testing.T) {
	t.Parallel()

	t.Run("reports fired and skipped executions", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Return(errors.New("injected"), failpoint.Always)
		fp.SetTrigger(failpoint.Trigger{After: 1})
		registry.Set(key1, fp)
		var events []failpoint.Event
		unsubscribe := registry.Subscribe(func(e failpoint.Event) { events = append(events, e) })

		src := registrySource{registry: registry}
		_, _ = failpoint.Execute[error](src, key1)
		_, _ = failpoint.Execute[error](src, key1)
		unsubscribe()
		_, _ = failpoint.Execute[error](src, key1)

		require.Equal(t, []failpoint.Event{
			{Key: key1, Action: failpoint.Return, Fired: false, Execution: 1},
			{Key: key1, Action: failpoint.Return, Fired: true, Execution: 2},
		}, events)
	})

	t.Run("reports a block before it waits", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fp := failpoint.NewFailpoint[error](key1)
		fp.Block(failpoint.Always)
		registry.Set(key1, fp)
		var mu sync.Mutex
		var events []failpoint.Event
		registry.Subscribe(func(e failpoint.Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, e)
		})

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = failpoint.Execute[error](registrySource{registry: registry}, key1)
		}()
		await.RequireTrue(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(events) == 1
		}, 5*time.Second, 10*time.Millisecond)
		fp.Disable()
		<-done
	})
}
