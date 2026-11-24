package failpoint_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/failpoint"
)

var errFallback = errors.New("from fallback")

// fakeFallback injects err into every execution it is asked about, and records the keys.
type fakeFallback struct {
	err  error
	mu   sync.Mutex
	keys []failpoint.Key
}

func (f *fakeFallback) Error(key failpoint.Key) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
	return f.err
}

func (f *fakeFallback) asked() []failpoint.Key {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]failpoint.Key(nil), f.keys...)
}

func newFallbackRegistry(t *testing.T) (failpoint.Registry, *fakeFallback) {
	t.Helper()
	registry := failpoint.NewRegistry()
	fallback := &fakeFallback{err: errFallback}
	registry.SetFallback(fallback)
	require.True(t, registry.SetFallbackActive(fallback, true))
	return registry, fallback
}

func TestRegistry_Fallback(t *testing.T) {
	t.Parallel()

	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	fallback := &fakeFallback{err: errFallback}
	require.Nil(t, registry.Fallback())

	// An installed fallback starts inactive.
	registry.SetFallback(fallback)
	require.Same(t, fallback, registry.Fallback())
	require.False(t, registry.Armed())
	require.NoError(t, failpoint.Checkpoint(src, key1))

	require.True(t, registry.SetFallbackActive(fallback, true))
	require.Same(t, fallback, registry.ActiveFallback())
	require.True(t, registry.Armed())
	require.Zero(t, registry.Len())
	require.ErrorIs(t, failpoint.Checkpoint(src, key1), errFallback)

	require.True(t, registry.SetFallbackActive(fallback, false))
	require.Nil(t, registry.ActiveFallback())
	require.False(t, registry.Armed())
	require.NoError(t, failpoint.Checkpoint(src, key1))

	// Replacing the fallback deactivates it, and the replaced one can't turn it on.
	require.True(t, registry.SetFallbackActive(fallback, true))
	other := &fakeFallback{}
	registry.SetFallback(other)
	require.Same(t, other, registry.Fallback())
	require.False(t, registry.Armed())
	require.False(t, registry.SetFallbackActive(fallback, true))
	require.False(t, registry.Armed())

	// InstallFallback keeps an installed fallback, also inside Configure.
	registry.Configure(func() {
		require.Same(t, other, registry.InstallFallback(fallback))
	})
	registry.SetFallback(nil)
	require.Nil(t, registry.Fallback())
	require.False(t, registry.SetFallbackActive(nil, true))
	require.Same(t, fallback, registry.InstallFallback(fallback))
	require.Equal(t, []failpoint.Key{key1}, fallback.asked())
}

func TestFallback_Execute(t *testing.T) {
	t.Parallel()

	t.Run("injects into error call sites with nothing registered", func(t *testing.T) {
		t.Parallel()
		registry, fallback := newFallbackRegistry(t)
		src := registrySource{registry: registry}
		key := key1.ForScope("ns")

		require.ErrorIs(t, failpoint.Checkpoint(src, key), errFallback)
		value, set, fired := failpoint.ExecuteWithFired[error](src, key, nil)
		require.ErrorIs(t, value, errFallback)
		require.True(t, set)
		require.True(t, fired)
		ctxErr, set := failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key)
		require.ErrorIs(t, ctxErr, errFallback)
		require.True(t, set)
		require.ErrorIs(t, failpoint.FallbackError(src, key), errFallback)

		_, set, fired = failpoint.ExecuteRegistered[error](src, key, nil)
		require.False(t, set)
		require.False(t, fired)
		_, set = failpoint.Execute[any](src, key)
		require.False(t, set)
		require.False(t, failpoint.Active(src, key))
		require.Len(t, fallback.asked(), 4)
	})

	t.Run("a registered failpoint takes precedence even if disabled", func(t *testing.T) {
		t.Parallel()
		registry, fallback := newFallbackRegistry(t)
		src := registrySource{registry: registry}
		key := key1.ForScope("ns")
		// The unscoped key covers the scoped one.
		registry.Set(key1, failpoint.NewFailpoint[error](key1))

		require.True(t, failpoint.Registered(src, key))
		require.NoError(t, failpoint.Checkpoint(src, key))
		require.NoError(t, failpoint.FallbackError(src, key))
		ctxErr, _ := failpoint.ExecuteCtx[error](failpoint.InjectRegistry(t.Context(), registry), key)
		require.NoError(t, ctxErr)
		require.Empty(t, fallback.asked())
	})

	t.Run("a registered shadow key takes precedence", func(t *testing.T) {
		t.Parallel()
		registry, _ := newFallbackRegistry(t)
		src := registrySource{registry: registry}
		key := key1.ForScope("ns")
		require.ErrorIs(t, failpoint.FallbackError(src, key, key2.ForScope("ns")), errFallback)

		registry.Set(key2, failpoint.NewFailpoint[error](key2))
		require.True(t, failpoint.Registered(src, key, key2.ForScope("ns")))
		require.NoError(t, failpoint.FallbackError(src, key, key2.ForScope("ns")))
		require.ErrorIs(t, failpoint.FallbackError(src, key, key3), errFallback)
	})

	t.Run("does nothing without a registry", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, failpoint.FallbackError(registrySource{}, key1))
		require.False(t, failpoint.Registered(registrySource{}, key1))
		require.False(t, failpoint.Armed(registrySource{}))
	})
}

func TestRegistry_Emit(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	var events []failpoint.Event
	registry.Subscribe(func(e failpoint.Event) { events = append(events, e) })
	event := failpoint.Event{Kind: failpoint.FallbackActivationEvent, Key: key1, Activated: true}
	registry.Emit(event)
	require.Equal(t, []failpoint.Event{event}, events)
}

func TestFallback_OffPathDoesNotAllocate(t *testing.T) {
	inactive := failpoint.NewRegistry()
	inactive.SetFallback(&fakeFallback{err: errFallback})
	for name, registry := range map[string]failpoint.Registry{
		"no registry":       nil,
		"empty registry":    failpoint.NewRegistry(),
		"inactive fallback": inactive,
	} {
		var src failpoint.Source = registrySource{registry: registry}
		key := key1.ForScope("ns")
		allocs := testing.AllocsPerRun(100, func() {
			_ = failpoint.FallbackError(src, key)
			_ = failpoint.Registered(src, key)
			_ = failpoint.Checkpoint(src, key)
		})
		require.Zero(t, allocs, name)
	}
}

func TestActiveRegistered(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()

	active, registered := failpoint.ActiveRegistered(registry, key1.ForScope("ns"))
	require.False(t, active)
	require.False(t, registered)

	fp := failpoint.NewFailpoint[error](key1)
	registry.Set(key1, fp)
	active, registered = failpoint.ActiveRegistered(registry, key1.ForScope("ns"))
	require.False(t, active)
	require.True(t, registered)

	fp.On(failpoint.Always)
	active, registered = failpoint.ActiveRegistered(registry, key1.ForScope("ns"))
	require.True(t, active)
	require.True(t, registered)
}
