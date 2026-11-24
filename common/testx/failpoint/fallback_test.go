package failpoint_test

import (
	"context"
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
		arg := any(&struct{}{})
		ctx := context.Background()
		if registry != nil {
			ctx = failpoint.InjectRegistry(ctx, registry)
		}
		var cache failpoint.FallbackCache
		allocs := testing.AllocsPerRun(100, func() {
			_ = failpoint.FallbackError(src, key)
			_ = failpoint.FallbackErrorCached(src, &cache, key, key2)
			_ = failpoint.Registered(src, key)
			_ = failpoint.Checkpoint(src, key)
			_, _ = failpoint.Execute[error](src, key)
			_, _ = failpoint.ExecuteWith[error](src, key, arg)
			_, _, _ = failpoint.ExecuteRegistered[error](src, key, arg)
			_, _ = failpoint.ExecuteCtx[error](ctx, key)
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

// cachingFallback is a fakeFallback that also implements failpoint.CachingFallback. It records the
// keys that ErrorCached was asked about, and stores each key in the cache.
type cachingFallback struct {
	fakeFallback
	cachedKeys []failpoint.Key
}

func (f *cachingFallback) ErrorCached(key failpoint.Key, cache failpoint.FallbackCacheSlot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cachedKeys = append(f.cachedKeys, key)
	cache.Store(key)
	return f.err
}

func TestFallbackErrorCached(t *testing.T) {
	t.Parallel()

	t.Run("passes the cache to a caching fallback", func(t *testing.T) {
		t.Parallel()
		registry := failpoint.NewRegistry()
		fallback := &cachingFallback{err: errFallback}
		registry.SetFallback(fallback)
		require.True(t, registry.SetFallbackActive(fallback, true))
		src := registrySource{registry: registry}
		var cache failpoint.FallbackCache

		key := key1.ForScope("ns")
		require.Equal(t, errFallback, failpoint.FallbackErrorCached(src, &cache, key, key2))
		// Without a cache, the fallback is asked with Error.
		require.Equal(t, errFallback, failpoint.FallbackErrorCached(src, nil, key, key2))
		require.Equal(t, errFallback, failpoint.FallbackError(src, key))
		require.Equal(t, []failpoint.Key{key}, fallback.cachedKeys)
		require.Equal(t, []failpoint.Key{key, key}, fallback.asked())

		// A failpoint registered for the unscoped shadow key takes precedence.
		registry.Set(key2, failpoint.NewFailpoint[error](key2))
		require.NoError(t, failpoint.FallbackErrorCached(src, &cache, key, key2))
		require.Len(t, fallback.cachedKeys, 1)
	})

	t.Run("asks a fallback that doesn't cache with Error", func(t *testing.T) {
		t.Parallel()
		registry, fallback := newFallbackRegistry(t)
		var cache failpoint.FallbackCache
		require.Equal(t, errFallback, failpoint.FallbackErrorCached(registrySource{registry: registry}, &cache, key1, key2))
		require.Equal(t, []failpoint.Key{key1}, fallback.asked())
	})
}

func TestFallbackError_ManyShadows(t *testing.T) {
	t.Parallel()
	registry, _ := newFallbackRegistry(t)
	src := registrySource{registry: registry}
	shadows := []failpoint.Key{key2, key3, key4, key5}
	require.Equal(t, errFallback, failpoint.FallbackError(src, key1, shadows...))

	// A registration of any of the shadows takes precedence, also of those beyond the keys that
	// a lookup holds without allocating.
	for _, shadow := range shadows {
		registry.Set(shadow, failpoint.NewFailpoint[error](shadow))
		require.NoError(t, failpoint.FallbackError(src, key1, shadows...), shadow)
		require.True(t, failpoint.Registered(src, key1, shadows...), shadow)
		registry.Delete(shadow)
	}
}

// loadingFallback is a cachingFallback that records the value it loads from the cache before it
// stores its own, which is its name.
type loadingFallback struct {
	cachingFallback
	name   string
	loaded []any
}

func (f *loadingFallback) ErrorCached(key failpoint.Key, cache failpoint.FallbackCacheSlot) error {
	f.mu.Lock()
	f.loaded = append(f.loaded, cache.Load())
	f.mu.Unlock()
	cache.Store(f.name)
	return f.err
}

func TestFallbackCache_IsBoundToTheInstallation(t *testing.T) {
	t.Parallel()
	registry := failpoint.NewRegistry()
	src := registrySource{registry: registry}
	install := func(f failpoint.Fallback) {
		registry.SetFallback(f)
		require.True(t, registry.SetFallbackActive(f, true))
	}
	var cache failpoint.FallbackCache

	first := &loadingFallback{err: errFallback, name: "first"}
	second := &loadingFallback{err: errFallback, name: "second"}
	install(first)
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	// A failpoint that is registered and removed keeps the installation.
	registry.Set(key5, failpoint.NewFailpoint[error](key5))
	registry.Delete(key5)
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	require.Equal(t, []any{nil, "first", "first"}, first.loaded)

	// A replacement never sees what the previous fallback stored.
	install(second)
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	require.Equal(t, []any{nil, "second"}, second.loaded)

	// Nor does the same fallback after it was turned off and on again, or the previous one that is
	// installed again.
	require.True(t, registry.SetFallbackActive(second, false))
	require.True(t, registry.SetFallbackActive(second, true))
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	install(first)
	require.Error(t, failpoint.FallbackErrorCached(src, &cache, key1))
	require.Equal(t, []any{nil, "second", nil}, second.loaded)
	require.Equal(t, []any{nil, "first", "first", nil}, first.loaded)
}

func TestFallbackErrorIfEmpty(t *testing.T) {
	t.Parallel()
	registry, fallback := newFallbackRegistry(t)
	src := registrySource{registry: registry}

	empty, err := failpoint.FallbackErrorIfEmpty(src, nil, key1)
	require.True(t, empty)
	require.Equal(t, errFallback, err)

	// Without a registry, or without an active fallback, there is nothing to inject.
	empty, err = failpoint.FallbackErrorIfEmpty(registrySource{}, nil, key1)
	require.True(t, empty)
	require.NoError(t, err)
	empty, err = failpoint.FallbackErrorIfEmpty(registrySource{registry: failpoint.NewRegistry()}, nil, key1)
	require.True(t, empty)
	require.NoError(t, err)

	// With any failpoint registered, even for another key, the caller executes its failpoints.
	registry.Set(key5, failpoint.NewFailpoint[error](key5))
	empty, err = failpoint.FallbackErrorIfEmpty(src, nil, key1)
	require.False(t, empty)
	require.NoError(t, err)
	require.Equal(t, []failpoint.Key{key1}, fallback.asked())
}

func TestFallback_OnPathDoesNotAllocate(t *testing.T) {
	registry := failpoint.NewRegistry()
	// nopFallback records nothing, so the measurement sees only the lookup's allocations.
	fallback := nopFallback{}
	registry.SetFallback(fallback)
	require.True(t, registry.SetFallbackActive(fallback, true))
	var src failpoint.Source = registrySource{registry: registry}
	key := key1.ForScope("ns")
	// The cases run in order: the second registers a failpoint, which the first must not see.
	for _, tc := range []struct {
		name      string
		configure func()
	}{
		{name: "nothing registered", configure: func() {}},
		// The lookup of the keys takes the lock.
		{name: "another key registered", configure: func() { registry.Set(key5, failpoint.NewFailpoint[error](key5)) }},
	} {
		tc.configure()
		allocs := testing.AllocsPerRun(1000, func() {
			_ = failpoint.FallbackError(src, key, key2)
			_ = failpoint.Registered(src, key, key2, key3)
			_ = failpoint.Checkpoint(src, key)
		})
		require.Zero(t, allocs, tc.name)
	}
}
