package failpoint_test

import (
	"errors"
	"sync"
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type fallbackSuite struct {
	parallelsuite.Suite[*fallbackSuite]
}

func TestFallbackSuite(t *testing.T) {
	parallelsuite.Run(t, &fallbackSuite{})
}

var errFallback = errors.New("from fallback")

// fakeFallback injects err into every execution it is asked about, and records the keys.
type fakeFallback struct {
	err  error
	mu   sync.Mutex
	keys []failpoint.Key[error]
}

func (f *fakeFallback) Error(key failpoint.Key[error]) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
	return f.err
}

func (f *fakeFallback) asked() []failpoint.Key[error] {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]failpoint.Key[error](nil), f.keys...)
}

func newFallbackRegistry(s *fallbackSuite) (*failpoint.Registry, *fakeFallback) {
	registry := failpoint.NewRegistry()
	fallback := &fakeFallback{err: errFallback}
	registry.SetFallbackIfAbsent(fallback)
	s.True(registry.SetFallbackActive(fallback, true))
	return registry, fallback
}

func (s *fallbackSuite) TestRegistryFallback() {
	registry := failpoint.NewRegistry()
	src := registry
	fallback := &fakeFallback{err: errFallback}
	s.Nil(registry.Fallback())

	// An installed fallback starts inactive.
	s.Same(fallback, registry.SetFallbackIfAbsent(fallback))
	s.Same(fallback, registry.Fallback())
	s.False(registry.MayFire())
	s.NoError(failpoint.CheckError(src, key1))

	s.True(registry.SetFallbackActive(fallback, true))
	s.Same(fallback, registry.ActiveFallback())
	s.True(registry.MayFire())
	s.Zero(registry.Len())
	s.ErrorIs(failpoint.CheckError(src, key1), errFallback)

	s.True(registry.SetFallbackActive(fallback, false))
	s.Nil(registry.ActiveFallback())
	s.False(registry.MayFire())
	s.NoError(failpoint.CheckError(src, key1))

	// SetFallbackIfAbsent keeps an installed fallback, and only the installed fallback can be
	// turned on.
	other := &fakeFallback{}
	s.Same(fallback, registry.SetFallbackIfAbsent(other))
	s.False(registry.SetFallbackActive(other, true))
	s.False(registry.SetFallbackActive(nil, true))
	s.False(registry.MayFire())
	s.Equal([]failpoint.Key[error]{key1}, fallback.asked())
}

func (s *fallbackSuite) TestFallbackExecute() {
	s.Run("injects into error call sites with nothing registered", func(s *fallbackSuite) {
		registry, fallback := newFallbackRegistry(s)
		key := key1.ForScope("ns")

		s.ErrorIs(failpoint.CheckError(registry, key), errFallback)
		got, set := failpoint.Execute[error](registry, key)
		s.ErrorIs(got, errFallback)
		s.True(set)

		_, set = failpoint.Execute(registry, anyKey.ForScope("ns"))
		s.False(set)
		s.False(failpoint.ExecuteSwitch(registry, anyKey.ForScope("ns")))
		s.Len(fallback.asked(), 2)
	})

	s.Run("a registered failpoint takes precedence even if disabled", func(s *fallbackSuite) {
		registry, fallback := newFallbackRegistry(s)
		key := key1.ForScope("ns")
		// The unscoped key covers the scoped one.
		registry.Set(failpoint.NewFailpoint[error](key1))

		s.NoError(failpoint.CheckError(registry, key))
		s.Empty(fallback.asked())
	})

	s.Run("does nothing without a registry", func(s *fallbackSuite) {
		s.False(failpoint.MayFire((*failpoint.Registry)(nil)))
		s.NoError(failpoint.CheckError((*failpoint.Registry)(nil), key1))
	})
}

func (s *fallbackSuite) TestExecuteSwitchRegistered() {
	registry := failpoint.NewRegistry()

	fired, registered := failpoint.ExecuteSwitchRegistered(registry, key1.ForScope("ns"))
	s.False(fired)
	s.False(registered)

	fp := failpoint.NewFailpoint[error](key1)
	registry.Set(fp)
	fired, registered = failpoint.ExecuteSwitchRegistered(registry, key1.ForScope("ns"))
	s.False(fired)
	s.True(registered)

	fp.On()
	fired, registered = failpoint.ExecuteSwitchRegistered(registry, key1.ForScope("ns"))
	s.True(fired)
	s.True(registered)
}
