package failpoint_test

import (
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type registrySuite struct {
	parallelsuite.Suite[*registrySuite]
}

func TestRegistrySuite(t *testing.T) {
	parallelsuite.Run(t, &registrySuite{})
}

func (s *registrySuite) TestSetGetDelete() {
	registry := failpoint.NewRegistry()
	s.Zero(registry.Len())
	s.Nil(registry.Get(key1))

	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)
	registry.Set(fp1)
	s.Equal(1, registry.Len())
	s.Same(fp1, registry.Get(key1))

	registry.Set(fp2)
	s.Equal(1, registry.Len())
	s.Same(fp2, registry.Get(key1))

	registry.Delete(key1)
	s.Zero(registry.Len())
	s.Nil(registry.Get(key1))
}

func (s *registrySuite) TestSetUnwrapsEmbeddedFailpoint() {
	type wrapped struct {
		*failpoint.Failpoint[int]
	}

	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(7)
	registry.Set(wrapped{Failpoint: fp})

	s.Same(fp, registry.Get(intKey))
	got, fired := failpoint.Execute(registry, intKey)
	s.True(fired)
	s.Equal(7, got)
}

func (s *registrySuite) TestSetRejectsNilAndEmptyKey() {
	type wrapped struct {
		*failpoint.Failpoint[int]
	}

	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(42)
	registry.Set(fp)

	s.Panics(func() { registry.Set(nil) })
	s.Panics(func() { registry.Set((*failpoint.Failpoint[int])(nil)) })
	s.Panics(func() { registry.Set(wrapped{}) })
	s.Panics(func() { registry.Set(new(failpoint.Failpoint[int])) })
	s.Panics(func() { registry.Set(failpoint.NewFailpoint(failpoint.Key[int]{})) })

	// Zero or nil keys cannot affect a valid registration.
	registry.Delete(failpoint.Key[string]{})
	registry.Delete(nil)
	s.Nil(registry.Get(failpoint.Key[int]{}))
	s.Nil(registry.Get(nil))
	s.Equal(1, registry.Len())
	s.Same(fp, registry.Get(intKey))
}
