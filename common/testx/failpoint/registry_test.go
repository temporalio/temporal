package failpoint_test

import (
	"errors"
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
	s.Nil(registry.Get(key1))

	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)
	registry.Set(fp1)
	s.Same(fp1, registry.Get(key1))

	registry.Set(fp2)
	s.Same(fp2, registry.Get(key1))

	registry.Delete(key1)
	s.Nil(registry.Get(key1))
}

func (s *registrySuite) TestSetRejectsNilAndEmptyKey() {
	registry := failpoint.NewRegistry()
	fp := failpoint.NewFailpoint[int](intKey)
	fp.Return(42)
	registry.Set(fp)

	s.Panics(func() { registry.Set(nil) })
	s.Panics(func() { registry.Set((*failpoint.Failpoint[int])(nil)) })
	s.Panics(func() { registry.Set(new(failpoint.Failpoint[int])) })
	s.Panics(func() { registry.Set(failpoint.NewFailpoint(failpoint.Key[int]{})) })

	// Zero or nil keys cannot affect a valid registration.
	registry.Delete(failpoint.Key[string]{})
	registry.Delete(nil)
	s.Nil(registry.Get(failpoint.Key[int]{}))
	s.Nil(registry.Get(nil))
	s.Same(fp, registry.Get(intKey))
}

func (s *registrySuite) TestSwapReleasesDisplacedBlock() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp1.Block()
	s.Nil(registry.Swap(fp1))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = failpoint.CheckError(registry, key1)
	}()
	s.NoError(fp1.Wait(s.Context()))

	// Replacing a registered block returns the displaced entry so the caller can release it.
	fp2 := failpoint.NewFailpoint[error](key1)
	displaced := registry.Swap(fp2)
	s.Same(fp1, displaced)
	s.Same(fp2, registry.Get(key1))

	displaced.Disable()
	s.Rcv(done)
}

func (s *registrySuite) TestGetOrSet() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)

	s.Same(fp1, registry.GetOrSet(fp1))
	s.Same(fp1, registry.GetOrSet(fp2))
	s.Same(fp1, registry.Get(key1))
}

func (s *registrySuite) TestCompareAndDelete() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp2 := failpoint.NewFailpoint[error](key1)
	registry.Set(fp1)

	s.False(registry.CompareAndDelete(fp2))
	s.Same(fp1, registry.Get(key1))
	s.True(registry.CompareAndDelete(fp1))
	s.Nil(registry.Get(key1))
	s.False(registry.CompareAndDelete(fp1))
}

func (s *registrySuite) TestID() {
	a, b := failpoint.NewRegistry(), failpoint.NewRegistry()
	id := a.ID()
	s.NotEmpty(id)
	s.Equal(id, a.ID())
	s.NotEqual(id, b.ID())
}

func (s *registrySuite) TestInfos() {
	registry := failpoint.NewRegistry()
	s.Empty(registry.Infos())
	fp1 := failpoint.NewFailpoint[error](key1.ForScope("ns"))
	fp1.Block(failpoint.Trigger{MaxFires: 1})
	fp2 := failpoint.NewFailpoint[error](key2)
	fp2.Return(errors.New("injected"))
	registry.Set(fp2)
	registry.Set(fp1)

	s.Equal([]failpoint.Info{
		{Key: key1.ForScope("ns"), Action: failpoint.ActionBlock, Trigger: failpoint.Trigger{MaxFires: 1}},
		{Key: key2, Action: failpoint.ActionReturn},
	}, registry.Infos())
	s.Equal(fp2.Info(), registry.Get(key2).Info())
}

func (s *registrySuite) TestDeleteFunc() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.NewFailpoint[error](key1)
	fp1.Block()
	fp2 := failpoint.NewFailpoint[error](key2)
	fp2.Block()
	registry.Set(fp1)
	registry.Set(fp2)
	released := make(chan struct{})
	go func() {
		defer close(released)
		_, _ = fp1.Execute()
	}()
	s.NoError(fp1.Wait(s.Context()))

	registry.DeleteFunc(func(info failpoint.Info) bool { return info.Key.Name() == key1.Name() })

	s.Rcv(released)
	s.Nil(registry.Get(key1))
	s.Same(fp2, registry.Get(key2))
	s.Len(registry.Infos(), 1)
	s.True(registry.MayFire())
	registry.DeleteFunc(func(failpoint.Info) bool { return true })
	s.Empty(registry.Infos())
	s.False(registry.MayFire())
}
