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
	s.Nil(registry.Get(errorKey))

	fp1 := failpoint.New[error](errorKey)
	fp2 := failpoint.New[error](errorKey)
	registry.Set(fp1)
	s.Same(fp1, registry.Get(errorKey))

	registry.Set(fp2)
	s.Same(fp2, registry.Get(errorKey))

	registry.Delete(errorKey)
	s.Nil(registry.Get(errorKey))
}

func (s *registrySuite) TestSetRejectsNilAndEmptyKey() {
	registry := failpoint.NewRegistry()
	fp := failpoint.New[int](intKey)
	fp.Return(42)
	registry.Set(fp)

	s.Panics(func() { registry.Set(nil) })
	s.Panics(func() { registry.Set((*failpoint.Failpoint[int])(nil)) })
	s.Panics(func() { registry.Set(new(failpoint.Failpoint[int])) })
	s.Panics(func() { registry.Set(failpoint.New(failpoint.Key[int]{})) })

	// Zero or nil keys cannot affect a valid registration.
	registry.Delete(failpoint.Key[string]{})
	registry.Delete(nil)
	s.Nil(registry.Get(failpoint.Key[int]{}))
	s.Nil(registry.Get(nil))
	s.Same(fp, registry.Get(intKey))
}

func (s *registrySuite) TestSwapReleasesDisplacedBlock() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.New[error](errorKey)
	fp1.Block()
	s.Nil(registry.Swap(fp1))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = failpoint.ExecuteError(registry, errorKey)
	}()
	s.NoError(fp1.Wait(s.Context()))

	// Replacing a registered block returns the displaced entry so the caller can release it.
	fp2 := failpoint.New[error](errorKey)
	displaced := registry.Swap(fp2)
	s.Same(fp1, displaced)
	s.Same(fp2, registry.Get(errorKey))

	displaced.Disable()
	s.Rcv(done)
}

func (s *registrySuite) TestGetOrSet() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.New[error](errorKey)
	fp2 := failpoint.New[error](errorKey)

	s.Same(fp1, registry.GetOrSet(fp1))
	s.Same(fp1, registry.GetOrSet(fp2))
	s.Same(fp1, registry.Get(errorKey))
}

func (s *registrySuite) TestCompareAndDelete() {
	registry := failpoint.NewRegistry()
	fp1 := failpoint.New[error](errorKey)
	fp2 := failpoint.New[error](errorKey)
	registry.Set(fp1)

	s.False(registry.CompareAndDelete(fp2))
	s.Same(fp1, registry.Get(errorKey))
	s.True(registry.CompareAndDelete(fp1))
	s.Nil(registry.Get(errorKey))
	s.False(registry.CompareAndDelete(fp1))
}
