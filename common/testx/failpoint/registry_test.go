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
