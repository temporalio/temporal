package parallelsuite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type FirstExtension struct {
	owner *extendedSuite
	value int
}

func (e *FirstExtension) Init(s *extendedSuite) {
	e.owner = s
}

func (e *FirstExtension) CheckFirstOwner() {
	e.owner.Same(e, &e.owner.FirstExtension)
}

type SecondExtension struct {
	owner *extendedSuite
}

func (e *SecondExtension) Init(s *extendedSuite) {
	e.owner = s
}

func (e *SecondExtension) CheckSecondOwner() {
	e.owner.Same(e, &e.owner.SecondExtension)
}

type extendedSuite struct {
	FirstExtension
	Suite[*extendedSuite]
	SecondExtension
}

func (s *extendedSuite) TestExtensions() {
	s.Same(s, s.FirstExtension.owner)
	s.Same(s, s.SecondExtension.owner)
	s.CheckFirstOwner()
	s.CheckSecondOwner()
	s.value = 42

	s.RunSequential("fresh extensions", func(child *extendedSuite) {
		child.Same(child, child.FirstExtension.owner)
		child.Same(child, child.SecondExtension.owner)
		child.Zero(child.value)
		child.NotSame(s, child.FirstExtension.owner)
		child.CheckFirstOwner()
		child.CheckSecondOwner()
	})
}

func (s *extendedSuite) TestWithAssertions() {
	s.value = 42
	cp := WithAssertions(s, s.T())
	s.NotSame(s, cp)
	s.Same(s.T(), cp.T())
	s.Same(s.T(), cp.AssertionT())
	s.Same(cp, cp.FirstExtension.owner)
	s.Same(cp, cp.SecondExtension.owner)
	s.Zero(cp.value)
	cp.CheckFirstOwner()
	cp.CheckSecondOwner()
}

type InitializedExtension[T TestingSuite] struct{}

func (e *InitializedExtension[T]) Init(T) {}

type namedExtensionSuite struct {
	Suite[*namedExtensionSuite]
	Extension InitializedExtension[*namedExtensionSuite]
}

func (s *namedExtensionSuite) TestA() {}

type pointerExtensionSuite struct {
	Suite[*pointerExtensionSuite]
	*InitializedExtension[*pointerExtensionSuite]
}

func (s *pointerExtensionSuite) TestA() {}

type UninitializedExtension struct{}

type uninitializedExtensionSuite struct {
	Suite[*uninitializedExtensionSuite]
	UninitializedExtension
}

func (s *uninitializedExtensionSuite) TestA() {}

func TestEmbeddedExtensions(t *testing.T) {
	require.NotPanics(t, func() { Run(t, &extendedSuite{}) })
}

func TestRejectInvalidExtensions(t *testing.T) {
	t.Parallel()
	t.Run("named field", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &namedExtensionSuite{}) })
	})
	t.Run("pointer field", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &pointerExtensionSuite{}) })
	})
	t.Run("missing initializer", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &uninitializedExtensionSuite{}) })
	})
}
