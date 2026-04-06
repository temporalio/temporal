package parallelsuite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type validSuite struct{ Suite[*validSuite] }

func (s *validSuite) TestA() {
	s.NotNil(s.T())
}

type validWithArgsSuite struct{ Suite[*validWithArgsSuite] }

func (s *validWithArgsSuite) TestA(name string, count int) {
	s.Equal("hello", name)
	s.Equal(42, count)
}

type noTestMethodsSuite struct{ Suite[*noTestMethodsSuite] }

type wrongSigSuite struct{ Suite[*wrongSigSuite] }

func (s *wrongSigSuite) TestBad(t *testing.T) {} //nolint:unused

type badNameTests struct{ Suite[*badNameTests] }

func (s *badNameTests) TestA() {} //nolint:unused

type exportedNonTestSuite struct{ Suite[*exportedNonTestSuite] }

func (s *exportedNonTestSuite) TestA()  {}
func (s *exportedNonTestSuite) Helper() {} //nolint:unused

type hasExtraFieldsSuite struct {
	Suite[*hasExtraFieldsSuite]
	x int //nolint:unused
}

func (s *hasExtraFieldsSuite) TestA() {} //nolint:unused

type setupTestSuite struct{ Suite[*setupTestSuite] }

func (s *setupTestSuite) TestA()     {}
func (s *setupTestSuite) SetupTest() {} //nolint:unused

type sealAfterRunSuite struct{ Suite[*sealAfterRunSuite] }

func (s *sealAfterRunSuite) TestAssertionAfterRun() {
	// Calling Run seals the parent's assertions and T().
	s.Run("subtest", func(s *sealAfterRunSuite) {
		s.NotNil(s.T()) // subtest assertions work fine
	})

	t := s.guardT.T

	// After Run: even passing assertions panic.
	require.Panics(t, func() { s.NotNil(t) })

	// T() also panics after Run.
	require.Panics(t, func() { s.T() })
}

type sealRunAfterAssertSuite struct {
	Suite[*sealRunAfterAssertSuite]
}

func (s *sealRunAfterAssertSuite) TestRunAfterAssertion() {
	// Use an assertion first.
	s.NotNil(s.T())

	t := s.guardT.T

	// Calling Run after assertions panics.
	require.Panics(t, func() {
		s.Run("should-not-run", func(*sealRunAfterAssertSuite) {})
	})
}

func TestRun_AcceptsSuite(t *testing.T) {
	t.Run("no args", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &validSuite{}) })
	})
	t.Run("with args", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &validWithArgsSuite{}, "hello", 42) })
	})
}

func TestRun_RejectsSuite(t *testing.T) {
	t.Run("no Test methods", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &noTestMethodsSuite{}) })
	})
	t.Run("wrong method signature", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &wrongSigSuite{}) })
	})
	t.Run("extra fields", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &hasExtraFieldsSuite{}) })
	})
	t.Run("name not ending in Suite", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &badNameTests{}) })
	})
	t.Run("non-Test exported method", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &exportedNonTestSuite{}) })
	})
	t.Run("SetupTest forbidden", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &setupTestSuite{}) })
	})
}

func TestGuardSeal(t *testing.T) {
	t.Run("assertion after Run", func(t *testing.T) {
		Run(t, &sealAfterRunSuite{})
	})
	t.Run("Run after assertion", func(t *testing.T) {
		Run(t, &sealRunAfterAssertSuite{})
	})
}
