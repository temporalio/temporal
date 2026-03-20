package parallelsuite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestGuardSeal(t *testing.T) {
	t.Run("assertion after Run", func(t *testing.T) {
		Run(t, &sealAfterRunSuite{})
	})
	t.Run("Run after assertion", func(t *testing.T) {
		Run(t, &sealRunAfterAssertSuite{})
	})
}
