package required

import (
	"testing"
	"adapter"

	legacy "github.com/stretchr/testify/suite"
	ps "go.temporal.io/testx/parallelsuite"
)

func TestOrdinary(t *testing.T) { t.Log("ordinary") } // want "use parallelsuite.Run for tests"

type validSuite struct{ ps.Suite[*validSuite] }

func (s *validSuite) TestWorks() { s.Equal(1, 1) }

func TestSuite(t *testing.T) { ps.Run(t, &validSuite{}) }
func TestGeneric(t *testing.T) { ps.Run[*validSuite](t, &validSuite{}) }
func TestSequential(t *testing.T) { ps.RunLegacySequential(t, &validSuite{}) }
func TestAdapter(t *testing.T) { adapter.Run(t, &validSuite{}) } // want "use parallelsuite.Run for tests"

type legacySuite struct{ legacy.Suite } // want "use parallelsuite.Suite instead of testify/suite.Suite"

func TestLegacy(t *testing.T) { legacy.Run(t, &legacySuite{}) } // want "use parallelsuite.Run instead of testify/suite.Run"

func TestSuiteAndMore(t *testing.T) { // want "use parallelsuite.Run for tests"
	t.Log("setup")
	ps.Run(t, &validSuite{})
}

func TestMain(m *testing.M) {}
func BenchmarkOrdinary(b *testing.B) {}
func FuzzOrdinary(f *testing.F) {}
func Example() {}
