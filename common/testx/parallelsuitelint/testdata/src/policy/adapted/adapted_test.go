package adapted

import (
	"testing"
	"adapter"
	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }

func (s *testSuite) TestGood() { s.Equal(1, 1) }
func TestSuite(t *testing.T) { adapter.Run(t, &testSuite{}) }
func TestForward(t *testing.T) { adapter.Forward(t, &testSuite{}) }
func TestSequential(t *testing.T) { adapter.Sequential(t, &testSuite{}) }
func TestFake(t *testing.T) { adapter.Fake(t, &testSuite{}) } // want "use parallelsuite.Run for tests"
func TestConditional(t *testing.T) { adapter.Conditional(t, &testSuite{}, true) } // want "use parallelsuite.Run for tests"
func TestDifferentTest(t *testing.T) { adapter.DifferentTest(t, &testSuite{}) } // want "use parallelsuite.Run for tests"
func TestDifferentSuite(t *testing.T) { adapter.DifferentSuite(t, &testSuite{}) } // want "use parallelsuite.Run for tests"
