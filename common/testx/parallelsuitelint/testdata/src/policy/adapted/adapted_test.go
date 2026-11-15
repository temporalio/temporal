package adapted

import (
	"testing"
	"adapter"
	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }

func (s *testSuite) TestGood() { s.Equal(1, 1) }
func TestSuite(t *testing.T) { adapter.Run(t, &testSuite{}) }
