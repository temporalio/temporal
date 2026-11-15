package allowed

import (
	"testing"

	"github.com/stretchr/testify/require"
	ps "go.temporal.io/testx/parallelsuite"
)

func TestOrdinary(t *testing.T) { require.Equal(t, 1, 1) }

type testSuite struct{ ps.Suite[*testSuite] }

func (other *testSuite) wrongReceiver() {} // want "suite receiver must be named s"

func checkSuiteRules(s *testSuite) {
	require.Equal(s.T(), 1, 1) // want "use suite assertion methods directly"
	s.Run("child", func(s *testSuite) { s.Equal(1, 1) })
	s.Equal(1, 1) // want "receiver is sealed"
}
