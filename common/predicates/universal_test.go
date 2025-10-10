package predicates

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	universalSuite struct {
		suite.Suite

		universal Predicate[int]
	}
)

func TestUniversalSuite(t *testing.T) {
	s := new(universalSuite)
	suite.Run(t, s)
}

func (s *universalSuite) SetupTest() {

	s.universal = Universal[int]()
}

func (s *universalSuite) TestUniversal_Test() {
	for i := 0; i != 10; i++ {
		require.True(s.T(), s.universal.Test(rand.Int()))
	}
}

func (s *universalSuite) TestUniversal_Equals() {
	require.True(s.T(), s.universal.Equals(s.universal))
	require.True(s.T(), s.universal.Equals(Universal[int]()))

	require.False(s.T(), s.universal.Equals(newTestPredicate(1, 2, 3)))
	require.False(s.T(), s.universal.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	require.False(s.T(), s.universal.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	require.False(s.T(), s.universal.Equals(Not[int](newTestPredicate(1, 2, 3))))
	require.False(s.T(), s.universal.Equals(Empty[int]()))
}
