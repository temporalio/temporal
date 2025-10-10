package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	emptySuite struct {
		suite.Suite

		emtpy Predicate[int]
	}
)

func TestNoneSuite(t *testing.T) {
	s := new(emptySuite)
	suite.Run(t, s)
}

func (s *emptySuite) SetupTest() {

	s.emtpy = Empty[int]()
}

func (s *emptySuite) TestEmpty_Test() {
	for i := 0; i != 10; i++ {
		require.False(s.T(), s.emtpy.Test(i))
	}
}

func (s *emptySuite) TestEmpty_Equals() {
	require.True(s.T(), s.emtpy.Equals(s.emtpy))
	require.True(s.T(), s.emtpy.Equals(Empty[int]()))

	require.False(s.T(), s.emtpy.Equals(newTestPredicate(1, 2, 3)))
	require.False(s.T(), s.emtpy.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	require.False(s.T(), s.emtpy.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	require.False(s.T(), s.emtpy.Equals(Not[int](newTestPredicate(1, 2, 3))))
	require.False(s.T(), s.emtpy.Equals(Universal[int]()))
}
