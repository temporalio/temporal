package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	emptySuite struct {
		suite.Suite
		*require.Assertions

		empty Predicate[int]
	}
)

func TestNoneSuite(t *testing.T) {
	s := new(emptySuite)
	suite.Run(t, s)
}

func (s *emptySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.empty = Empty[int]()
}

func (s *emptySuite) TestEmpty_Test() {
	for i := 0; i != 10; i++ {
		s.False(s.empty.Test(i))
	}
}

func (s *emptySuite) TestEmpty_Equals() {
	s.True(s.empty.Equals(s.empty))
	s.True(s.empty.Equals(Empty[int]()))

	s.False(s.empty.Equals(newTestPredicate(1, 2, 3)))
	s.False(s.empty.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(s.empty.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(s.empty.Equals(Not[int](newTestPredicate(1, 2, 3))))
	s.False(s.empty.Equals(Universal[int]()))
}
