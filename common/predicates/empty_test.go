package predicates

import (
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

type (
	emptySuite struct {
		parallelsuite.Suite[*emptySuite]
	}
)

func TestNoneSuite(t *testing.T) {
	parallelsuite.Run(t, new(emptySuite))
}

func (s *emptySuite) TestEmpty_Test() {
	empty := Empty[int]()
	for i := 0; i != 10; i++ {
		s.False(empty.Test(i))
	}
}

func (s *emptySuite) TestEmpty_Equals() {
	empty := Empty[int]()
	s.True(empty.Equals(empty))
	s.True(empty.Equals(Empty[int]()))

	s.False(empty.Equals(newTestPredicate(1, 2, 3)))
	s.False(empty.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(empty.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(empty.Equals(Not[int](newTestPredicate(1, 2, 3))))
	s.False(empty.Equals(Universal[int]()))
}
