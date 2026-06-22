package predicates

import (
	"math/rand"
	"testing"

	"go.temporal.io/server/common/testing/parallelsuite"
)

type (
	universalSuite struct {
		parallelsuite.Suite[*universalSuite]
	}
)

func TestUniversalSuite(t *testing.T) {
	parallelsuite.Run(t, new(universalSuite))
}

func (s *universalSuite) TestUniversal_Test() {
	universal := Universal[int]()
	for i := 0; i != 10; i++ {
		s.True(universal.Test(rand.Int()))
	}
}

func (s *universalSuite) TestUniversal_Equals() {
	universal := Universal[int]()
	s.True(universal.Equals(universal))
	s.True(universal.Equals(Universal[int]()))

	s.False(universal.Equals(newTestPredicate(1, 2, 3)))
	s.False(universal.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(universal.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(universal.Equals(Not[int](newTestPredicate(1, 2, 3))))
	s.False(universal.Equals(Empty[int]()))
}
