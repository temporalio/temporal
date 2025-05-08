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
		*require.Assertions

		universal Predicate[int]
	}
)

func TestUniversalSuite(t *testing.T) {
	s := new(universalSuite)
	suite.Run(t, s)
}

func (s *universalSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.universal = Universal[int]()
}

func (s *universalSuite) TestUniversal_Test() {
	for i := 0; i != 10; i++ {
		s.True(s.universal.Test(rand.Int()))
	}
}

func (s *universalSuite) TestUniversal_Equals() {
	s.True(s.universal.Equals(s.universal))
	s.True(s.universal.Equals(Universal[int]()))

	s.False(s.universal.Equals(newTestPredicate(1, 2, 3)))
	s.False(s.universal.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(s.universal.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(s.universal.Equals(Not[int](newTestPredicate(1, 2, 3))))
	s.False(s.universal.Equals(Empty[int]()))
}
