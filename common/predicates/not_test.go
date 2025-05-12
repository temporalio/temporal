package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	notSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestNotSuite(t *testing.T) {
	s := new(notSuite)
	suite.Run(t, s)
}

func (s *notSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *notSuite) TestNot_Test() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not[int](p1)

	for i := 1; i != 4; i++ {
		s.False(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.True(p.Test(i))
	}

	p = Not(p)
	for i := 1; i != 4; i++ {
		s.True(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = Not(Universal[int]())
	for i := 1; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = Not(Empty[int]())
	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
}

func (s *notSuite) TestNot_Equals() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not[int](p1)

	s.True(p.Equals(p))
	s.True(p.Equals(Not[int](p1)))
	s.True(p.Equals(Not[int](newTestPredicate(3, 2, 1))))

	s.False(p.Equals(newTestPredicate(1, 2, 3)))
	s.False(p.Equals(Not[int](newTestPredicate(4, 5, 6))))
	s.False(p.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(p.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(p.Equals(Empty[int]()))
	s.False(p.Equals(Universal[int]()))
}

func (s *notSuite) TestNot_Size() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not(p1)

	s.Equal(28, p.Size()) // 8 bytes per int64 * 3 ints + 4 bytes of overhead.
}
