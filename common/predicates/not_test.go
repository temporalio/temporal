package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	notSuite struct {
		suite.Suite
	}
)

func TestNotSuite(t *testing.T) {
	s := new(notSuite)
	suite.Run(t, s)
}

func (s *notSuite) TestNot_Test() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not[int](p1)

	for i := 1; i != 4; i++ {
		require.False(s.T(), p.Test(i))
	}
	for i := 4; i != 7; i++ {
		require.True(s.T(), p.Test(i))
	}

	p = Not(p)
	for i := 1; i != 4; i++ {
		require.True(s.T(), p.Test(i))
	}
	for i := 4; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}

	p = Not(Universal[int]())
	for i := 1; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}

	p = Not(Empty[int]())
	for i := 1; i != 7; i++ {
		require.True(s.T(), p.Test(i))
	}
}

func (s *notSuite) TestNot_Equals() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not[int](p1)

	require.True(s.T(), p.Equals(p))
	require.True(s.T(), p.Equals(Not[int](p1)))
	require.True(s.T(), p.Equals(Not[int](newTestPredicate(3, 2, 1))))

	require.False(s.T(), p.Equals(newTestPredicate(1, 2, 3)))
	require.False(s.T(), p.Equals(Not[int](newTestPredicate(4, 5, 6))))
	require.False(s.T(), p.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	require.False(s.T(), p.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	require.False(s.T(), p.Equals(Empty[int]()))
	require.False(s.T(), p.Equals(Universal[int]()))
}

func (s *notSuite) TestNot_Size() {
	p1 := newTestPredicate(1, 2, 3)
	p := Not(p1)

	require.Equal(s.T(), 28, p.Size()) // 8 bytes per int64 * 3 ints + 4 bytes of overhead.
}
