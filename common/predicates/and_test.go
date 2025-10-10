package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	andSuite struct {
		suite.Suite
	}
)

func TestAndSuite(t *testing.T) {
	s := new(andSuite)
	suite.Run(t, s)
}



func (s *andSuite) TestAnd_Normal() {
	p1 := newTestPredicate(1, 2, 6)
	p2 := And[int](
		newTestPredicate(3, 4, 6),
		newTestPredicate(4, 5, 6),
	)
	p := And[int](p1, p2)

	for i := 1; i != 6; i++ {
		require.False(s.T(), p.Test(i))
	}
	require.True(s.T(), p.Test(6))
}

func (s *andSuite) TestAnd_All() {
	p := And[int](
		newTestPredicate(1, 2, 3),
		Universal[int](),
	)

	for i := 1; i != 4; i++ {
		require.True(s.T(), p.Test(i))
	}
	for i := 4; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}

	p = And(
		Universal[int](),
		Universal[int](),
	)
	for i := 1; i != 7; i++ {
		require.True(s.T(), p.Test(i))
	}
}

func (s *andSuite) TestAnd_None() {
	p := And[int](
		newTestPredicate(1, 2, 3),
		Empty[int](),
	)

	for i := 1; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}
}

func (s *andSuite) TestAnd_Duplication() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p3 := newTestPredicate(3, 4, 5)

	_, ok := And[int](p1, p1).(*testPredicate)
	require.True(s.T(), ok)

	p := And[int](p1, p2)
	pAnd, ok := And[int](p, p1).(*AndImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pAnd.Predicates, 2)

	pAnd, ok = And(p, p).(*AndImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pAnd.Predicates, 2)

	pAnd, ok = And(p, And[int](p1, p2)).(*AndImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pAnd.Predicates, 2)

	pAnd, ok = And(p, And[int](p1, p2, p3)).(*AndImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pAnd.Predicates, 3)
}

func (s *andSuite) TestAnd_Equals() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p := And[int](p1, p2)

	require.True(s.T(), p.Equals(p))
	require.True(s.T(), p.Equals(And[int](p1, p2)))
	require.True(s.T(), p.Equals(And[int](p2, p1)))
	require.True(s.T(), p.Equals(And[int](p1, p1, p2)))
	require.True(s.T(), p.Equals(And[int](
		newTestPredicate(4, 3, 2),
		newTestPredicate(3, 2, 1),
	)))

	require.False(s.T(), p.Equals(p1))
	require.False(s.T(), p.Equals(And[int](p2, p2)))
	require.False(s.T(), p.Equals(And[int](p2, newTestPredicate(5, 6, 7))))
	require.False(s.T(), p.Equals(Or[int](p1, p2)))
	require.False(s.T(), p.Equals(Not(p)))
	require.False(s.T(), p.Equals(Empty[int]()))
	require.False(s.T(), p.Equals(Universal[int]()))
}

func (s *andSuite) TestAnd_Size() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p := And(p1, p2)

	require.Equal(s.T(), 52, p.Size()) // 8 bytes per int64 * 6 ints + 4 bytes of overhead.
}
