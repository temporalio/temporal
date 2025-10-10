package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	orSuite struct {
		suite.Suite
	}
)

func TestOrSuite(t *testing.T) {
	s := new(orSuite)
	suite.Run(t, s)
}



func (s *orSuite) TestOr_Normal() {
	p1 := newTestPredicate(1, 2, 6)
	p2 := Or[int](
		newTestPredicate(3, 4, 6),
		newTestPredicate(4, 5, 6),
	)
	p := Or[int](p1, p2)

	for i := 1; i != 7; i++ {
		require.True(s.T(), p.Test(i))
	}
	require.False(s.T(), p.Test(7))
}

func (s *orSuite) TestOr_All() {
	p := Or[int](
		newTestPredicate(1, 2, 3),
		Universal[int](),
	)

	for i := 1; i != 7; i++ {
		require.True(s.T(), p.Test(i))
	}
}

func (s *orSuite) TestOr_None() {
	p := Or[int](
		newTestPredicate(1, 2, 3),
		Empty[int](),
	)

	for i := 1; i != 4; i++ {
		require.True(s.T(), p.Test(i))
	}
	for i := 4; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}

	p = Or(
		Empty[int](),
		Empty[int](),
	)
	for i := 1; i != 7; i++ {
		require.False(s.T(), p.Test(i))
	}
}

func (s *orSuite) TestOr_Duplication() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p3 := newTestPredicate(3, 4, 5)

	_, ok := Or[int](p1, p1).(*testPredicate)
	require.True(s.T(), ok)

	p := Or[int](p1, p2)
	pOr, ok := Or[int](p, p1).(*OrImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pOr.Predicates, 2)

	pOr, ok = Or(p, p).(*OrImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pOr.Predicates, 2)

	pOr, ok = Or(p, Or[int](p1, p2)).(*OrImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pOr.Predicates, 2)

	pOr, ok = Or(p, Or[int](p1, p2, p3)).(*OrImpl[int])
	require.True(s.T(), ok)
	require.Len(s.T(), pOr.Predicates, 3)
}

func (s *orSuite) TestOr_Equals() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p := Or[int](p1, p2)

	require.True(s.T(), p.Equals(p))
	require.True(s.T(), p.Equals(Or[int](p1, p2)))
	require.True(s.T(), p.Equals(Or[int](p2, p1)))
	require.True(s.T(), p.Equals(Or[int](p1, p1, p2)))
	require.True(s.T(), p.Equals(Or[int](
		newTestPredicate(4, 3, 2),
		newTestPredicate(3, 2, 1),
	)))

	require.False(s.T(), p.Equals(p1))
	require.False(s.T(), p.Equals(Or[int](p2, p2)))
	require.False(s.T(), p.Equals(Or[int](p2, newTestPredicate(5, 6, 7))))
	require.False(s.T(), p.Equals(And[int](p1, p2)))
	require.False(s.T(), p.Equals(Not(p)))
	require.False(s.T(), p.Equals(Empty[int]()))
	require.False(s.T(), p.Equals(Universal[int]()))
}

func (s *orSuite) TestOr_Size() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p := Or(p1, p2)

	require.Equal(s.T(), 52, p.Size()) // 8 bytes per int64 * 6 ints + 4 bytes of overhead.
}
