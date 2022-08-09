// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package predicates

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	orSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestOrSuite(t *testing.T) {
	s := new(orSuite)
	suite.Run(t, s)
}

func (s *orSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *orSuite) TestOr_Normal() {
	p1 := newTestPredicate(1, 2, 6)
	p2 := Or[int](
		newTestPredicate(3, 4, 6),
		newTestPredicate(4, 5, 6),
	)
	p := Or[int](p1, p2)

	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
	s.False(p.Test(7))
}

func (s *orSuite) TestOr_All() {
	p := Or[int](
		newTestPredicate(1, 2, 3),
		Universal[int](),
	)

	for i := 1; i != 7; i++ {
		s.True(p.Test(i))
	}
}

func (s *orSuite) TestOr_None() {
	p := Or[int](
		newTestPredicate(1, 2, 3),
		Empty[int](),
	)

	for i := 1; i != 4; i++ {
		s.True(p.Test(i))
	}
	for i := 4; i != 7; i++ {
		s.False(p.Test(i))
	}

	p = Or(
		Empty[int](),
		Empty[int](),
	)
	for i := 1; i != 7; i++ {
		s.False(p.Test(i))
	}
}

func (s *orSuite) TestOr_Duplication() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p3 := newTestPredicate(3, 4, 5)

	_, ok := Or[int](p1, p1).(*testPredicate)
	s.True(ok)

	p := Or[int](p1, p2)
	pOr, ok := Or[int](p, p1).(*OrImpl[int])
	s.True(ok)
	s.Len(pOr.Predicates, 2)

	pOr, ok = Or(p, p).(*OrImpl[int])
	s.True(ok)
	s.Len(pOr.Predicates, 2)

	pOr, ok = Or(p, Or[int](p1, p2)).(*OrImpl[int])
	s.True(ok)
	s.Len(pOr.Predicates, 2)

	pOr, ok = Or(p, Or[int](p1, p2, p3)).(*OrImpl[int])
	s.True(ok)
	s.Len(pOr.Predicates, 3)
}

func (s *orSuite) TestOr_Equals() {
	p1 := newTestPredicate(1, 2, 3)
	p2 := newTestPredicate(2, 3, 4)
	p := Or[int](p1, p2)

	s.True(p.Equals(p))
	s.True(p.Equals(Or[int](p1, p2)))
	s.True(p.Equals(Or[int](p2, p1)))
	s.True(p.Equals(Or[int](p1, p1, p2)))
	s.True(p.Equals(Or[int](
		newTestPredicate(4, 3, 2),
		newTestPredicate(3, 2, 1),
	)))

	s.False(p.Equals(p1))
	s.False(p.Equals(Or[int](p2, p2)))
	s.False(p.Equals(Or[int](p2, newTestPredicate(5, 6, 7))))
	s.False(p.Equals(And[int](p1, p2)))
	s.False(p.Equals(Not(p)))
	s.False(p.Equals(Empty[int]()))
	s.False(p.Equals(Universal[int]()))
}
