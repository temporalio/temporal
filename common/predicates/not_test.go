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
