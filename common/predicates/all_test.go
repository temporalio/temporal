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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	allSuite struct {
		suite.Suite
		*require.Assertions

		all Predicate[int]
	}
)

func TestAllSuite(t *testing.T) {
	s := new(allSuite)
	suite.Run(t, s)
}

func (s *allSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.all = All[int]()
}

func (s *allSuite) TestAll_Test() {
	for i := 1; i != 10; i++ {
		s.True(s.all.Test(rand.Int()))
	}
}

func (s *allSuite) TestAll_Equals() {
	s.True(s.all.Equals(s.all))
	s.True(s.all.Equals(All[int]()))

	s.False(s.all.Equals(newTestPredicate(1, 2, 3)))
	s.False(s.all.Equals(And[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(2, 3, 4),
	)))
	s.False(s.all.Equals(Or[int](
		newTestPredicate(1, 2, 3),
		newTestPredicate(4, 5, 6),
	)))
	s.False(s.all.Equals(Not[int](newTestPredicate(1, 2, 3))))
	s.False(s.all.Equals(Empty[int]()))
}
