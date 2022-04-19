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

package collection

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	PriorityQueueSuite struct {
		suite.Suite
		pq Queue[*testPriorityQueueItem]
	}

	testPriorityQueueItem struct {
		value int
	}
)

func testPriorityQueueItemCompareLess(this *testPriorityQueueItem, that *testPriorityQueueItem) bool {
	return this.value < that.value
}

func TestPriorityQueueSuite(t *testing.T) {
	suite.Run(t, new(PriorityQueueSuite))
}

func (s *PriorityQueueSuite) SetupTest() {
	s.pq = NewPriorityQueue(testPriorityQueueItemCompareLess)
}

func (s *PriorityQueueSuite) TestInsertAndPop() {
	s.pq.Add(&testPriorityQueueItem{10})
	s.pq.Add(&testPriorityQueueItem{3})
	s.pq.Add(&testPriorityQueueItem{5})
	s.pq.Add(&testPriorityQueueItem{4})
	s.pq.Add(&testPriorityQueueItem{1})
	s.pq.Add(&testPriorityQueueItem{16})
	s.pq.Add(&testPriorityQueueItem{-10})

	expected := []int{-10, 1, 3, 4, 5, 10, 16}
	result := []int{}

	for !s.pq.IsEmpty() {
		result = append(result, s.pq.Remove().value)
	}
	s.Equal(expected, result)

	s.pq.Add(&testPriorityQueueItem{1000})
	s.pq.Add(&testPriorityQueueItem{1233})
	s.pq.Remove() // remove 1000
	s.pq.Add(&testPriorityQueueItem{4})
	s.pq.Add(&testPriorityQueueItem{18})
	s.pq.Add(&testPriorityQueueItem{192})
	s.pq.Add(&testPriorityQueueItem{255})
	s.pq.Remove() // remove 4
	s.pq.Remove() // remove 18
	s.pq.Add(&testPriorityQueueItem{59})
	s.pq.Add(&testPriorityQueueItem{727})

	expected = []int{59, 192, 255, 727, 1233}
	result = []int{}

	for !s.pq.IsEmpty() {
		result = append(result, s.pq.Remove().value)
	}
	s.Equal(expected, result)
}

func (s *PriorityQueueSuite) TestRandomNumber() {
	for round := 0; round < 1000; round++ {

		expected := []int{}
		result := []int{}
		for i := 0; i < 1000; i++ {
			num := rand.Int()
			s.pq.Add(&testPriorityQueueItem{num})
			expected = append(expected, num)
		}
		sort.Ints(expected)

		for !s.pq.IsEmpty() {
			result = append(result, s.pq.Remove().value)
		}
		s.Equal(expected, result)
	}
}
