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

package matching

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	taskIDSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestTaskIDSuite(t *testing.T) {
	s := new(taskIDSuite)
	suite.Run(t, s)
}

func (s *taskIDSuite) SetupSuite() {
}

func (s *taskIDSuite) TearDownSuite() {
}

func (s *taskIDSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *taskIDSuite) TearDownTest() {

}

func (s *taskIDSuite) TestSort_Sorted() {
	expectedTaskIDs := []int64{1, 3, 5, 10001, 10005, 10008, 100001, 100007, 200017}

	taskIDSlice := make([]int64, len(expectedTaskIDs))
	copy(taskIDSlice, expectedTaskIDs)
	actualTaskIDs := taskIDs(taskIDSlice)
	sort.Sort(actualTaskIDs)

	s.Equal(expectedTaskIDs, []int64(actualTaskIDs))
}

func (s *taskIDSuite) TestSort_ReverseSorted() {
	expectedTaskIDs := []int64{1, 3, 5, 10001, 10005, 10008, 100001, 100007, 200017}
	actualTaskIDs := taskIDs([]int64{200017, 100007, 100001, 10008, 10005, 10001, 5, 3, 1})

	sort.Sort(actualTaskIDs)

	s.Equal(expectedTaskIDs, []int64(actualTaskIDs))
}

func (s *taskIDSuite) TestSort_Random() {
	totalNum := 4096
	var actualTaskIDs taskIDs
	for i := 0; i < totalNum; i++ {
		actualTaskIDs = append(actualTaskIDs, rand.Int63())
	}
	sort.Sort(actualTaskIDs)

	for i := 1; i < totalNum; i++ {
		s.True(actualTaskIDs[i-1] <= actualTaskIDs[i])
	}
}
