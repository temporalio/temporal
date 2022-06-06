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

package tasks

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	rangeSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestRangeSuite(t *testing.T) {
	s := new(rangeSuite)
	suite.Run(t, s)
}

func (s *rangeSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *rangeSuite) TestNewRange_Invalid() {
	minKey := s.newRandomKey()
	maxKey := NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()-1),
		minKey.TaskID,
	)
	s.Panics(func() { NewRange(minKey, maxKey) })

	maxKey = NewKey(minKey.FireTime, minKey.TaskID-1)
	s.Panics(func() { NewRange(minKey, maxKey) })
}

func (s *rangeSuite) TestNewRange_Valid() {
	minKey := s.newRandomKey()
	_ = NewRange(minKey, minKey)

	maxKey := NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID,
	)
	_ = NewRange(minKey, maxKey)

	maxKey = NewKey(minKey.FireTime, minKey.TaskID+1)
	_ = NewRange(minKey, maxKey)

	maxKey = NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID-1,
	)
	_ = NewRange(minKey, maxKey)
}

func (s *rangeSuite) TestIsEmpty() {
	minKey := s.newRandomKey()
	r := NewRange(minKey, minKey)
	s.True(r.IsEmpty())

	maxKey := NewKey(
		time.Unix(0, minKey.FireTime.UnixNano()+1),
		minKey.TaskID,
	)
	r = NewRange(minKey, maxKey)
	s.False(r.IsEmpty())

	maxKey = NewKey(minKey.FireTime, minKey.TaskID+1)
	r = NewRange(minKey, maxKey)
	s.False(r.IsEmpty())
}

func (s *rangeSuite) TestContainsKey_EmptyRange() {
	key := s.newRandomKey()
	r := NewRange(key, key)

	testKey := key
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(key.FireTime.Add(time.Nanosecond), key.TaskID)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(key.FireTime.Add(-time.Nanosecond), key.TaskID)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(key.FireTime, key.TaskID-1)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(key.FireTime, key.TaskID+1)
	s.False(r.ContainsKey(testKey))
}

func (s *rangeSuite) TestContainsKey_NonEmptyRange() {
	r := s.newRandomRange()

	testKey := r.InclusiveMin
	s.True(r.ContainsKey(testKey))

	testKey = r.ExclusiveMax
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(r.InclusiveMin.FireTime.Add(-time.Nanosecond), r.InclusiveMin.TaskID)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(r.ExclusiveMax.FireTime.Add(time.Nanosecond), r.ExclusiveMax.TaskID)
	s.False(r.ContainsKey(testKey))

	testKey = NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1)
	s.False(r.ContainsKey(testKey))

	for i := 0; i != 1000; i++ {
		s.True(r.ContainsKey(s.newRandomKeyInRange(r)))
	}
}

func (s *rangeSuite) TestContainsRange_EmptyRange() {
	r := s.newRandomRange()
	s.True(r.ContainsRange(NewRange(r.InclusiveMin, r.InclusiveMin)))
	s.True(r.ContainsRange(NewRange(r.ExclusiveMax, r.ExclusiveMax)))

	key := s.newRandomKey()
	r = NewRange(key, key)
	s.True(r.ContainsRange(r))

	s.False(r.ContainsRange(s.newRandomRange()))
}

func (s *rangeSuite) TestContainsRange_NonEmptyRange() {
	r := s.newRandomRange()

	testRange := r
	s.True(r.ContainsRange(testRange))

	testRange = NewRange(
		NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID+1),
		r.ExclusiveMax,
	)
	s.True(r.ContainsRange(testRange))
	s.False(testRange.ContainsRange(r))

	testRange = NewRange(
		r.InclusiveMin,
		NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
	)
	s.False(r.ContainsRange(testRange))
	s.True(testRange.ContainsRange(r))

	testRange = NewRange(
		s.newRandomKeyInRange(r),
		r.ExclusiveMax,
	)
	s.True(r.ContainsRange(testRange))
	s.False(testRange.ContainsRange(r))

	testRange = NewRange(
		r.InclusiveMin,
		s.newRandomKeyInRange(r),
	)
	s.True(r.ContainsRange(testRange))
	s.False(testRange.ContainsRange(r))
}

func (s *rangeSuite) TestCanSplit() {
	key := s.newRandomKey()
	ranges := []Range{
		s.newRandomRange(),
		NewRange(key, key),
	}

	for _, r := range ranges {
		testKey := r.InclusiveMin
		s.True(r.CanSplit(testKey))

		testKey = r.ExclusiveMax
		s.True(r.CanSplit(testKey))

		if !r.IsEmpty() {
			for i := 0; i != 1000; i++ {
				s.True(r.CanSplit(s.newRandomKeyInRange(r)))
			}
		}
	}
}

func (s *rangeSuite) TestCanMerge() {
	key := s.newRandomKey()
	ranges := []Range{
		s.newRandomRange(),
		NewRange(key, key),
	}

	for _, r := range ranges {
		if !r.IsEmpty() {
			testRange := NewRange(
				NewKey(DefaultFireTime, 0),
				s.newRandomKeyInRange(r),
			)
			s.True(r.CanMerge(testRange))
			s.True(testRange.CanMerge(r))

			testRange = NewRange(
				s.newRandomKeyInRange(r),
				NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64),
			)
			s.True(r.CanMerge(testRange))
			s.True(testRange.CanMerge(r))
		}

		testRange := NewRange(
			NewKey(DefaultFireTime, 0),
			NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64),
		)
		s.True(r.CanMerge(testRange))
		s.True(testRange.CanMerge(r))

		testRange = NewRange(
			NewKey(DefaultFireTime, 0),
			r.InclusiveMin,
		)
		s.True(r.CanMerge(testRange))
		s.True(testRange.CanMerge(r))

		testRange = NewRange(
			r.ExclusiveMax,
			NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64),
		)
		s.True(r.CanMerge(testRange))
		s.True(testRange.CanMerge(r))

		testRange = NewRange(
			NewKey(DefaultFireTime, 0),
			NewKey(r.InclusiveMin.FireTime, r.InclusiveMin.TaskID-1),
		)
		s.False(r.CanMerge(testRange))
		s.False(testRange.CanMerge(r))

		testRange = NewRange(
			NewKey(r.ExclusiveMax.FireTime, r.ExclusiveMax.TaskID+1),
			NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64),
		)
		s.False(r.CanMerge(testRange))
		s.False(testRange.CanMerge(r))
	}
}

func (s *rangeSuite) TestSplit() {
	r := s.newRandomRange()
	splitKey := s.newRandomKeyInRange(r)

	left, right := r.Split(splitKey)
	s.True(left.Equal(NewRange(r.InclusiveMin, splitKey)))
	s.True(right.Equal(NewRange(splitKey, r.ExclusiveMax)))
}

func (s *rangeSuite) TestMerge() {
	r := s.newRandomRange()
	minKey := NewKey(DefaultFireTime, 0)
	maxKey := NewKey(time.Unix(0, math.MaxInt64), math.MaxInt64)

	testRange := NewRange(
		minKey,
		s.newRandomKeyInRange(r),
	)
	mergedRange := r.Merge(testRange)
	s.True(mergedRange.Equal(testRange.Merge(r)))
	s.True(mergedRange.Equal(NewRange(minKey, r.ExclusiveMax)))

	testRange = NewRange(
		s.newRandomKeyInRange(r),
		maxKey,
	)
	mergedRange = r.Merge(testRange)
	s.True(mergedRange.Equal(testRange.Merge(r)))
	s.True(mergedRange.Equal(NewRange(r.InclusiveMin, maxKey)))

	testRange = NewRange(minKey, maxKey)
	mergedRange = r.Merge(testRange)
	s.True(mergedRange.Equal(testRange.Merge(r)))
	s.True(mergedRange.Equal(NewRange(minKey, maxKey)))

	testRange = NewRange(minKey, r.InclusiveMin)
	mergedRange = r.Merge(testRange)
	s.True(mergedRange.Equal(testRange.Merge(r)))
	s.True(mergedRange.Equal(NewRange(minKey, r.ExclusiveMax)))

	testRange = NewRange(r.ExclusiveMax, maxKey)
	mergedRange = r.Merge(testRange)
	s.True(mergedRange.Equal(testRange.Merge(r)))
	s.True(mergedRange.Equal(NewRange(r.InclusiveMin, maxKey)))
}

func (s *rangeSuite) newRandomKey() Key {
	return NewKey(time.Unix(0, rand.Int63()), rand.Int63())
}

func (s *rangeSuite) newRandomRange() Range {
	maxKey := s.newRandomKey()
	minKey := NewKey(
		time.Unix(0, rand.Int63n(maxKey.FireTime.UnixNano())),
		rand.Int63(),
	)
	return NewRange(minKey, maxKey)
}

func (s *rangeSuite) newRandomKeyInRange(
	r Range,
) Key {
	if r.IsEmpty() {
		panic("can not create key in range for an empty range")
	}

	minFireTimeUnixNano := r.InclusiveMin.FireTime.UnixNano()
	maxFireTimeUnixNano := r.ExclusiveMax.FireTime.UnixNano()
	minTaskID := r.InclusiveMin.TaskID
	maxTaskID := r.ExclusiveMax.TaskID

	if minFireTimeUnixNano == maxFireTimeUnixNano {
		return NewKey(
			r.InclusiveMin.FireTime,
			rand.Int63n(1+maxTaskID-minTaskID)+minTaskID,
		)
	}

	fireTime := time.Unix(0, rand.Int63n(1+maxFireTimeUnixNano-minFireTimeUnixNano)+minFireTimeUnixNano)
	if fireTime.Equal(r.InclusiveMin.FireTime) {
		return NewKey(
			fireTime,
			rand.Int63n(math.MaxInt64-minTaskID)+minTaskID,
		)
	}

	if fireTime.Equal(r.ExclusiveMax.FireTime) {
		return NewKey(
			fireTime,
			rand.Int63n(maxTaskID),
		)
	}

	return NewKey(fireTime, rand.Int63())
}
