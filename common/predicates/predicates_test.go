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
	"golang.org/x/exp/maps"
)

var _ Predicate[int] = (*testPredicate)(nil)

type (
	testPredicate struct {
		nums map[int]struct{}
	}
)

func newTestPredicate(nums ...int) *testPredicate {
	numsMap := make(map[int]struct{}, len(nums))
	for _, x := range nums {
		numsMap[x] = struct{}{}
	}
	return &testPredicate{
		nums: numsMap,
	}
}

func (p *testPredicate) Test(x int) bool {
	_, ok := p.nums[x]
	return ok
}

func (p *testPredicate) Equals(predicate Predicate[int]) bool {
	testPrediate, ok := predicate.(*testPredicate)
	if !ok {
		return false
	}

	return maps.Equal(p.nums, testPrediate.nums)
}
