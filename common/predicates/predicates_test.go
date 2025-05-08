package predicates

import (
	"maps"
	"strconv"
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

func (p *testPredicate) Size() int {
	return strconv.IntSize / 8 * len(p.nums)
}
