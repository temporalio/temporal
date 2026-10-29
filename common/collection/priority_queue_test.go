package collection

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestPriorityQueueRemoveReleasesItems(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		preloaded bool
		values    []int
	}{
		{name: "added single item", values: []int{1}},
		{name: "added multiple items", values: []int{3, 1, 2}},
		{name: "preloaded single item", preloaded: true, values: []int{1}},
		{name: "preloaded multiple items", preloaded: true, values: []int{3, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			items := make([]*testPriorityQueueItem, len(tc.values))
			for i, value := range tc.values {
				items[i] = &testPriorityQueueItem{value: value}
			}
			queue := NewPriorityQueue(testPriorityQueueItemCompareLess)
			if tc.preloaded {
				queue = NewPriorityQueueWithItems(testPriorityQueueItemCompareLess, items)
			} else {
				for _, item := range items {
					queue.Add(item)
				}
			}
			pq := queue.(*priorityQueueImpl[*testPriorityQueueItem])
			for i := range len(tc.values) {
				require.Equal(t, &testPriorityQueueItem{value: i + 1}, queue.Remove())
				require.Equal(t, make([]*testPriorityQueueItem, cap(pq.items)-pq.Len()), pq.items[:cap(pq.items)][pq.Len():])
			}
			item := &testPriorityQueueItem{value: 4}
			queue.Add(item)
			require.Same(t, item, queue.Remove())
			require.Equal(t, make([]*testPriorityQueueItem, cap(pq.items)), pq.items[:cap(pq.items)])
		})
	}
}

func TestPriorityQueueRemoveReleasesInterfaceItems(t *testing.T) {
	t.Parallel()
	for _, preloaded := range []bool{false, true} {
		name := "added items"
		if preloaded {
			name = "preloaded items"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			compareLess := func(a, b any) bool {
				return a.(*testPriorityQueueItem).value < b.(*testPriorityQueueItem).value
			}
			items := []any{&testPriorityQueueItem{value: 2}, &testPriorityQueueItem{value: 1}}
			queue := NewPriorityQueue(compareLess)
			if preloaded {
				queue = NewPriorityQueueWithItems(compareLess, items)
			} else {
				for _, item := range items {
					queue.Add(item)
				}
			}
			pq := queue.(*priorityQueueImpl[any])
			for i := range len(items) {
				require.Equal(t, &testPriorityQueueItem{value: i + 1}, queue.Remove())
				require.Equal(t, make([]any, cap(pq.items)-pq.Len()), pq.items[:cap(pq.items)][pq.Len():])
			}
		})
	}
}

func (s *PriorityQueueSuite) SetupTest() {
	s.pq = NewPriorityQueue(testPriorityQueueItemCompareLess)
}

func (s *PriorityQueueSuite) TestNewPriorityQueueWithItems() {
	items := []*testPriorityQueueItem{
		{value: 10},
		{value: 3},
		{value: 5},
		{value: 4},
		{value: 1},
		{value: 16},
		{value: -10},
	}
	s.pq = NewPriorityQueueWithItems(
		testPriorityQueueItemCompareLess,
		items,
	)

	expected := []int{-10, 1, 3, 4, 5, 10, 16}
	result := []int{}

	for !s.pq.IsEmpty() {
		result = append(result, s.pq.Remove().value)
	}
	s.Equal(expected, result)
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
	for range 1000 {

		expected := []int{}
		result := []int{}
		for range 1000 {
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
