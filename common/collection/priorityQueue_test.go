package collection

import (
	"math/rand"
	"runtime"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	PriorityQueueSuite struct {
		suite.Suite
		pq Queue
	}

	testPriorityQueueItem struct {
		value int
	}
)

func testPriorityQueueItemCompareLess(this interface{}, that interface{}) bool {
	return this.(*testPriorityQueueItem).value < that.(*testPriorityQueueItem).value
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
		result = append(result, s.pq.Remove().(*testPriorityQueueItem).value)
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
		result = append(result, s.pq.Remove().(*testPriorityQueueItem).value)
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
			result = append(result, s.pq.Remove().(*testPriorityQueueItem).value)
		}
		s.Equal(expected, result)
	}
}

type testTask struct {
	id       string
	priority int
}

func BenchmarkConcurrentPriorityQueue(b *testing.B) {
	queue := NewConcurrentPriorityQueue(func(this interface{}, other interface{}) bool {
		return this.(*testTask).priority < other.(*testTask).priority
	})

	for i := 0; i < 100; i++ {
		go send(queue)
	}

	for n := 0; n < b.N; n++ {
		remove(queue)
	}
}

func remove(queue Queue) interface{} {
	for queue.IsEmpty() {
		runtime.Gosched()
	}
	return queue.Remove()
}

func send(queue Queue) {
	for {
		t := &testTask{
			id:       "abc",
			priority: rand.Int() % numPriorities,
		}
		queue.Add(t)
	}
}
