package tasks

type testSequentialTaskQueue[T Task] struct {
	q  chan T
	id int
}

func newTestSequentialTaskQueue[T Task](id, capacity int) SequentialTaskQueue[T] {
	return &testSequentialTaskQueue[T]{
		q:  make(chan T, capacity),
		id: id,
	}
}

func (s *testSequentialTaskQueue[T]) ID() interface{} {
	return s.id
}

func (s *testSequentialTaskQueue[T]) Add(task T) {
	s.q <- task
}

func (s *testSequentialTaskQueue[T]) Remove() T {
	select {
	case t := <-s.q:
		return t
	default:
		var emptyT T
		return emptyT
	}
}

func (s *testSequentialTaskQueue[T]) IsEmpty() bool {
	return len(s.q) == 0
}

func (s *testSequentialTaskQueue[T]) Len() int {
	return len(s.q)
}
