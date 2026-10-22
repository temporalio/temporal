package tasks

import "sync"

type testSequentialTaskQueue[T Task] struct {
	sync.Mutex
	tasks []T
	id    int
}

func newTestSequentialTaskQueue[T Task](id int) SequentialTaskQueue[T] {
	return &testSequentialTaskQueue[T]{
		id: id,
	}
}

func (s *testSequentialTaskQueue[T]) ID() any {
	return s.id
}

func (s *testSequentialTaskQueue[T]) Add(task T) {
	s.Lock()
	defer s.Unlock()
	s.tasks = append(s.tasks, task)
}

func (s *testSequentialTaskQueue[T]) Remove() T {
	s.Lock()
	defer s.Unlock()
	var task T
	if len(s.tasks) == 0 {
		return task
	}
	task, s.tasks = s.tasks[0], s.tasks[1:]
	return task
}

func (s *testSequentialTaskQueue[T]) IsEmpty() bool {
	s.Lock()
	defer s.Unlock()
	return len(s.tasks) == 0
}

func (s *testSequentialTaskQueue[T]) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.tasks)
}
