package tasks

type (
	SequentialTaskQueueFactory[T Task] func(task T) SequentialTaskQueue[T]

	SequentialTaskQueue[T Task] interface {
		// ID return the ID of the queue, as well as the tasks inside (same)
		ID() any
		// Add push a task to the task set. Implementations must not block:
		// SequentialScheduler calls Add while holding the write lock of the shard
		// that owns this queue, and the worker draining the queue needs that same
		// lock to remove it once empty.
		Add(T)
		// Remove pop a task from the task set
		Remove() T
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}
)
