package matching

import (
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

// a circular array of a fixed size for tracking tasks
type circularTaskBuffer struct {
	buffer     []int32
	currentPos int
}

func newCircularTaskBuffer(size int) circularTaskBuffer {
	return circularTaskBuffer{
		buffer: make([]int32, size),
	}
}

func (cb *circularTaskBuffer) inc(n int) {
	cb.buffer[cb.currentPos] += int32(n)
}

func (cb *circularTaskBuffer) advance() {
	cb.currentPos = (cb.currentPos + 1) % len(cb.buffer)
	cb.buffer[cb.currentPos] = 0 // Reset the task count for the new interval
}

// returns the total number of tasks in the buffer
func (cb *circularTaskBuffer) totalTasks() int {
	totalTasks := 0
	for _, count := range cb.buffer {
		totalTasks += int(count)
	}
	return totalTasks
}

type taskTracker struct {
	lock            sync.Mutex
	clock           clock.TimeSource
	startTime       time.Time     // time when taskTracker was initialized
	bucketStartTime time.Time     // the starting time of a bucket in the buffer
	bucketSize      time.Duration // duration of each bucket in the buffer
	buckets         int           // number of buckets in the buffer
	totalInterval   time.Duration // duration over which rate of tasks is measured
	tasks           circularTaskBuffer
}

func newTaskTracker(
	timeSource clock.TimeSource,
	bucketSize time.Duration,
	totalInterval time.Duration,
) *taskTracker {
	bucketSize = max(bucketSize, time.Millisecond)
	buckets := int(totalInterval/bucketSize) + 1
	return &taskTracker{
		clock:           timeSource,
		startTime:       timeSource.Now(),
		bucketStartTime: timeSource.Now(),
		bucketSize:      bucketSize,
		buckets:         buckets,
		totalInterval:   totalInterval,
		tasks:           newCircularTaskBuffer(buckets),
	}
}

// advanceAndResetLocked advances the trackers position and clears out any expired intervals
// This method must be called with taskTracker's lock held.
func (s *taskTracker) advanceAndResetLocked(elapsed time.Duration) {
	// Calculate the number of intervals elapsed since the start interval time
	intervalsElapsed := int(elapsed / s.bucketSize)

	for range min(intervalsElapsed, s.buckets) {
		s.tasks.advance() // advancing our circular buffer's position until we land on the right interval
	}
	s.bucketStartTime = s.bucketStartTime.Add(time.Duration(intervalsElapsed) * s.bucketSize)
}

// inc increments the count of tasks by n at the current time
func (s *taskTracker) inc(n int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetLocked(elapsed)
	s.tasks.inc(n)
}

// rate returns the rate of tasks added/dispatched in a given interval
func (s *taskTracker) rate() float32 {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetLocked(elapsed)
	totalTasks := s.tasks.totalTasks()

	elapsedTime := min(currentTime.Sub(s.bucketStartTime)+s.totalInterval,
		currentTime.Sub(s.startTime))

	if elapsedTime <= 0 {
		return 0
	}

	// rate per second
	return float32(totalTasks) / float32(elapsedTime.Seconds())
}
