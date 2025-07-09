package matching

import (
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

const (
	// The duration of each mini-bucket in the circularTaskBuffer
	intervalSize = 5
	// The total duration which is used to calculate the rate of tasks added/dispatched
	totalIntervalSize = 30
)

// a circular array of a fixed size for tracking tasks
type circularTaskBuffer struct {
	buffer     []int
	currentPos int
}

func newCircularTaskBuffer(size int) circularTaskBuffer {
	return circularTaskBuffer{
		buffer: make([]int, size),
	}
}

func (cb *circularTaskBuffer) incrementTaskCount() {
	cb.buffer[cb.currentPos]++
}

func (cb *circularTaskBuffer) advance() {
	cb.currentPos = (cb.currentPos + 1) % len(cb.buffer)
	cb.buffer[cb.currentPos] = 0 // Reset the task count for the new interval
}

// returns the total number of tasks in the buffer
func (cb *circularTaskBuffer) totalTasks() int {
	totalTasks := 0
	for _, count := range cb.buffer {
		totalTasks += count
	}
	return totalTasks
}

type taskTracker struct {
	lock              sync.Mutex
	clock             clock.TimeSource
	startTime         time.Time                    // time when taskTracker was initialized
	bucketStartTime   time.Time                    // the starting time of a bucket in the buffer
	bucketSize        time.Duration                // the duration of each bucket in the buffer
	numberOfBuckets   int                          // the total number of buckets in the buffer
	totalIntervalSize time.Duration                // the number of seconds over which rate of tasks are added/dispatched
	tasksInIntervals  map[int32]circularTaskBuffer // keyed by priority
}

func newTaskTracker(timeSource clock.TimeSource) *taskTracker {
	return &taskTracker{
		clock:             timeSource,
		startTime:         timeSource.Now(),
		bucketStartTime:   timeSource.Now(),
		bucketSize:        time.Duration(intervalSize) * time.Second,
		numberOfBuckets:   (totalIntervalSize / intervalSize) + 1,
		totalIntervalSize: time.Duration(totalIntervalSize) * time.Second,
		tasksInIntervals:  make(map[int32]circularTaskBuffer),
	}
}

// advanceAndResetTracker advances the trackers position and clears out any expired intervals
// This method must be called with taskTracker's lock held.
func (s *taskTracker) advanceAndResetTracker(elapsed time.Duration) {
	// Calculate the number of intervals elapsed since the start interval time
	intervalsElapsed := int(elapsed / s.bucketSize)

	for _, cb := range s.tasksInIntervals {
		for i := 0; i < min(intervalsElapsed, s.numberOfBuckets); i++ {
			cb.advance() // advancing our circular buffer's position until we land on the right interval
		}
	}
	s.bucketStartTime = s.bucketStartTime.Add(time.Duration(intervalsElapsed) * s.bucketSize)
}

// incrementTaskCount adds/removes tasks from the current time that falls in the appropriate interval
func (s *taskTracker) incrementTaskCount(priority int32) {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetTracker(elapsed)

	cb, ok := s.tasksInIntervals[priority]
	if !ok {
		cb = newCircularTaskBuffer(s.numberOfBuckets)
		s.tasksInIntervals[priority] = cb
	}
	cb.incrementTaskCount()
}

// rate returns the rate of tasks added/dispatched in a given interval, per subqueue
func (s *taskTracker) rate() map[int32]float32 {
	s.lock.Lock()
	defer s.lock.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time.
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndResetTracker(elapsed)

	result := make(map[int32]float32)
	elapsedTime := min(currentTime.Sub(s.bucketStartTime)+s.totalIntervalSize, currentTime.Sub(s.startTime))
	if elapsedTime <= 0 {
		return result
	}

	for priority, cb := range s.tasksInIntervals {
		result[priority] = float32(cb.totalTasks()) / float32(elapsedTime.Seconds())
	}
	return result
}
