package matching

import (
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

// incAgo adds n to the bucket that is `ago` buckets older than the current one.
func (cb *circularTaskBuffer) incAgo(ago int, n int) {
	numBuckets := len(cb.buffer)
	stepsBack := ago % numBuckets // going back a full lap lands on the same bucket
	// adding numBuckets before the modulo keeps the index non-negative when stepping back past 0
	pos := (cb.currentPos - stepsBack + numBuckets) % numBuckets
	cb.buffer[pos] += int32(n)
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

// taskTracker is not safe for concurrent use; callers must provide their own
// synchronization.
type taskTracker struct {
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

// advanceAndReset advances the trackers position and clears out any expired intervals.
func (s *taskTracker) advanceAndReset(elapsed time.Duration) {
	// Calculate the number of intervals elapsed since the start interval time
	intervalsElapsed := int(elapsed / s.bucketSize)

	for range min(intervalsElapsed, s.buckets) {
		s.tasks.advance() // advancing our circular buffer's position until we land on the right interval
	}
	s.bucketStartTime = s.bucketStartTime.Add(time.Duration(intervalsElapsed) * s.bucketSize)
}

// inc increments the count of tasks by n at the current time
func (s *taskTracker) inc(n int) {
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndReset(elapsed)
	s.tasks.inc(n)
}

// incOver adds n tasks spread uniformly over (from, now]. Batched adds represent tasks that
// arrived over that whole interval, so attributing them all to the current bucket would bias
// short windows high.
func (s *taskTracker) incOver(n int, from time.Time) {
	now := s.clock.Now()
	s.advanceAndReset(now.Sub(s.bucketStartTime))

	span := now.Sub(from)
	// With no interval to spread over (first call, or the clock moved backwards) fall back to
	// counting at the current instant, as inc does.
	if span <= 0 || n == 0 || now.Before(s.bucketStartTime) {
		s.tasks.inc(n)
		return
	}
	lower := from
	if s.startTime.After(lower) {
		lower = s.startTime
	}

	start, end := s.bucketStartTime, now
	var coveredTime time.Duration
	assignedSoFar := 0
	for ago := range s.buckets {
		if !end.After(lower) {
			break
		}
		if lower.After(start) {
			start = lower
		}
		coveredTime += end.Sub(start)
		// cumulative rounding keeps per-bucket error <= 1 and the total exact
		assignedThroughThisBucket := int(float64(n)*float64(coveredTime)/float64(span) + 0.5)
		s.tasks.incAgo(ago, assignedThroughThisBucket-assignedSoFar)
		assignedSoFar = assignedThroughThisBucket
		end = start
		start = start.Add(-s.bucketSize)
	}
}

// rate returns the rate of increments in a given interval
func (s *taskTracker) rate() float32 {
	rate, _ := s.rateAndFull()
	return rate
}

// rateAndFull returns the rate of increments in a given interval, plus whether the full
// interval has elapsed.
func (s *taskTracker) rateAndFull() (float32, bool) {
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.bucketStartTime)
	s.advanceAndReset(elapsed)
	totalTasks := s.tasks.totalTasks()

	elapsedTime := min(
		currentTime.Sub(s.bucketStartTime)+s.totalInterval,
		currentTime.Sub(s.startTime))

	if elapsedTime <= 0 {
		return 0, false
	}

	// rate per second
	full := elapsedTime >= s.totalInterval
	return float32(totalTasks) / float32(elapsedTime.Seconds()), full
}
