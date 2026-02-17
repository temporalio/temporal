package clock

import (
	"sync"
	"time"
)

type (
	// TimeSkippingTimeSource wraps another TimeSource and adds accumulated skipped time to it.
	// This allows testing scenarios where time can be artificially advanced while still using
	// real timers for AfterFunc and NewTimer operations. Timers are aware of skipped time and
	// will fire early if skipped time causes their deadline to be reached.
	TimeSkippingTimeSource struct {
		baseSource      TimeSource
		mu              sync.RWMutex
		skippedTimeInNs []int64
		timers          []*skippingTimer
		nextTimerID     int64
	}

	// skippingTimer tracks a timer that is aware of time skips
	skippingTimer struct {
		id            int64
		baseTimer     Timer
		deadline      time.Time // in adjusted time
		callback      func()
		source        *TimeSkippingTimeSource
		fired         bool
		c             chan time.Time // for NewTimer, buffered channel
		mu            sync.Mutex
	}
)

var _ TimeSource = (*TimeSkippingTimeSource)(nil)
var _ Timer = (*skippingTimer)(nil)

// NewTimeSkippingTimeSource creates a new TimeSkippingTimeSource.
// If baseSource is nil, it defaults to RealTimeSource.
func NewTimeSkippingTimeSource(baseSource TimeSource) *TimeSkippingTimeSource {
	if baseSource == nil {
		realSource := NewRealTimeSource()
		baseSource = realSource
	}
	return &TimeSkippingTimeSource{
		baseSource:      baseSource,
		skippedTimeInNs: make([]int64, 0),
		timers:          make([]*skippingTimer, 0),
		nextTimerID:     0,
	}
}

// NewTimeSkippingTimeSourceWithSkippedTime creates a new TimeSkippingTimeSource
// with an initial set of skipped time durations.
// If baseSource is nil, it defaults to RealTimeSource.
// The skippedTime slice is copied, so modifications to the original slice won't affect the TimeSource.
func NewTimeSkippingTimeSourceWithSkippedTime(baseSource TimeSource, skippedTime []time.Duration) *TimeSkippingTimeSource {
	if baseSource == nil {
		realSource := NewRealTimeSource()
		baseSource = realSource
	}

	// Convert time.Duration slice to int64 nanoseconds slice
	skippedTimeInNs := make([]int64, len(skippedTime))
	for i, duration := range skippedTime {
		skippedTimeInNs[i] = int64(duration)
	}

	return &TimeSkippingTimeSource{
		baseSource:      baseSource,
		skippedTimeInNs: skippedTimeInNs,
		timers:          make([]*skippingTimer, 0),
		nextTimerID:     0,
	}
}

// Now returns the current time from the base source plus all accumulated skipped time.
func (ts *TimeSkippingTimeSource) Now() time.Time {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	base := ts.baseSource.Now()
	totalSkipped := ts.totalSkippedTimeLocked()
	return base.Add(time.Duration(totalSkipped))
}

// Since returns the time elapsed since t, accounting for skipped time.
func (ts *TimeSkippingTimeSource) Since(t time.Time) time.Duration {
	return ts.Now().Sub(t)
}

// AfterFunc creates a timer that fires after duration d in adjusted time.
// If skipped time causes the deadline to pass, the timer fires immediately.
func (ts *TimeSkippingTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	now := ts.nowLocked()
	deadline := now.Add(d)

	timer := &skippingTimer{
		id:       ts.nextTimerID,
		deadline: deadline,
		callback: f,
		source:   ts,
		fired:    false,
	}
	ts.nextTimerID++

	// Calculate how much real time until deadline, accounting for current skipped time
	realDeadline := deadline.Sub(now)
	if realDeadline < 0 {
		realDeadline = 0
	}

	// Create base timer that fires based on real time
	timer.baseTimer = ts.baseSource.AfterFunc(realDeadline, func() {
		timer.mu.Lock()
		if timer.fired {
			timer.mu.Unlock()
			return
		}
		timer.fired = true
		timer.mu.Unlock()

		ts.removeTimer(timer.id)
		f()
	})

	ts.timers = append(ts.timers, timer)
	return timer
}

// NewTimer creates a timer that fires after duration d in adjusted time.
// When the timer fires, it sends the adjusted current time on the channel.
func (ts *TimeSkippingTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	ch := make(chan time.Time, 1)
	timer := ts.AfterFunc(d, func() {
		ch <- ts.Now()
	}).(*skippingTimer)
	timer.c = ch
	return ch, timer
}

// AddSkippedTime adds a duration to the accumulated skipped time.
// Future calls to Now() will include this additional time.
// Any timers whose deadlines are now in the past will fire immediately.
func (ts *TimeSkippingTimeSource) AddSkippedTime(d time.Duration) {
	timersToFire := ts.addSkippedTimeAndGetTimersToFire(d)

	// Fire callbacks outside the lock to avoid deadlocks
	for _, timer := range timersToFire {
		timer.callback()
	}
}

// addSkippedTimeAndGetTimersToFire adds skipped time and returns timers that should fire.
// This is extracted to ensure the lock is always released via defer.
func (ts *TimeSkippingTimeSource) addSkippedTimeAndGetTimersToFire(d time.Duration) []*skippingTimer {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.skippedTimeInNs = append(ts.skippedTimeInNs, int64(d))
	now := ts.nowLocked()

	// Find timers that should fire due to the skip
	timersToFire := make([]*skippingTimer, 0)
	for _, timer := range ts.timers {
		timer.mu.Lock()
		if !timer.fired && !now.Before(timer.deadline) {
			timer.fired = true
			timer.baseTimer.Stop() // Stop the base timer
			timersToFire = append(timersToFire, timer)
		}
		timer.mu.Unlock()
	}

	// Remove fired timers from the list
	for _, timer := range timersToFire {
		ts.removeTimerLocked(timer.id)
	}

	return timersToFire
}

// totalSkippedTimeLocked returns the sum of all skipped time in nanoseconds.
// Must be called with read or write lock held.
func (ts *TimeSkippingTimeSource) totalSkippedTimeLocked() int64 {
	total := int64(0)
	for _, skip := range ts.skippedTimeInNs {
		total += skip
	}
	return total
}

// nowLocked returns the current adjusted time. Must be called with lock held.
func (ts *TimeSkippingTimeSource) nowLocked() time.Time {
	base := ts.baseSource.Now()
	totalSkipped := ts.totalSkippedTimeLocked()
	return base.Add(time.Duration(totalSkipped))
}

// removeTimer removes a timer from the list by ID. Acquires lock.
func (ts *TimeSkippingTimeSource) removeTimer(id int64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.removeTimerLocked(id)
}

// removeTimerLocked removes a timer from the list by ID. Must be called with lock held.
func (ts *TimeSkippingTimeSource) removeTimerLocked(id int64) {
	for i, timer := range ts.timers {
		if timer.id == id {
			// Swap with last element and truncate
			ts.timers[i] = ts.timers[len(ts.timers)-1]
			ts.timers = ts.timers[:len(ts.timers)-1]
			return
		}
	}
}

// Timer implementation for skippingTimer

// Stop prevents the timer from firing.
func (st *skippingTimer) Stop() bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.fired {
		return false
	}

	st.fired = true
	st.source.removeTimer(st.id)
	return st.baseTimer.Stop()
}

// Reset changes the timer to fire after duration d from now.
func (st *skippingTimer) Reset(d time.Duration) bool {
	st.mu.Lock()
	wasActive := !st.fired

	// Stop the old base timer
	if !st.fired {
		st.baseTimer.Stop()
	}

	st.fired = false
	st.mu.Unlock()

	// Calculate new deadline in adjusted time
	st.source.mu.Lock()
	now := st.source.nowLocked()
	st.deadline = now.Add(d)

	// If this timer was previously fired, add it back to the list
	if !wasActive {
		st.id = st.source.nextTimerID
		st.source.nextTimerID++
		st.source.timers = append(st.source.timers, st)
	}
	st.source.mu.Unlock()

	// Calculate real duration until deadline
	realDeadline := d
	if realDeadline < 0 {
		realDeadline = 0
	}

	// Create new base timer
	st.mu.Lock()
	st.baseTimer = st.source.baseSource.AfterFunc(realDeadline, func() {
		st.mu.Lock()
		if st.fired {
			st.mu.Unlock()
			return
		}
		st.fired = true
		st.mu.Unlock()

		st.source.removeTimer(st.id)
		st.callback()
	})
	st.mu.Unlock()

	return wasActive
}
