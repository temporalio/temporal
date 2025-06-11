package clock

import (
	"sync"
	"time"

	"go.temporal.io/server/common/util"
)

type (
	// EventTimeSource is a fake TimeSource. Unlike other fake clock implementations, the methods are synchronous, so
	// when you call Advance or Update, all triggered timers from AfterFunc will fire before the method returns, in the
	// same goroutine.
	EventTimeSource struct {
		mu     sync.RWMutex
		now    time.Time
		timers []*fakeTimer
		async  bool
	}

	// fakeTimer is a fake implementation of [Timer].
	fakeTimer struct {
		// need a link to the parent timeSource for synchronization
		timeSource *EventTimeSource
		// deadline for when the timer should fire
		deadline time.Time
		// callback to call when the timer fires
		callback func()
		// done is true if the timer has fired or been stopped
		done bool
		// index of the timer in the parent timeSource
		index int
		// channel on which the current time is sent when a timer fires
		c chan time.Time
	}
)

var _ TimeSource = (*EventTimeSource)(nil)

// NewEventTimeSource returns a EventTimeSource with the current time set to Unix zero: 1970-01-01 00:00:00 +0000 UTC.
func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{
		now: time.Unix(0, 0),
	}
}

// Some clients depend on the fact that the runtime's timers do _not_ run synchronously.
// If UseAsyncTimers(true) is called, then EventTimeSource will behave that way also.
func (ts *EventTimeSource) UseAsyncTimers(async bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.async = async
}

// Now return the current time.
func (ts *EventTimeSource) Now() time.Time {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.now
}

func (ts *EventTimeSource) Since(t time.Time) time.Duration {
	return ts.Now().Sub(t)
}

// AfterFunc return a timer that will fire after the specified duration. It is important to note that the timeSource is
// locked while the callback is called. This means that you must be cautious about calling any other mutating methods on
// the timeSource from within the callback. Doing so will probably result in a deadlock. To avoid this, you may want to
// wrap all such calls in a goroutine. If the duration is non-positive, the callback will fire immediately before
// AfterFunc returns.
func (ts *EventTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	if d < 0 {
		d = 0
	}
	timer := &fakeTimer{timeSource: ts, deadline: ts.Now().Add(d), callback: f}
	ts.addTimer(timer)
	return timer
}

// NewTimer creates a Timer that will send the current time on a channel after at least
// duration d. It returns the channel and the Timer.
func (ts *EventTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	c := make(chan time.Time, 1)
	// we can't call ts.Now() from the callback so just calculate what it should be
	target := ts.Now().Add(d)
	timer := &fakeTimer{
		timeSource: ts,
		deadline:   target,
		callback:   func() { c <- target },
		c:          c,
	}
	ts.addTimer(timer)
	return c, timer
}

func (ts *EventTimeSource) addTimer(t *fakeTimer) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	t.index = len(ts.timers)
	ts.timers = append(ts.timers, t)
	ts.fireTimers()
}

// Update the fake current time. It returns the timeSource so that you can chain calls like this:
// timeSource := NewEventTimeSource().Update(time.Now())
func (ts *EventTimeSource) Update(now time.Time) *EventTimeSource {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.now = now
	ts.fireTimers()
	return ts
}

// Advance the timer by the specified duration.
func (ts *EventTimeSource) Advance(d time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.now = ts.now.Add(d)
	ts.fireTimers()
}

// AdvanceNext advances to the next timer.
func (ts *EventTimeSource) AdvanceNext() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if len(ts.timers) == 0 {
		return
	}
	// just do linear search, this is efficient enough for now
	tmin := ts.timers[0].deadline
	for _, t := range ts.timers[1:] {
		tmin = util.MinTime(tmin, t.deadline)
	}
	ts.now = tmin
	ts.fireTimers()
}

// NumTimers returns the number of outstanding timers.
func (ts *EventTimeSource) NumTimers() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	return len(ts.timers)
}

// Sleep is a convenience function for waiting on a new timer.
func (ts *EventTimeSource) Sleep(d time.Duration) {
	t, _ := ts.NewTimer(d)
	<-t
}

// fireTimers fires all timers that are ready.
func (ts *EventTimeSource) fireTimers() {
	n := 0
	for _, t := range ts.timers {
		if t.deadline.After(ts.now) {
			ts.timers[n] = t
			t.index = n
			n++
		} else {
			if ts.async {
				go t.callback()
			} else {
				t.callback()
			}
			t.done = true
		}
	}
	ts.timers = ts.timers[:n]
}

// Reset the timer to fire after the specified duration. Returns true if the timer was active.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.timeSource.mu.Lock()
	defer t.timeSource.mu.Unlock()

	if d < 0 {
		d = 0
	}

	wasActive := !t.done
	t.deadline = t.timeSource.now.Add(d)
	if t.done {
		t.done = false
		t.index = len(t.timeSource.timers)
		t.timeSource.timers = append(t.timeSource.timers, t)
		// Only reset the callback if this timer was created via NewTimer
		if t.c != nil {
			t.callback = func() { t.c <- t.deadline }
		}
	}
	t.timeSource.fireTimers()
	return wasActive
}

// Stop the timer. Returns true if the timer was active.
func (t *fakeTimer) Stop() bool {
	t.timeSource.mu.Lock()
	defer t.timeSource.mu.Unlock()

	if t.done {
		return false
	}

	i := t.index
	timers := t.timeSource.timers

	timers[i] = timers[len(timers)-1] // swap with last timer
	timers[i].index = i               // update index of swapped timer
	timers = timers[:len(timers)-1]   // shrink list

	t.timeSource.timers = timers
	t.done = true // ensure that the timer is not reused

	return true
}
