package clock_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestTimeSkippingTimeSource_DefaultsToRealTimeSource(t *testing.T) {
	t.Parallel()

	ts := clock.NewTimeSkippingTimeSource(nil)
	now := ts.Now()

	// Should be close to real time
	assert.WithinDuration(t, time.Now().UTC(), now, 100*time.Millisecond)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	skippedTime := []time.Duration{
		5 * time.Second,
		3 * time.Second,
		2 * time.Second,
	}

	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)

	// Should start with all skipped time applied
	now := ts.Now()
	expected := time.Unix(10, 0) // 5 + 3 + 2 = 10 seconds
	assert.Equal(t, expected, now)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_DefaultsToRealTimeSource(t *testing.T) {
	t.Parallel()

	skippedTime := []time.Duration{1 * time.Hour}
	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(nil, skippedTime)

	now := ts.Now()
	expected := time.Now().UTC().Add(1 * time.Hour)

	// Should be real time + 1 hour
	assert.WithinDuration(t, expected, now, 100*time.Millisecond)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_EmptySlice(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, []time.Duration{})

	// Should start at Unix epoch with no skipped time
	now := ts.Now()
	assert.Equal(t, time.Unix(0, 0), now)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_NilSlice(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, nil)

	// Should start at Unix epoch with no skipped time
	now := ts.Now()
	assert.Equal(t, time.Unix(0, 0), now)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_IndependentSlice(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	skippedTime := []time.Duration{5 * time.Second}

	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)

	// Modify the original slice
	skippedTime[0] = 100 * time.Second
	skippedTime = append(skippedTime, 50*time.Second)

	// TimeSource should be unaffected
	now := ts.Now()
	assert.Equal(t, time.Unix(5, 0), now)
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_CanAddMore(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	skippedTime := []time.Duration{5 * time.Second}

	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)
	assert.Equal(t, time.Unix(5, 0), ts.Now())

	// Can still add more skipped time
	ts.AddSkippedTime(3 * time.Second)
	assert.Equal(t, time.Unix(8, 0), ts.Now())
}

func TestNewTimeSkippingTimeSourceWithSkippedTime_WithNegativeSkips(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	skippedTime := []time.Duration{
		10 * time.Second,
		-3 * time.Second,
		5 * time.Second,
	}

	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)

	// Should sum to 10 - 3 + 5 = 12 seconds
	now := ts.Now()
	assert.Equal(t, time.Unix(12, 0), now)
}

func TestTimeSkippingTimeSource_Now_WithoutSkips(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	baseNow := baseSource.Now()
	skippingNow := ts.Now()

	// Without skips, should be essentially the same
	assert.WithinDuration(t, baseNow, skippingNow, 1*time.Millisecond)
}

func TestTimeSkippingTimeSource_Now_WithSingleSkip(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	beforeSkip := ts.Now()
	skipAmount := 10 * time.Second

	ts.AddSkippedTime(skipAmount)

	afterSkip := ts.Now()

	// The difference should be approximately the skip amount
	diff := afterSkip.Sub(beforeSkip)
	assert.Greater(t, diff, skipAmount-time.Millisecond)
	assert.Less(t, diff, skipAmount+time.Millisecond)
}

func TestTimeSkippingTimeSource_Now_WithMultipleSkips(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	start := ts.Now()

	ts.AddSkippedTime(5 * time.Second)
	ts.AddSkippedTime(3 * time.Second)
	ts.AddSkippedTime(2 * time.Second)

	end := ts.Now()

	// Total skip should be 10 seconds
	diff := end.Sub(start)
	assert.Greater(t, diff, 10*time.Second-time.Millisecond)
	assert.Less(t, diff, 10*time.Second+time.Millisecond)
}

func TestTimeSkippingTimeSource_Since(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	start := ts.Now()
	ts.AddSkippedTime(7 * time.Second)

	elapsed := ts.Since(start)

	// Should be approximately 7 seconds
	assert.Greater(t, elapsed, 7*time.Second-time.Millisecond)
	assert.Less(t, elapsed, 7*time.Second+time.Millisecond)
}

func TestTimeSkippingTimeSource_AfterFunc(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	fired := make(chan bool, 1)
	ts.AfterFunc(50*time.Millisecond, func() {
		fired <- true
	})

	// Should fire after real time, not adjusted time
	select {
	case <-fired:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timer did not fire in time")
	}
}

func TestTimeSkippingTimeSource_AfterFunc_NotAffectedBySkips(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	// Add skipped time before creating timer
	ts.AddSkippedTime(1 * time.Hour)

	start := time.Now()
	fired := make(chan bool, 1)
	ts.AfterFunc(50*time.Millisecond, func() {
		fired <- true
	})

	select {
	case <-fired:
		elapsed := time.Since(start)
		// Should fire after ~50ms of real time
		assert.Less(t, elapsed, 200*time.Millisecond)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timer did not fire in time")
	}
}

func TestTimeSkippingTimeSource_AfterFunc_FiresEarlyOnSkip(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	fired := make(chan bool, 1)
	ts.AfterFunc(10*time.Second, func() {
		fired <- true
	})

	// Timer set for 10 seconds, but we skip 15 seconds
	ts.AddSkippedTime(15 * time.Second)

	// Should fire immediately due to skip
	select {
	case <-fired:
		// Success - timer fired due to skip
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timer did not fire after skipping past deadline")
	}
}

func TestTimeSkippingTimeSource_MultipleTimers_FireOnSkip(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	fired1 := make(chan bool, 1)
	fired2 := make(chan bool, 1)
	fired3 := make(chan bool, 1)

	ts.AfterFunc(5*time.Second, func() { fired1 <- true })
	ts.AfterFunc(10*time.Second, func() { fired2 <- true })
	ts.AfterFunc(15*time.Second, func() { fired3 <- true })

	// Skip 12 seconds - should fire first two timers
	ts.AddSkippedTime(12 * time.Second)

	// First two should fire
	select {
	case <-fired1:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timer 1 did not fire")
	}

	select {
	case <-fired2:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timer 2 did not fire")
	}

	// Third should not have fired yet
	select {
	case <-fired3:
		t.Fatal("Timer 3 fired too early")
	case <-time.After(10 * time.Millisecond):
		// Expected - timer 3 shouldn't fire yet
	}

	// Skip more time to fire the third timer
	ts.AddSkippedTime(5 * time.Second)

	select {
	case <-fired3:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timer 3 did not fire after second skip")
	}
}

func TestTimeSkippingTimeSource_SkipDoesNotAffectStoppedTimers(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	fired := make(chan bool, 1)
	timer := ts.AfterFunc(10*time.Second, func() {
		fired <- true
	})

	// Stop the timer
	stopped := timer.Stop()
	assert.True(t, stopped)

	// Skip past the deadline
	ts.AddSkippedTime(15 * time.Second)

	// Timer should not fire
	select {
	case <-fired:
		t.Fatal("Stopped timer should not fire")
	case <-time.After(50 * time.Millisecond):
		// Expected - stopped timer doesn't fire
	}
}

func TestTimeSkippingTimeSource_NewTimer(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	ch, timer := ts.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	select {
	case fireTime := <-ch:
		now := ts.Now()
		// Fire time should include any skipped time
		assert.WithinDuration(t, now, fireTime, 10*time.Millisecond)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timer did not fire in time")
	}
}

func TestTimeSkippingTimeSource_NewTimer_WithSkippedTime(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	// Skip 1 hour
	ts.AddSkippedTime(1 * time.Hour)

	beforeTimer := ts.Now()
	ch, timer := ts.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	select {
	case fireTime := <-ch:
		// The time sent on channel should be adjusted (include skipped time)
		// It should be approximately beforeTimer + 50ms
		diff := fireTime.Sub(beforeTimer)
		assert.Greater(t, diff, 40*time.Millisecond)
		assert.Less(t, diff, 100*time.Millisecond)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timer did not fire in time")
	}
}

func TestTimeSkippingTimeSource_TimerStop(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	ch, timer := ts.NewTimer(100 * time.Millisecond)

	// Stop the timer immediately
	stopped := timer.Stop()
	assert.True(t, stopped, "Should return true when stopping an active timer")

	// Wait a bit to ensure it doesn't fire
	time.Sleep(150 * time.Millisecond)

	select {
	case <-ch:
		t.Fatal("Timer fired after being stopped")
	default:
		// Success - timer did not fire
	}
}

func TestTimeSkippingTimeSource_TimerReset(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewRealTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	ch, timer := ts.NewTimer(200 * time.Millisecond)

	// Reset to a shorter duration
	time.Sleep(50 * time.Millisecond)
	wasActive := timer.Reset(50 * time.Millisecond)
	assert.True(t, wasActive, "Should return true when resetting an active timer")

	// Should fire after the reset duration (~50ms from reset, not original 200ms)
	select {
	case <-ch:
		// Success
	case <-time.After(150 * time.Millisecond):
		t.Fatal("Timer did not fire after reset")
	}
}

func TestTimeSkippingTimeSource_WithEventTimeSource(t *testing.T) {
	t.Parallel()

	// Use EventTimeSource as the base
	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	start := ts.Now()
	assert.Equal(t, time.Unix(0, 0), start, "Should start at Unix epoch")

	// Add skipped time
	ts.AddSkippedTime(5 * time.Second)
	now := ts.Now()
	assert.Equal(t, time.Unix(5, 0), now)

	// Advance base time
	baseSource.Advance(3 * time.Second)
	now = ts.Now()
	assert.Equal(t, time.Unix(8, 0), now, "Should be base time (3s) + skipped time (5s)")

	// Add more skipped time
	ts.AddSkippedTime(2 * time.Second)
	now = ts.Now()
	assert.Equal(t, time.Unix(10, 0), now, "Should be base time (3s) + total skipped time (7s)")
}

func TestTimeSkippingTimeSource_ConcurrentSkips(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	done := make(chan bool)
	numGoroutines := 10
	skipsPerGoroutine := 100

	// Concurrently add skipped time
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < skipsPerGoroutine; j++ {
				ts.AddSkippedTime(1 * time.Millisecond)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Total skipped time should be numGoroutines * skipsPerGoroutine milliseconds
	expectedSkip := time.Duration(numGoroutines*skipsPerGoroutine) * time.Millisecond
	elapsed := ts.Since(time.Unix(0, 0))
	assert.Equal(t, expectedSkip, elapsed)
}

func TestTimeSkippingTimeSource_NegativeSkip(t *testing.T) {
	t.Parallel()

	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	start := ts.Now()

	// Add positive skip
	ts.AddSkippedTime(10 * time.Second)
	assert.Equal(t, time.Unix(10, 0), ts.Now())

	// Add negative skip (going backwards)
	ts.AddSkippedTime(-5 * time.Second)
	assert.Equal(t, time.Unix(5, 0), ts.Now())

	// Verify it's start + net skip
	elapsed := ts.Since(start)
	assert.Equal(t, 5*time.Second, elapsed)
}

func ExampleTimeSkippingTimeSource() {
	// Create a time source that can skip time
	ts := clock.NewTimeSkippingTimeSource(nil)

	before := ts.Now()

	// Skip 1 hour into the future
	ts.AddSkippedTime(1 * time.Hour)

	after := ts.Now()

	// The difference is approximately 1 hour
	diff := after.Sub(before)
	fmt.Println(diff >= 1*time.Hour)

	// Output:
	// true
}

func ExampleTimeSkippingTimeSource_withEventTimeSource() {
	// Use EventTimeSource as base for fully controlled time
	baseSource := clock.NewEventTimeSource()
	ts := clock.NewTimeSkippingTimeSource(baseSource)

	fmt.Println("Start:", ts.Now().Unix()) // 0

	// Skip 10 seconds
	ts.AddSkippedTime(10 * time.Second)
	fmt.Println("After skip:", ts.Now().Unix()) // 10

	// Advance base time by 5 seconds
	baseSource.Advance(5 * time.Second)
	fmt.Println("After advance:", ts.Now().Unix()) // 15

	// Output:
	// Start: 0
	// After skip: 10
	// After advance: 15
}

func ExampleNewTimeSkippingTimeSourceWithSkippedTime() {
	// Create a time source with pre-existing skipped time
	baseSource := clock.NewEventTimeSource()
	skippedTime := []time.Duration{
		5 * time.Second,
		3 * time.Second,
		2 * time.Second,
	}

	ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)

	// Time already includes all the skips
	fmt.Println("Initial time:", ts.Now().Unix()) // 10 (5+3+2)

	// Can still add more skipped time
	ts.AddSkippedTime(7 * time.Second)
	fmt.Println("After adding more:", ts.Now().Unix()) // 17

	// Advance base time
	baseSource.Advance(3 * time.Second)
	fmt.Println("After base advance:", ts.Now().Unix()) // 20

	// Output:
	// Initial time: 10
	// After adding more: 17
	// After base advance: 20
}
