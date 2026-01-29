package eventually

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequire_ImmediateSuccess(t *testing.T) {
	called := 0
	Require(t, func(t *T) {
		called++
		require.True(t, true)
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, 1, called, "condition should be called exactly once")
}

func TestRequire_EventualSuccess(t *testing.T) {
	var counter atomic.Int32
	go func() {
		time.Sleep(50 * time.Millisecond)
		counter.Store(42)
	}()

	Require(t, func(t *T) {
		require.Equal(t, int32(42), counter.Load())
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_MultipleAssertions(t *testing.T) {
	type state struct {
		count  atomic.Int32
		ready  atomic.Bool
		status atomic.Value
	}

	s := &state{}
	s.status.Store("initializing")

	go func() {
		time.Sleep(20 * time.Millisecond)
		s.count.Store(5)
		time.Sleep(20 * time.Millisecond)
		s.status.Store("ready")
		s.ready.Store(true)
	}()

	Require(t, func(t *T) {
		require.True(t, s.ready.Load(), "should be ready")
		require.Equal(t, int32(5), s.count.Load(), "count should be 5")
		require.Equal(t, "ready", s.status.Load().(string), "status should be ready")
	}, time.Second, 10*time.Millisecond)
}

func TestRequire_RetriesUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(100 * time.Millisecond)
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		attempts.Add(1)
		require.True(t, ready.Load())
	}, time.Second, 10*time.Millisecond)

	assert.True(t, attempts.Load() > 1, "should have retried multiple times, got %d", attempts.Load())
}

func TestRequire_FailNowStopsIteration(t *testing.T) {
	// When require.* fails, it calls FailNow which should stop the current
	// iteration but allow retry
	var attempts atomic.Int32
	var ready atomic.Bool

	go func() {
		time.Sleep(50 * time.Millisecond)
		ready.Store(true)
	}()

	Require(t, func(t *T) {
		attempts.Add(1)
		if !ready.Load() {
			require.Fail(t, "not ready yet")
			// This line should not execute after FailNow in require.Fail
			t.Errorf("should not reach here")
		}
	}, time.Second, 10*time.Millisecond)

	assert.True(t, ready.Load())
	assert.True(t, attempts.Load() > 1)
}

func TestT_Errorf(t *testing.T) {
	et := &T{TB: t}
	et.Errorf("error: %s", "details")
	assert.Equal(t, "error: details", et.lastErr)
}

func TestT_Helper(t *testing.T) {
	et := &T{TB: t}
	// Just verify it doesn't panic
	et.Helper()
}

func TestDefaults(t *testing.T) {
	// Note: defaults are doubled on CI
	if os.Getenv("CI") != "" {
		assert.Equal(t, 10*time.Second, DefaultTimeout)
		assert.Equal(t, 400*time.Millisecond, DefaultPollInterval)
	} else {
		assert.Equal(t, 5*time.Second, DefaultTimeout)
		assert.Equal(t, 200*time.Millisecond, DefaultPollInterval)
	}
}
