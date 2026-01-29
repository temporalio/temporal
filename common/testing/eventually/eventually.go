// Package eventually provides polling utilities for tests.
//
// Unlike testify's Eventually which waits for the full timeout on failure,
// this package immediately retries when assertions fail, making tests faster
// while still allowing proper timeout behavior.
//
// Background: testify's Eventually uses goroutines but waits for the full timeout
// even when require.* fails (because runtime.Goexit() terminates the goroutine
// without signaling failure). This implementation detects Goexit and retries
// immediately. See https://github.com/stretchr/testify/issues/1810
package eventually

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

var (
	// DefaultTimeout is the default maximum time to wait for condition to succeed.
	// Doubled on CI for slower environments.
	DefaultTimeout = 5 * time.Second

	// DefaultPollInterval is the default interval between condition checks.
	// Doubled on CI for slower environments.
	DefaultPollInterval = 200 * time.Millisecond
)

func init() {
	if os.Getenv("CI") != "" {
		DefaultTimeout *= 2
		DefaultPollInterval *= 2
	}
}

// T wraps testing.TB for use in eventually conditions.
// It captures the last error message for reporting on timeout.
type T struct {
	testing.TB
	lastErr string
}

// Errorf records an error message.
func (t *T) Errorf(format string, args ...any) {
	t.lastErr = fmt.Sprintf(format, args...)
}

// FailNow is called by require.* on failure. It triggers runtime.Goexit()
// which terminates the goroutine and is detected by Require to retry.
// Unlike testing.TB.FailNow(), this does NOT mark the test as failed.
func (t *T) FailNow() {
	runtime.Goexit()
}

// Require runs condition repeatedly until it completes without assertion failures,
// or until the timeout expires. If timeout expires, the last error is reported
// and the test fails.
//
// The condition receives a *T that wraps the test's testing.TB. Pass this *T
// to require.* functions. When assertions fail, Require detects the failure
// and retries until timeout.
//
// Example:
//
//	eventually.Require(t, func(t *eventually.T) {
//	    resp, err := client.GetStatus(ctx)
//	    require.NoError(t, err)
//	    require.Equal(t, "ready", resp.Status)
//	}, 5*time.Second, 200*time.Millisecond)
func Require(tb testing.TB, condition func(*T), timeout, pollInterval time.Duration) {
	tb.Helper()

	deadline := time.Now().Add(timeout)
	polls := 0

	for {
		polls++
		t := &T{TB: tb}

		// Run condition in goroutine to detect runtime.Goexit() from FailNow()
		done := make(chan bool, 1)
		go func() {
			defer func() {
				// If we reach here via Goexit (from FailNow), send false
				// If condition completed normally, true was already sent
				select {
				case done <- false:
				default:
				}
			}()
			condition(t)
			done <- true // success - condition completed without FailNow
		}()

		success := <-done
		if success {
			return // condition passed
		}

		// Check timeout before sleeping
		if time.Now().After(deadline) {
			if t.lastErr != "" {
				tb.Errorf("%s", t.lastErr)
			}
			tb.Fatalf("eventually.Require: condition not satisfied after %v (%d polls)", timeout, polls)
			return
		}

		// Wait before next attempt, but respect deadline
		remaining := time.Until(deadline)
		if remaining < pollInterval {
			time.Sleep(remaining)
		} else {
			time.Sleep(pollInterval)
		}
	}
}
