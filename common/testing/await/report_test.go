package await

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReportTimeout(t *testing.T) {
	t.Run("without message", func(t *testing.T) {
		tb := newReportRecordingTB()

		tb.run(func() {
			timeoutReport{
				attempts: 12,
				derivation: timeoutDerivation{
					cfgTotalTimeout:  30 * time.Second,
					effectiveTimeout: 30 * time.Second,
					attemptTimeout:   10 * time.Second,
					minPollInterval:  500 * time.Millisecond,
					maxPollInterval:  2 * time.Second,
				},
			}.reportTimeout(tb, "Require", "")
		})

		require.Equal(t, strings.Join([]string{
			"Require: condition not satisfied after 30s",
			"details:",
			"  cfg.totalTimeout      = 30s",
			"  parent ctx deadline   = (none)",
			"  effective deadline    = 30s from start",
			"  attempts              = 12",
			"  per-attempt timeout   = 10s",
			"  attempt timeouts      = 0",
			"  poll interval         = 500ms -> 2s (exp backoff)",
		}, "\n"), tb.fatals())
	})

	t.Run("with message and cap hit", func(t *testing.T) {
		tb := newReportRecordingTB()

		tb.run(func() {
			timeoutReport{
				attempts: 4,
				derivation: timeoutDerivation{
					cfgTotalTimeout:  90 * time.Second,
					effectiveTimeout: 45 * time.Second,
					attemptTimeout:   10 * time.Second,
					minPollInterval:  time.Second,
					maxPollInterval:  time.Second,
					hasTestCap:       true,
					testCapHit:       true,
					testCapRemaining: 45 * time.Second,
				},
			}.reportTimeout(tb, "Require", "workflow wf-123 not ready")
		})

		require.Equal(t, strings.Join([]string{
			"Require: workflow wf-123 not ready (not satisfied after 45s)",
			"details:",
			"  cfg.totalTimeout      = 1m30s",
			"  parent ctx deadline   = (none)",
			"  test 2min cap         = +45s remaining (HIT)",
			"  effective deadline    = 45s from start",
			"  attempts              = 4",
			"  per-attempt timeout   = 10s",
			"  attempt timeouts      = 0",
			"  poll interval         = 1s (fixed)",
		}, "\n"), tb.fatals())
	})

	t.Run("reports attempt errors", func(t *testing.T) {
		tb := newReportRecordingTB()

		tb.run(func() {
			timeoutReport{
				attempts: 2,
				failures: []attemptFailure{
					{attempt: 1, errors: []string{"first line\nsecond line"}},
					{attempt: 2, errors: []string{"last attempt"}},
				},
				derivation: timeoutDerivation{
					cfgTotalTimeout:  time.Second,
					effectiveTimeout: time.Second,
					attemptTimeout:   10 * time.Millisecond,
					attemptTimeouts:  2,
					minPollInterval:  time.Millisecond,
					maxPollInterval:  time.Millisecond,
				},
			}.reportTimeout(tb, "Require", "")
		})

		require.Equal(t, "attempt errors:\n  attempt 1:\n    first line\n    second line\n  attempt 2:\n    last attempt", tb.errors())
		require.Equal(t, strings.Join([]string{
			"Require: condition not satisfied after 1s",
			"details:",
			"  cfg.totalTimeout      = 1s",
			"  parent ctx deadline   = (none)",
			"  effective deadline    = 1s from start",
			"  attempts              = 2",
			"  per-attempt timeout   = 10ms",
			"  attempt timeouts      = 2",
			"  poll interval         = 1ms (fixed)",
		}, "\n"), tb.fatals())
	})
}

type reportRecordingTB struct {
	testing.TB
	mu            sync.Mutex
	failed        atomic.Bool
	errorMessages []string
	fatalMessages []string
}

func newReportRecordingTB() *reportRecordingTB {
	return &reportRecordingTB{}
}

func (r *reportRecordingTB) Helper()      {}
func (r *reportRecordingTB) Failed() bool { return r.failed.Load() }
func (r *reportRecordingTB) Context() context.Context {
	return context.Background()
}

func (r *reportRecordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed.Store(true)
	r.errorMessages = append(r.errorMessages, fmt.Sprintf(format, args...))
}

func (r *reportRecordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed.Store(true)
	r.fatalMessages = append(r.fatalMessages, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *reportRecordingTB) run(fn func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
}

func (r *reportRecordingTB) fatals() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.fatalMessages, "\n")
}

func (r *reportRecordingTB) errors() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.errorMessages, "\n")
}
