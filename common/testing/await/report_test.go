package await

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testcontext"
)

func TestReportTimeout(t *testing.T) {
	t.Run("without message", func(t *testing.T) {
		tb := newReportRecordingTB()

		timeoutReport{
			effectiveTimeout:   time.Second,
			attempts:           3,
			attemptDurationSum: 60 * time.Millisecond,
			attemptDurationMax: 30 * time.Millisecond,
		}.reportTimeout(tb, "Require", "")

		require.Equal(t, strings.Join([]string{
			"Require: condition not satisfied after 1s",
			"details:",
			"  attempts         = 3",
			"  attempt duration = avg 20ms, max 30ms",
		}, "\n"), tb.fatals())
	})

	t.Run("with message", func(t *testing.T) {
		tb := newReportRecordingTB()

		timeoutReport{
			effectiveTimeout:  time.Second,
			configuredTimeout: 2 * time.Second,
			attemptTimeout:    50 * time.Millisecond,
			deadlineCause:     "parent context deadline",
			attempts:          4,
			attemptTimeouts:   1,
		}.reportTimeout(tb, "Require", "workflow wf-123 not ready")

		require.Equal(t, strings.Join([]string{
			"Require: workflow wf-123 not ready (not satisfied after 1s)",
			"details:",
			"  await timeout    = 1s (configured 2s; limited by parent context deadline)",
			"  attempts         = 4",
			"  attempt timeouts = 1 (attempt timeout 50ms)",
			"  attempt duration = avg 0s, max 0s",
		}, "\n"), tb.fatals())
	})

	t.Run("with test context extension audit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := testcontext.For(t)
			testcontext.EnsureRemaining(ctx, t, testcontext.DefaultTimeout()+10*time.Second)
			tb := newReportRecordingTB()

			timeoutReport{
				effectiveTimeout: time.Second,
				testContext:      ctx,
			}.reportTimeout(tb, "Require", "")

			require.Equal(t, strings.Join([]string{
				"Require: condition not satisfied after 1s",
				"details:",
				"  attempts         = 0",
				"  ctx extensions   = 1 (+10s total)",
				"    1. +10s after 0s",
			}, "\n"), tb.fatals())
		})
	})
}

type reportRecordingTB struct {
	testing.TB
	mu            sync.Mutex
	fatalMessages []string
}

func newReportRecordingTB() *reportRecordingTB {
	return &reportRecordingTB{}
}

func (r *reportRecordingTB) Helper() {}

func (r *reportRecordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fatalMessages = append(r.fatalMessages, fmt.Sprintf(format, args...))
}

func (r *reportRecordingTB) fatals() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.fatalMessages, "\n")
}
