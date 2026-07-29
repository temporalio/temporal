package await

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReportTimeout(t *testing.T) {
	t.Run("without message", func(t *testing.T) {
		tb := newReportRecordingTB()

		timeoutReport{
			effectiveTimeout: time.Second,
			attempts:         3,
			attemptTimeouts:  2,
		}.reportTimeout(tb, "Require", "")

		require.Equal(t, strings.Join([]string{
			"Require: condition not satisfied after 1s",
			"details:",
			"  attempts         = 3",
			"  attempt timeouts = 2",
		}, "\n"), tb.fatals())
	})

	t.Run("with message", func(t *testing.T) {
		tb := newReportRecordingTB()

		timeoutReport{
			effectiveTimeout: 2 * time.Second,
			attempts:         4,
			attemptTimeouts:  1,
		}.reportTimeout(tb, "Require", "workflow wf-123 not ready")

		require.Equal(t, strings.Join([]string{
			"Require: workflow wf-123 not ready (not satisfied after 2s)",
			"details:",
			"  attempts         = 4",
			"  attempt timeouts = 1",
		}, "\n"), tb.fatals())
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
