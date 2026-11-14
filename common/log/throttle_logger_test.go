package log

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
)

// A rate this low allows a burst of exactly one log line, and the next token
// arrives only after ~1000s, so throttling is deterministic within a test.
const testThrottleRPS = 0.001

type recordingLogger struct {
	*noopLogger

	mu   sync.Mutex
	msgs []string
}

func (r *recordingLogger) record(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, msg)
}

func (r *recordingLogger) Info(msg string, _ ...tag.Tag)  { r.record(msg) }
func (r *recordingLogger) Error(msg string, _ ...tag.Tag) { r.record(msg) }

func (r *recordingLogger) With(...tag.Tag) Logger { return r }

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{noopLogger: NewNoopLogger()}
}

func TestThrottledLoggerThrottlesEachMessageIndependently(t *testing.T) {
	recorder := newRecordingLogger()
	logger := NewThrottledLogger(recorder, func() float64 { return testThrottleRPS })

	for range 100 {
		logger.Info("frequent message")
	}
	logger.Error("rare error")
	logger.Error("rare error")

	require.Equal(t, []string{"frequent message", "rare error"}, recorder.msgs)
}

func TestThrottledLoggerWithSharesLimiters(t *testing.T) {
	recorder := newRecordingLogger()
	logger := NewThrottledLogger(recorder, func() float64 { return testThrottleRPS })
	withLogger := logger.With(tag.WorkflowID("wid"))

	logger.Info("message")
	withLogger.Info("message")
	withLogger.Info("other message")

	require.Equal(t, []string{"message", "other message"}, recorder.msgs)
}

func TestThrottledLoggerOverflowMessagesShareLimiter(t *testing.T) {
	recorder := newRecordingLogger()
	logger := newThrottledLogger(recorder, func() float64 { return testThrottleRPS }, 1)

	logger.Info("tracked")
	logger.Info("overflow 1")
	logger.Info("overflow 2")
	logger.Info("tracked")

	require.Equal(t, []string{"tracked", "overflow 1"}, recorder.msgs)
}
