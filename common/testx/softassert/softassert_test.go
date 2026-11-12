package softassert_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/softassert"
	"go.temporal.io/testx/testlogger"
	testzap "go.temporal.io/testx/testlogger/zap"
	"go.uber.org/zap"
)

type recordingLogger struct {
	message string
	fields  []string
	calls   int
}

func (l *recordingLogger) Error(message string, fields ...string) {
	l.message = message
	l.fields = fields
	l.calls++
}

func TestThat(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		condition   bool
		wantCalls   int
		wantMessage string
		wantFields  []string
	}{
		{
			name:      "pass",
			condition: true,
		},
		{
			name:        "fail",
			condition:   false,
			wantCalls:   1,
			wantMessage: "failed assertion: invariant",
			wantFields:  []string{"detail"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := &recordingLogger{}
			require.Equal(t, tc.condition, softassert.That(logger, tc.condition, "invariant", "detail"))
			require.Equal(t, tc.wantCalls, logger.calls)
			require.Equal(t, tc.wantMessage, logger.message)
			require.Equal(t, tc.wantFields, logger.fields)
		})
	}
}

func TestFail(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	softassert.Fail(logger, "unreachable", "detail")
	require.Equal(t, 1, logger.calls)
	require.Equal(t, "failed assertion: unreachable", logger.message)
	require.Equal(t, []string{"detail"}, logger.fields)
}

type failureT struct {
	*testing.T
	failure string
}

func (t *failureT) Fatalf(format string, args ...any) { t.failure = fmt.Sprintf(format, args...) }

func TestFailedAssertionFailsTestLogger(t *testing.T) {
	t.Parallel()
	tb := &failureT{T: t}
	logger := testzap.Wrap(tb, testlogger.FailOnAnyUnexpectedError, zap.NewNop())
	require.False(t, softassert.That(logger, false, "broken invariant", zap.String("detail", "value")))
	failure := logger.Failure()
	require.NotNil(t, failure)
	require.Equal(t, "failed assertion: broken invariant", failure.Msg)
	require.Equal(t, zap.String("detail", "value"), failure.Tags[0].Field)
	require.Contains(t, tb.failure, "failed assertion: broken invariant")
}
