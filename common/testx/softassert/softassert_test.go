package softassert_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/softassert"
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
