package softassert_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/softassert"
)

type detail string

func (detail) Key() string {
	return "detail"
}

func (d detail) Value() any {
	return string(d)
}

type recordingLogger struct {
	message string
	fields  []softassert.Field
	calls   int
}

func (l *recordingLogger) Error(message string, fields ...softassert.Field) {
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
		wantFields  []softassert.Field
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
			wantFields:  []softassert.Field{softassert.FailedAssertion, detail("value")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := &recordingLogger{}
			require.Equal(t, tc.condition, softassert.That(logger, tc.condition, "invariant", detail("value")))
			require.Equal(t, tc.wantCalls, logger.calls)
			require.Equal(t, tc.wantMessage, logger.message)
			require.Equal(t, tc.wantFields, logger.fields)
		})
	}
}

func TestFail(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	softassert.Fail(logger, "unreachable", detail("value"))
	require.Equal(t, 1, logger.calls)
	require.Equal(t, "failed assertion: unreachable", logger.message)
	require.Equal(t, []softassert.Field{softassert.FailedAssertion, detail("value")}, logger.fields)
}
