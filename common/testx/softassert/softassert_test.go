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
	tags    []softassert.Tag
	calls   int
}

func (l *recordingLogger) Error(message string, tags ...softassert.Tag) {
	l.message = message
	l.tags = tags
	l.calls++
}

func TestThat(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		condition   bool
		wantCalls   int
		wantMessage string
		wantTags    []softassert.Tag
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
			wantTags:    []softassert.Tag{softassert.FailedAssertion, detail("value")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := &recordingLogger{}
			require.Equal(t, tc.condition, softassert.That(logger, tc.condition, "invariant", detail("value")))
			require.Equal(t, tc.wantCalls, logger.calls)
			require.Equal(t, tc.wantMessage, logger.message)
			require.Equal(t, tc.wantTags, logger.tags)
		})
	}
}

func TestFail(t *testing.T) {
	t.Parallel()
	logger := &recordingLogger{}
	softassert.Fail(logger, "unreachable", detail("value"))
	require.Equal(t, 1, logger.calls)
	require.Equal(t, "failed assertion: unreachable", logger.message)
	require.Equal(t, []softassert.Tag{softassert.FailedAssertion, detail("value")}, logger.tags)
}
