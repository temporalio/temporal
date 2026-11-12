package softassert_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestForwardingPreservesTemporalMarker(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	expected := logger.Expect(testlogger.Error, "^failed assertion: broken invariant$", tag.FailedAssertion)
	capture := logger.StartCapture(tag.FailedAssertion)
	require.True(t, softassert.That(logger, true, "passing invariant"))
	require.Empty(t, capture.Snapshot())
	require.False(t, softassert.That(logger, false, "broken invariant", tag.String("detail", "value")))
	softassert.Fail(logger, "broken invariant", tag.String("detail", "value"))
	require.EqualValues(t, 2, expected.MatchCount())
	record := testlogger.CapturedLog{
		Level: testlogger.Error, Message: "failed assertion: broken invariant",
		Tags: []tag.Tag{tag.FailedAssertion, tag.String("detail", "value")},
	}
	require.Equal(t, []testlogger.CapturedLog{record, record}, capture.Snapshot())
}
