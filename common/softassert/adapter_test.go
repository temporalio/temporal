package softassert_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	serversoftassert "go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/softassert"
)

func TestAdapterPreservesSharedMarker(t *testing.T) {
	t.Parallel()
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	expected := logger.Expect(testlogger.Error, "^failed assertion: broken invariant$", softassert.FailedAssertion)
	capture := logger.StartCapture(softassert.FailedAssertion)
	require.True(t, serversoftassert.That(logger, true, "passing invariant"))
	require.Empty(t, capture.Snapshot())
	require.False(t, serversoftassert.That(logger, false, "broken invariant", tag.String("detail", "value")))
	serversoftassert.Fail(logger, "broken invariant", tag.String("detail", "value"))
	require.EqualValues(t, 2, expected.MatchCount())
	record := testlogger.CapturedLog{
		Level: testlogger.Error, Message: "failed assertion: broken invariant",
		Tags: []tag.Tag{tag.FailedAssertion, tag.String("detail", "value")},
	}
	require.Equal(t, []testlogger.CapturedLog{record, record}, capture.Snapshot())
}
