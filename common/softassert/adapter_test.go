package softassert_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	server "go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/softassert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestAdapterPreservesSharedMarker(t *testing.T) {
	t.Parallel()

	core, observed := observer.New(zap.ErrorLevel)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError, testlogger.WrapLogger(log.NewZapLogger(zap.New(core))))
	expected := logger.Expect(testlogger.Error, "^failed assertion: broken invariant$", softassert.FailedAssertion)
	capture := logger.StartCapture(softassert.FailedAssertion)

	require.True(t, server.That(logger, true, "passing invariant"))
	require.Empty(t, capture.Snapshot())

	require.False(t, server.That(logger, false, "broken invariant", tag.String("detail", "value")))
	server.Fail(logger, "broken invariant", tag.String("detail", "value"))

	require.EqualValues(t, 2, expected.MatchCount())
	record := testlogger.CapturedLog{
		Level: testlogger.Error, Message: "failed assertion: broken invariant",
		Tags: []tag.Tag{softassert.FailedAssertion, tag.String("detail", "value")},
	}
	require.Equal(t, []testlogger.CapturedLog{record, record}, capture.Snapshot())

	entries := observed.All()
	require.Len(t, entries, 2)
	for _, entry := range entries {
		require.Equal(t, true, entry.ContextMap()["failed-assertion"])
	}
}
