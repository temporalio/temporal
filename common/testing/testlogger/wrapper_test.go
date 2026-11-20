package testlogger_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestWithLoggerPreservesServerInterface(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()

	log.With(testLogger, tag.String("inherited", "value")).Info("with")
	testLogger.WithTags(tag.String("direct", "value")).Info("with-tags")

	require.Equal(t, []testlogger.CapturedLog{
		{Level: testlogger.Info, Message: "with", Tags: []tag.Tag{tag.String("inherited", "value")}},
		{Level: testlogger.Info, Message: "with-tags", Tags: []tag.Tag{tag.String("direct", "value")}},
	}, capture.Snapshot())
}
