package testlogger_test

import (
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/parallelsuite"
)

type compatibilitySuite struct {
	parallelsuite.Suite[*compatibilitySuite]
}

func TestCompatibilitySuite(t *testing.T) {
	parallelsuite.Run(t, &compatibilitySuite{})
}

func (s *compatibilitySuite) TestWithLoggerPreservesServerInterface() {
	testLogger := testlogger.NewTestLogger(s.T(), testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()

	log.With(testLogger, tag.String("inherited", "value")).Info("with")
	testLogger.WithTags(tag.String("direct", "value")).Info("with-tags")

	s.Equal([]testlogger.CapturedLog{
		{Level: testlogger.Info, Message: "with", Tags: []tag.Tag{tag.String("inherited", "value")}},
		{Level: testlogger.Info, Message: "with-tags", Tags: []tag.Tag{tag.String("direct", "value")}},
	}, capture.Snapshot())
}
