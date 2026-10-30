package testlogger_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestCaptureLifecycle(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	testLogger.Info("before") // won't be captured

	capture := testLogger.StartCapture()
	testLogger.Debug("debug", tag.String("key", "value"))
	testLogger.Info("info")
	testLogger.Warn("warn")
	testLogger.Error("error")

	testLogger.StopCapture(capture)
	testLogger.Info("after") // won't be captured

	require.Equal(t, []testlogger.CapturedLog{
		{Level: testlogger.Debug, Message: "debug", Tags: []tag.Tag{tag.String("key", "value")}},
		{Level: testlogger.Info, Message: "info"},
		{Level: testlogger.Warn, Message: "warn"},
		{Level: testlogger.Error, Message: "error"},
	}, capture.Snapshot())
}

func TestCaptureSharesStateWithDerivedLoggers(t *testing.T) {
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

func TestCaptureFiltersIndependently(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	all := testLogger.StartCapture()
	matching := testLogger.StartCapture(tag.String("keep", "true"))

	testLogger.Info("excluded")
	testLogger.Info("included", tag.String("keep", "true"))

	require.Len(t, all.Snapshot(), 2)
	require.Equal(t, []testlogger.CapturedLog{{
		Level:   testlogger.Info,
		Message: "included",
		Tags:    []tag.Tag{tag.String("keep", "true")},
	}}, matching.Snapshot())
}

func TestCaptureSnapshotIsDefensiveCopy(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()
	tags := []tag.Tag{tag.String("key", "original")}

	testLogger.Info("message", tags...)
	tags[0] = tag.String("key", "mutated input")

	// mutate the snapshot
	first := capture.Snapshot()
	first[0].Message = "mutated snapshot"
	first[0].Tags[0] = tag.String("key", "mutated snapshot")

	require.Equal(t, []testlogger.CapturedLog{{
		Level:   testlogger.Info,
		Message: "message",
		Tags:    []tag.Tag{tag.String("key", "original")},
	}}, capture.Snapshot())
}

func TestCaptureRequireContains(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()
	testLogger.Error("failed", tag.String("operation", "StartOperation"), tag.Time("attempt-start", time.Now()), tag.Error(errors.New("failure")))

	t.Run("matching pattern", func(t *testing.T) {
		t.Parallel()

		capture.RequireContains(t, testlogger.CapturedLogPattern{
			Level:   testlogger.Error,
			Message: "failed",
			Tags: map[string]any{
				"operation":     "StartOperation",
				"attempt-start": testlogger.AnyTagValue,
				"error":         "failure",
			},
		})
	})

	t.Run("mismatched tag", func(t *testing.T) {
		t.Parallel()

		recorder := &mockT{T: t}
		capture.RequireContains(recorder, testlogger.CapturedLogPattern{
			Level:   testlogger.Error,
			Message: "failed",
			Tags: map[string]any{
				"operation":     "CancelOperation", // mismatch!
				"attempt-start": testlogger.AnyTagValue,
				"error":         "failure",
			},
		})
		failure := recorder.failure.Load()
		require.NotNil(t, failure)
		require.Contains(t, *failure, "candidate 1 tag mismatch")
		require.Contains(t, *failure, "CancelOperation")
		require.Contains(t, *failure, "StartOperation")
	})

	// A pattern with an extra tag matches nothing.
	t.Run("extra tag", func(t *testing.T) {
		t.Parallel()

		recorder := &mockT{T: t}
		capture.RequireContains(recorder, testlogger.CapturedLogPattern{
			Level:   testlogger.Error,
			Message: "failed",
			Tags: map[string]any{
				"operation":     "StartOperation",
				"attempt-start": testlogger.AnyTagValue,
				"error":         "failure",
				"unexpected":    "value", // extra!
			},
		})
		failure := recorder.failure.Load()
		require.NotNil(t, failure)
		require.Contains(t, *failure, "unexpected")
	})

	t.Run("missing level and message", func(t *testing.T) {
		t.Parallel()

		recorder := &mockT{T: t}
		capture.RequireContains(recorder, testlogger.CapturedLogPattern{
			Level:   testlogger.Warn,
			Message: "missing",
			// missing!
		})
		failure := recorder.failure.Load()
		require.NotNil(t, failure)
		require.Contains(t, *failure, `Error "failed" operation=StartOperation`)
		require.NotContains(t, *failure, "zapcore")
	})
}

func TestCaptureRecordsConcurrentDerivedLoggers(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly,
		testlogger.WrapLogger(log.NewNoopLogger()),
	)
	capture := testLogger.StartCapture()

	const (
		loggerCount   = 10
		recordsPerLog = 20
	)
	var wg sync.WaitGroup
	for loggerID := range loggerCount {
		derived := log.With(testLogger, tag.Int("logger-id", loggerID))
		wg.Go(func() {
			for range recordsPerLog {
				derived.Info("concurrent")
			}
		})
	}
	wg.Wait()

	require.Len(t, capture.Snapshot(), loggerCount*recordsPerLog)
}
