package testlogger_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testlogger"
)

func TestCaptureLifecycle(t *testing.T) {
	t.Parallel()

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	testLogger.Info("before") // won't be captured

	capture := testLogger.StartCapture()
	testLogger.Debug("debug", field{"key", "value"})
	testLogger.Info("info")
	testLogger.Warn("warn")
	testLogger.Error("error")

	testLogger.StopCapture(capture)
	testLogger.Info("after") // won't be captured

	require.Equal(t, []testlogger.CapturedLog[field]{
		{Level: testlogger.Debug, Message: "debug", Tags: []field{field{"key", "value"}}},
		{Level: testlogger.Info, Message: "info"},
		{Level: testlogger.Warn, Message: "warn"},
		{Level: testlogger.Error, Message: "error"},
	}, capture.Snapshot())
}

func TestCaptureSharesStateWithDerivedLoggers(t *testing.T) {
	t.Parallel()

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()

	testLogger.With(field{"inherited", "value"}).Info("with")
	testLogger.WithTags(field{"direct", "value"}).Info("with-tags")

	require.Equal(t, []testlogger.CapturedLog[field]{
		{Level: testlogger.Info, Message: "with", Tags: []field{field{"inherited", "value"}}},
		{Level: testlogger.Info, Message: "with-tags", Tags: []field{field{"direct", "value"}}},
	}, capture.Snapshot())
}

func TestCaptureFiltersIndependently(t *testing.T) {
	t.Parallel()

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	all := testLogger.StartCapture()
	matching := testLogger.StartCapture(field{"keep", "true"})

	testLogger.Info("excluded")
	testLogger.Info("included", field{"keep", "true"})

	require.Len(t, all.Snapshot(), 2)
	require.Equal(t, []testlogger.CapturedLog[field]{{
		Level:   testlogger.Info,
		Message: "included",
		Tags:    []field{field{"keep", "true"}},
	}}, matching.Snapshot())
}

func TestCaptureSnapshotIsDefensiveCopy(t *testing.T) {
	t.Parallel()

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()
	tags := []field{field{"key", "original"}}

	testLogger.Info("message", tags...)
	tags[0] = field{"key", "mutated input"}

	// mutate the snapshot
	first := capture.Snapshot()
	first[0].Message = "mutated snapshot"
	first[0].Tags[0] = field{"key", "mutated snapshot"}

	require.Equal(t, []testlogger.CapturedLog[field]{{
		Level:   testlogger.Info,
		Message: "message",
		Tags:    []field{field{"key", "original"}},
	}}, capture.Snapshot())
}

func TestCaptureRequireContains(t *testing.T) {
	t.Parallel()

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()
	testLogger.Error("failed", field{"operation", "StartOperation"}, field{"attempt-start", time.Now()}, field{"error", errors.New("failure")})

	t.Run("matching pattern", func(t *testing.T) {
		t.Parallel()

		capture.RequireContains(t, testlogger.CapturedLogPattern[field]{
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
		capture.RequireContains(recorder, testlogger.CapturedLogPattern[field]{
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
		capture.RequireContains(recorder, testlogger.CapturedLogPattern[field]{
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
		capture.RequireContains(recorder, testlogger.CapturedLogPattern[field]{
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

	testLogger := newEngine(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()

	const (
		loggerCount   = 10
		recordsPerLog = 20
	)
	var wg sync.WaitGroup
	for loggerID := range loggerCount {
		derived := testLogger.With(field{"logger-id", loggerID})
		wg.Go(func() {
			for range recordsPerLog {
				derived.Info("concurrent")
			}
		})
	}
	wg.Wait()

	require.Len(t, capture.Snapshot(), loggerCount*recordsPerLog)
}
