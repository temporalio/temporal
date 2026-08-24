package testlogger_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

type fatalRecorder struct {
	helperCalled bool
	message      string
}

func (r *fatalRecorder) Helper() {
	r.helperCalled = true
}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.message = fmt.Sprintf(format, args...)
}

func TestCaptureLifecycle(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	testLogger.Info("before")

	capture := testLogger.StartCapture()
	testLogger.Debug("debug", tag.String("key", "value"))
	testLogger.Info("info")
	testLogger.Warn("warn")
	testLogger.Error("error")
	testLogger.StopCapture(capture)
	testLogger.Info("after")

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

func TestCaptureContains(t *testing.T) {
	t.Parallel()

	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	capture := testLogger.StartCapture()
	testLogger.Error("failed", tag.String("operation", "StartOperation"), tag.Time("attempt-start", time.Now()), tag.Error(errors.New("failure")))

	pattern := testlogger.CapturedLogPattern{
		Level:   testlogger.Error,
		Message: "failed",
		Tags: map[string]any{
			"operation":     "StartOperation",
			"attempt-start": testlogger.AnyTagValue,
			"error":         "failure",
		},
	}
	capture.RequireContains(t, pattern)

	pattern.Tags["operation"] = "CancelOperation"
	recorder := &fatalRecorder{}
	capture.RequireContains(recorder, pattern)
	require.True(t, recorder.helperCalled)
	require.Contains(t, recorder.message, "candidate 1 tag mismatch")
	require.Contains(t, recorder.message, "CancelOperation")
	require.Contains(t, recorder.message, "StartOperation")

	// A pattern with an extra tag matches nothing, since tag sets must match exactly.
	pattern.Tags["operation"] = "StartOperation"
	pattern.Tags["unexpected"] = "value"
	recorder = &fatalRecorder{}
	capture.RequireContains(recorder, pattern)
	require.Contains(t, recorder.message, "unexpected")
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
