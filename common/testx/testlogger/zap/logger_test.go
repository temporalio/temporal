package zap_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testlogger"
	testzap "go.temporal.io/testx/testlogger/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestZapFieldsReachExpectationsAndOutput(t *testing.T) {
	t.Parallel()
	core, observed := observer.New(zap.DebugLevel)
	logger := testzap.Wrap(t, testlogger.FailOnAnyUnexpectedError, zap.New(core))
	expected := logger.Expect(testlogger.Error, "expected error", zap.Int("code", 404))
	capture := logger.StartCapture(zap.String("tenant", "one"))
	logger.With(zap.String("tenant", "one")).Error("expected error", zap.Int("code", 404))
	require.True(t, expected.Matched())
	require.Len(t, observed.All(), 1)
	require.Equal(t, map[string]any{"tenant": "one", "code": int64(404)}, observed.All()[0].ContextMap())
	records := capture.Snapshot()
	require.Len(t, records, 1)
	require.Equal(t, zap.Int("code", 404), records[0].Tags[0].Field)
	capture.RequireContains(t, testlogger.CapturedLogPattern[testzap.Field]{
		Level: testlogger.Error, Message: "expected error", Tags: map[string]any{"code": 404},
	})
}

func TestZapOutputPreservesCaller(t *testing.T) {
	t.Parallel()
	core, observed := observer.New(zap.DebugLevel)
	logger := testzap.Wrap(t, testlogger.FailOnAnyUnexpectedError, zap.New(core, zap.AddCaller()))
	_, file, line, _ := runtime.Caller(0)
	logger.With(zap.String("tenant", "one")).Info("visible")
	require.Len(t, observed.All(), 1)
	require.Equal(t, file, observed.All()[0].Caller.File)
	require.Equal(t, line+1, observed.All()[0].Caller.Line)
}

type outputT struct {
	*testing.T
	logs    []string
	failure string
}

func (t *outputT) Logf(format string, args ...any) {
	t.logs = append(t.logs, fmt.Sprintf(format, args...))
}
func (t *outputT) Fatalf(format string, args ...any) { t.failure = fmt.Sprintf(format, args...) }

func TestDefaultOutputAndFailure(t *testing.T) {
	t.Parallel()
	tb := &outputT{T: t}
	logger := testzap.New(tb, testlogger.FailOnAnyUnexpectedError)
	logger.Info("visible")
	require.Len(t, tb.logs, 1)
	require.Contains(t, tb.logs[0], "visible")
	logger.Error("unexpected", zap.String("key", "value"))
	require.Contains(t, tb.failure, "Unexpected Error log encountered: unexpected")
	require.Contains(t, logger.Failure().Stack, "TestDefaultOutputAndFailure")
	logger.Close()
	count := len(tb.logs)
	logger.Info("after close")
	require.Len(t, tb.logs, count)
}

func TestDevelopmentDPanicObeysTestLoggerControls(t *testing.T) {
	t.Parallel()
	core, observed := observer.New(zap.DebugLevel)
	logger := testzap.Wrap(t, testlogger.FailOnAnyUnexpectedError, zap.New(core, zap.Development()))
	expected := logger.Expect(testlogger.DPanic, "allowed")
	require.NotPanics(t, func() { logger.DPanic("allowed") })
	require.True(t, expected.Matched())
	logger.FailOnDPanic(false)
	require.NotPanics(t, func() { logger.DPanic("disabled") })
	require.Len(t, observed.All(), 2)
}

type inlineObject struct{}

func (inlineObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("key", "value")
	return nil
}

func TestRejectsNonKeyValueFields(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		field zap.Field
	}{
		{
			name:  "namespace",
			field: zap.Namespace("context"),
		},
		{
			name:  "inline",
			field: zap.Inline(inlineObject{}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := testzap.Wrap(t, testlogger.FailOnAnyUnexpectedError, zap.NewNop())
			require.PanicsWithValue(t, "testlogger/zap: namespace and inline modifiers are unsupported", func() { logger.With(tc.field) })
			require.PanicsWithValue(t, "testlogger/zap: namespace and inline modifiers are unsupported", func() { logger.Info("message", tc.field) })
		})
	}
}
