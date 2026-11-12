package testlogger_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testlogger"
)

type field struct {
	key   string
	value any
}

func (f field) Key() string { return f.key }
func (f field) Value() any  { return f.value }

type discardLogger struct{}

func (discardLogger) Debug(string, ...field)  {}
func (discardLogger) Info(string, ...field)   {}
func (discardLogger) Warn(string, ...field)   {}
func (discardLogger) Error(string, ...field)  {}
func (discardLogger) DPanic(string, ...field) {}
func (discardLogger) Panic(string, ...field)  {}
func (discardLogger) Fatal(string, ...field)  {}

func TestEngineUsesCallerFieldType(t *testing.T) {
	t.Parallel()

	logger := testlogger.NewEngine[field](t, testlogger.FailOnAnyUnexpectedError, discardLogger{})
	expected := logger.Expect(testlogger.Error, "expected error", field{"code", "4[0-9]+"})
	capture := logger.StartCapture(field{"tenant", "one"})

	logger.WithTags(field{"tenant", "one"}).Error("expected error", field{"code", 404})
	logger.Info("another tenant", field{"tenant", "two"})

	require.True(t, expected.Matched())
	require.Equal(t, []testlogger.CapturedLog[field]{
		{
			Level:   testlogger.Error,
			Message: "expected error",
			Tags: []field{
				{"code", 404},
				{"tenant", "one"},
			},
		},
	}, capture.Snapshot())
	capture.RequireContains(t, testlogger.CapturedLogPattern[field]{
		Level:   testlogger.Error,
		Message: "expected error",
		Tags:    map[string]any{"code": 404},
	})

	logger.StopCapture(capture)
	logger.Info("not captured", field{"tenant", "one"})

	require.Len(t, capture.Snapshot(), 1)
	expected.Forget()
}

type panicLogger struct{ discardLogger }

func (panicLogger) Panic(message string, _ ...field) { panic(message) }

func newEngine(t testlogger.TestingT, mode testlogger.Mode) *testlogger.Engine[field] {
	return testlogger.NewEngine[field](t, mode, panicLogger{})
}
