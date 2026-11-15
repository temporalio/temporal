package testlogger_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type outputT struct {
	*testing.T
	logs []string
}

func (t *outputT) Logf(format string, args ...any) {
	t.logs = append(t.logs, fmt.Sprintf(format, args...))
}

func TestOutputPreservesServerConfiguration(t *testing.T) {
	t.Setenv(log.TestLogFormatEnvVar, "json")
	t.Setenv(log.TestLogLevelEnvVar, "info")
	t.Setenv(log.TestLogFileEnvVar, "")
	tb := &outputT{T: t}
	logger := testlogger.NewTestLogger(tb, testlogger.FailOnAnyUnexpectedError,
		testlogger.WithLogTags(tag.String("inherited", "value")),
	)
	logger.Debug("filtered")
	_, file, line, _ := runtime.Caller(0)
	logger.Info("visible", tag.Int("count", 7))

	require.Len(t, tb.logs, 1)
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(tb.logs[0]), &record))
	delete(record, "ts")
	require.Equal(t, map[string]any{
		"level": "info", "msg": "visible", "count": float64(7), "inherited": "value",
		"logging-call-at": fmt.Sprintf("%s:%d", file, line+1),
	}, record)
}

func TestFailureReportsCaller(t *testing.T) {
	if level := os.Getenv("TESTLOGGER_FAILURE_PROBE"); level != "" {
		logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
		switch level {
		case "Error":
			logger.Error("unexpected")
		case "DPanic":
			logger.DPanic("unexpected")
		case "Panic":
			logger.Panic("unexpected")
		case "Fatal":
			logger.Fatal("unexpected")
		case "Capture":
			logger.StartCapture().RequireContains(t, testlogger.CapturedLogPattern{Message: "missing"})
		default:
			t.Fatalf("unknown failure probe %q", level)
		}
		return
	}
	t.Parallel()
	for _, level := range []string{"Error", "DPanic", "Panic", "Fatal", "Capture"} {
		t.Run(level, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestFailureReportsCaller$")
			cmd.Env = append(os.Environ(), "TESTLOGGER_FAILURE_PROBE="+level)
			output, err := cmd.CombinedOutput()
			require.Error(t, err)
			require.Regexp(t, `output_test.go:[0-9]+: (FAIL: Unexpected|captured log pattern not found)`, string(output))
		})
	}
}

func TestWrappedOutputPreservesCaller(t *testing.T) {
	t.Parallel()
	core, records := observer.New(zap.InfoLevel)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError,
		testlogger.WrapLogger(log.NewZapLogger(zap.New(core)).Skip(1)),
	)
	_, file, line, _ := runtime.Caller(0)
	logger.Info("visible")

	require.Len(t, records.All(), 1)
	require.Equal(t, map[string]any{"logging-call-at": fmt.Sprintf("%s:%d", file, line+1)}, records.All()[0].ContextMap())
}

type customTag struct{}

func (customTag) Key() string { return "custom" }
func (customTag) Value() any  { return "value" }

func TestCustomTagsSurviveAdapter(t *testing.T) {
	t.Parallel()
	core, records := observer.New(zap.InfoLevel)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError,
		testlogger.WrapLogger(log.NewZapLogger(zap.New(core))),
	)
	expected := logger.Expect(testlogger.Info, "message", customTag{})
	capture := logger.StartCapture(customTag{})
	logger.Info("message", customTag{})

	require.Len(t, records.All(), 1)
	require.Equal(t, "value", records.All()[0].ContextMap()["custom"])
	require.True(t, expected.Matched())
	require.Len(t, capture.Snapshot(), 1)
	require.Equal(t, "value", capture.Snapshot()[0].Tags[0].Value())
}

func TestOptionsReceiveInitializedLogger(t *testing.T) {
	t.Parallel()
	var expected *testlogger.Expectation
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError, func(logger *testlogger.TestLogger) {
		require.Same(t, t, logger.T())
		logger.FailOnFatal(false)
		expected = logger.Expect(testlogger.Info, "configured")
	})
	logger.Info("configured")
	require.True(t, expected.Matched())
}

func TestExpectationOutputPreservesCaller(t *testing.T) {
	t.Setenv(log.TestLogFormatEnvVar, "json")
	t.Setenv(log.TestLogFileEnvVar, "")
	tb := &outputT{T: t}
	logger := testlogger.NewTestLogger(tb, testlogger.FailOnAnyUnexpectedError, func(logger *testlogger.TestLogger) {
		testlogger.LogExpectations(logger)
	})
	_, file, line, _ := runtime.Caller(0)
	logger.Expect(testlogger.Info, "expected")
	require.Len(t, tb.logs, 1)
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(tb.logs[0]), &record))
	require.Equal(t, fmt.Sprintf("%s:%d", file, line+1), record["logging-call-at"])
	require.Contains(t, record["msg"], "TestLogger::Expecting")
}
