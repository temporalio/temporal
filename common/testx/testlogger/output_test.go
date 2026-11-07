package testlogger_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testlogger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type outputT struct {
	*testing.T
	logs []string
}

func (t *outputT) Logf(format string, args ...any) {
	t.logs = append(t.logs, fmt.Sprintf(format, args...))
}

func TestDefaultOutputHonorsOptions(t *testing.T) {
	t.Parallel()
	tb := &outputT{T: t}
	logger := testlogger.NewTestLogger(tb, testlogger.FailOnAnyUnexpectedError,
		testlogger.LogLevel(zapcore.InfoLevel), testlogger.WithoutCaller(),
	)
	logger.Debug("filtered")
	logger.Info("visible", zap.Int("count", 7))

	require.Len(t, tb.logs, 1)
	require.Contains(t, tb.logs[0], "visible")
	require.Contains(t, tb.logs[0], `"count": 7`)
	require.NotContains(t, tb.logs[0], "output_test.go")
}

func TestWrappedZapLoggerReceivesMergedFields(t *testing.T) {
	t.Parallel()
	core, records := observer.New(zapcore.DebugLevel)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError,
		testlogger.WrapLogger(zap.New(core)),
		testlogger.WithFields(zap.String("inherited", "value")),
	)
	logger.With(zap.String("operation", "old")).Info("message", zap.String("operation", "new"))

	require.Len(t, records.All(), 1)
	require.Equal(t, map[string]any{"inherited": "value", "operation": "new"}, records.All()[0].ContextMap())
}
