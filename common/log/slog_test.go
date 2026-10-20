package log

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/quotas"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type writeBuffer struct {
	buf []byte
}

var _ zapcore.WriteSyncer = (*writeBuffer)(nil)

// Sync implements zapcore.WriteSyncer.
func (*writeBuffer) Sync() error {
	return nil
}

// Write implements zapcore.WriteSyncer.
func (b *writeBuffer) Write(p []byte) (n int, err error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func TestSlogFromLayeredLogger(t *testing.T) {
	buffer := writeBuffer{nil}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), &buffer, zap.InfoLevel)
	l := NewSlogLogger(NewThrottledLogger(With(NewZapLogger(zap.New(core)), tag.ClusterName("corn")), quotas.InfDuration.Hours))
	require.True(t, l.Enabled(context.Background(), slog.LevelInfo))
	require.False(t, l.Enabled(context.Background(), slog.LevelDebug))
	l.With("a", "b").WithGroup("hey").With("ho", "lets").WithGroup("go").With("c", 4).Info("wow", "gg", "wp")
	var record map[string]any
	err := json.Unmarshal(buffer.buf, &record)
	require.NoError(t, err)
	require.Equal(t, "b", record["a"])
	require.Equal(t, "lets", record["hey.ho"])
	require.Equal(t, float64(4), record["hey.go.c"]) // untyped json unmarshals numbers as floats
	require.Equal(t, "wp", record["hey.go.gg"])
	require.Equal(t, "wow", record["msg"])
	require.Equal(t, "corn", record["cluster-name"])
	require.True(t, strings.HasSuffix(record["logging-call-at"].(string), "slog_test.go:41"), "%q doesn't end with %q", record["logging-call-at"], "slog_test.go:41")
}

// TestSlogLevelMapping pins Handle's level mapping to the one Enabled already uses
// (convertSlogToZapLevel). slog levels are plain ints and the API supports values between the
// named constants, so a record admitted by Enabled must also be emitted by Handle.
func TestSlogLevelMapping(t *testing.T) {
	core, recorded := observer.New(zap.DebugLevel)
	l := NewSlogLogger(NewZapLogger(zap.New(core)))

	for _, tc := range []struct {
		name  string
		level slog.Level
		want  zapcore.Level
	}{
		// The named levels.
		{"debug", slog.LevelDebug, zapcore.DebugLevel},
		{"info", slog.LevelInfo, zapcore.InfoLevel},
		{"warn", slog.LevelWarn, zapcore.WarnLevel},
		{"error", slog.LevelError, zapcore.ErrorLevel},
		// Levels between and beyond them, which slog explicitly permits.
		{"below debug", slog.LevelDebug - 1, zapcore.DebugLevel},
		{"between debug and info", slog.LevelDebug + 1, zapcore.DebugLevel},
		{"between info and warn", slog.LevelInfo + 1, zapcore.InfoLevel},
		{"between warn and error", slog.LevelWarn + 1, zapcore.WarnLevel},
		{"above error", slog.LevelError + 1, zapcore.ErrorLevel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(recorded.All())
			require.True(t, l.Enabled(context.Background(), tc.level),
				"Enabled must admit level %d", tc.level)

			l.Log(context.Background(), tc.level, tc.name)

			entries := recorded.All()
			require.Len(t, entries, before+1,
				"level %d was admitted by Enabled but dropped by Handle", tc.level)
			got := entries[before]
			require.Equal(t, tc.want, got.Level, "wrong zap level for slog level %d", tc.level)
			require.Equal(t, tc.name, got.Message)
		})
	}
}
