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
	require.True(t, strings.HasSuffix(record["logging-call-at"].(string), "slog_test.go:40"), "%q doesn't end with %q", record["logging-call-at"], "slog_test.go:40")
}
