// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package log

import (
	"context"
	"encoding/json"
	"log/slog"
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
	require.Equal(t, "slog_test.go:61", record["logging-call-at"])
}
