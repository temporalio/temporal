package testcore

import (
	"bytes"
	"os"

	"go.uber.org/zap/zapcore"
)

// clusterLogWriter routes zap log lines to the sole registered test's t.Logf,
// or to stderr when there is zero or more than one active test.
type clusterLogWriter struct {
	s *FunctionalTestBase
}

var _ zapcore.WriteSyncer = (*clusterLogWriter)(nil)

func (w *clusterLogWriter) Write(p []byte) (int, error) {
	if t := w.s.currentT(); t != nil {
		t.Logf("%s", bytes.TrimRight(p, "\n"))
		return len(p), nil
	}
	return os.Stderr.Write(p)
}

func (w *clusterLogWriter) Sync() error { return nil }
