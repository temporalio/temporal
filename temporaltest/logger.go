package temporaltest

import (
	"testing"

	"go.temporal.io/sdk/log"
)

var _ log.Logger = &testLogger{}

// testLogger implements a Go SDK logger by writing to the test output.
//
// Text will be printed only if the test fails or the -test.v flag is set.
type testLogger struct {
	t *testing.T
}

func (tl *testLogger) logLevel(lvl, msg string, keyvals ...interface{}) {
	if tl.t == nil {
		return
	}
	args := []interface{}{lvl, msg}
	args = append(args, keyvals...)
	tl.t.Log(args...)
}

func (tl *testLogger) Debug(msg string, keyvals ...interface{}) {
	tl.logLevel("DEBUG", msg, keyvals)
}

func (tl *testLogger) Info(msg string, keyvals ...interface{}) {
	tl.logLevel("INFO ", msg, keyvals)
}

func (tl *testLogger) Warn(msg string, keyvals ...interface{}) {
	tl.logLevel("WARN ", msg, keyvals)
}

func (tl *testLogger) Error(msg string, keyvals ...interface{}) {
	tl.logLevel("ERROR", msg, keyvals)
}
