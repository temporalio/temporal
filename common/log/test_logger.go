package log

import (
	"fmt"
	"go.temporal.io/server/common/log/tag"
	"testing"
)

type testLogger struct {
	t *testing.T
}

func (l testLogger) Debug(msg string, tags ...tag.Tag) {
	l.log("DEBUG", msg, tags...)
}

func (l testLogger) Info(msg string, tags ...tag.Tag) {
	l.log("INFO", msg, tags...)
}

func (l testLogger) Warn(msg string, tags ...tag.Tag) {
	l.log("WARN", msg, tags...)
}

func (l testLogger) Error(msg string, tags ...tag.Tag) {
	l.log("ERROR", msg, tags...)
}

func (l testLogger) Fatal(msg string, tags ...tag.Tag) {
	l.log("FATAL", msg, tags...)
}

func (l testLogger) log(prefix string, msg string, tags ...tag.Tag) {
	l.t.Log(fmt.Sprintf("%s: %s", prefix, msg), tags)
}

func NewUnitTestLogger(t *testing.T) *testLogger {
	return &testLogger{t: t}
}