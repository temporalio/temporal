package log

import (
	"go.temporal.io/server/common/log/tag"
)

type (
	noopLogger struct{}
)

// NewNoopLogger return a noopLogger
func NewNoopLogger() *noopLogger {
	return &noopLogger{}
}

func (n *noopLogger) Debug(string, ...tag.Tag)  {}
func (n *noopLogger) Info(string, ...tag.Tag)   {}
func (n *noopLogger) Warn(string, ...tag.Tag)   {}
func (n *noopLogger) Error(string, ...tag.Tag)  {}
func (n *noopLogger) DPanic(string, ...tag.Tag) {}
func (n *noopLogger) Panic(string, ...tag.Tag)  {}
func (n *noopLogger) Fatal(string, ...tag.Tag)  {}
func (n *noopLogger) With(...tag.Tag) Logger {
	return n
}
