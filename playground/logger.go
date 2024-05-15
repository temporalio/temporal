package playground

import (
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ log.Logger = &toggleLogger{}

type toggleLogger struct {
	enabled atomic.Bool
	inner   log.Logger
}

func (l *toggleLogger) Disable() {
	l.enabled.Store(false)
}

func (l *toggleLogger) Debug(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Debug(msg, tags...)
	}
}

func (l *toggleLogger) Info(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Info(msg, tags...)
	}
}

func (l *toggleLogger) Warn(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Warn(msg, tags...)
	}
}

func (l *toggleLogger) Error(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Error(msg, tags...)
	}
}

func (l *toggleLogger) DPanic(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.DPanic(msg, tags...)
	}
}

func (l *toggleLogger) Panic(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Panic(msg, tags...)
	}
}

func (l *toggleLogger) Fatal(msg string, tags ...tag.Tag) {
	if l.enabled.Load() {
		l.inner.Fatal(msg, tags...)
	}
}
