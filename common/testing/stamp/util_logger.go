package stamp

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var (
	actionIdTag = func(t ActID) tag.ZapTag {
		return tag.NewStringTag("actionID", string(t))
	}
)

type (
	logWrapper struct {
		log.Logger
	}
)

func newLogger(logger log.Logger) log.Logger {
	return &logWrapper{Logger: logger}
}

func (l *logWrapper) Debug(msg string, tags ...tag.Tag) {
	l.Logger.Debug(msg+"\n", tags...) // TODO
}

func (l *logWrapper) Info(msg string, tags ...tag.Tag) {
	l.Logger.Info(msg+"\n", tags...)
}

func (l *logWrapper) Warn(msg string, tags ...tag.Tag) {
	l.Logger.Warn(msg+"\n", tags...)
}

func (l *logWrapper) Error(msg string, tags ...tag.Tag) {
	l.Logger.Error(redStr(msg)+"\n", tags...)
}

func (l *logWrapper) DPanic(msg string, tags ...tag.Tag) {
	l.Logger.DPanic(msg+"\n", tags...)
}

func (l *logWrapper) Panic(msg string, tags ...tag.Tag) {
	l.Logger.Panic(msg+"\n", tags...)
}

func (l *logWrapper) Fatal(msg string, tags ...tag.Tag) {
	l.Logger.Fatal("💥 "+redStr(msg)+"\n", tags...)
}
