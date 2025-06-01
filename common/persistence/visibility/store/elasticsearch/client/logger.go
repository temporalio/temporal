package client

import (
	"fmt"

	"go.temporal.io/server/common/log"
)

type (
	errorLogger struct {
		log.Logger
	}

	infoLogger struct {
		log.Logger
	}
)

func newErrorLogger(logger log.Logger) *errorLogger {
	return &errorLogger{logger}
}

func (l *errorLogger) Printf(format string, v ...interface{}) {
	l.Error(fmt.Sprintf(format, v...))
}

func newInfoLogger(logger log.Logger) *infoLogger {
	return &infoLogger{logger}
}

func (l *infoLogger) Printf(format string, v ...interface{}) {
	l.Info(fmt.Sprintf(format, v...))
}
