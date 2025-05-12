package log

import (
	"fmt"

	"go.temporal.io/sdk/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	extraSkipForSdkLogger = 1
	noValue               = "no value"
)

type SdkLogger struct {
	logger Logger
}

var _ log.Logger = (*SdkLogger)(nil)

func NewSdkLogger(logger Logger) *SdkLogger {
	if sl, ok := logger.(SkipLogger); ok {
		logger = sl.Skip(extraSkipForSdkLogger)
	}

	return &SdkLogger{
		logger: logger,
	}
}

func (l *SdkLogger) tags(keyvals []interface{}) []tag.Tag {
	var tags []tag.Tag
	for i := 0; i < len(keyvals); i++ {
		if t, keyvalIsTag := keyvals[i].(tag.Tag); keyvalIsTag {
			tags = append(tags, t)
			continue
		}

		key, keyIsString := keyvals[i].(string)
		if !keyIsString {
			key = fmt.Sprintf("%v", keyvals[i])
		}
		var val interface{}
		if i+1 == len(keyvals) {
			val = noValue
		} else {
			val = keyvals[i+1]
			i++
		}

		tags = append(tags, tag.NewAnyTag(key, val))
	}

	return tags
}

func (l *SdkLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.Debug(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.Info(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Warn(msg string, keyvals ...interface{}) {
	l.logger.Warn(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.Error(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) With(keyvals ...interface{}) log.Logger {
	return NewSdkLogger(
		With(l.logger, l.tags(keyvals)...))
}
