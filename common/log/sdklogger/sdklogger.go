// Package sdklogger adapts the server logger to the Go SDK logger interface.
package sdklogger

import (
	"fmt"

	sdk "go.temporal.io/sdk/log"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	extraSkipForSdkLogger = 1
	noValue               = "no value"
)

type Logger struct {
	logger log.Logger
}

var _ sdk.Logger = (*Logger)(nil)

func New(logger log.Logger) *Logger {
	if sl, ok := logger.(log.SkipLogger); ok {
		logger = sl.Skip(extraSkipForSdkLogger)
	}

	return &Logger{
		logger: logger,
	}
}

func (l *Logger) tags(keyvals []any) []tag.Tag {
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
		var val any
		if i+1 == len(keyvals) {
			val = noValue
		} else {
			val = keyvals[i+1]
			i++
		}

		tags = append(tags, tag.Any(key, val))
	}

	return tags
}

func (l *Logger) Debug(msg string, keyvals ...any) {
	l.logger.Debug(msg, l.tags(keyvals)...)
}

func (l *Logger) Info(msg string, keyvals ...any) {
	l.logger.Info(msg, l.tags(keyvals)...)
}

func (l *Logger) Warn(msg string, keyvals ...any) {
	l.logger.Warn(msg, l.tags(keyvals)...)
}

func (l *Logger) Error(msg string, keyvals ...any) {
	l.logger.Error(msg, l.tags(keyvals)...)
}

func (l *Logger) With(keyvals ...any) sdk.Logger {
	return New(
		log.With(l.logger, l.tags(keyvals)...))
}
