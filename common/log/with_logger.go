package log

import (
	"go.temporal.io/server/common/log/tag"
)

type withLogger struct {
	logger Logger
	tags   []tag.Tag
}

var _ Logger = (*withLogger)(nil)

// With returns Logger instance that prepend every log entry with tags. If logger implements
// WithLogger it is used, otherwise every log call will be intercepted.
func With(logger Logger, tags ...tag.Tag) Logger {
	if wl, ok := logger.(WithLogger); ok {
		return wl.With(tags...)
	}
	return newWithLogger(logger, tags...)
}

func newWithLogger(logger Logger, tags ...tag.Tag) *withLogger {
	return &withLogger{logger: logger, tags: tags}
}

func (l *withLogger) prependTags(tags []tag.Tag) []tag.Tag {
	allTags := make([]tag.Tag, len(l.tags)+len(tags))
	copy(allTags, l.tags)
	copy(allTags[len(l.tags):], tags)

	return allTags
}

// Debug writes message to the log (if enabled).
func (l *withLogger) Debug(msg string, tags ...tag.Tag) {
	l.logger.Debug(msg, l.prependTags(tags)...)
}

// Info writes message to the log (if enabled).
func (l *withLogger) Info(msg string, tags ...tag.Tag) {
	l.logger.Info(msg, l.prependTags(tags)...)
}

// Warn writes message to the log (if enabled).
func (l *withLogger) Warn(msg string, tags ...tag.Tag) {
	l.logger.Warn(msg, l.prependTags(tags)...)
}

// Error writes message to the log (if enabled).
func (l *withLogger) Error(msg string, tags ...tag.Tag) {
	l.logger.Error(msg, l.prependTags(tags)...)
}

// DPanic writes message to the log (if enabled), then calls panic() in development mode
func (l *withLogger) DPanic(msg string, tags ...tag.Tag) {
	l.logger.DPanic(msg, l.prependTags(tags)...)
}

// Panic writes message to the log (if enabled), then calls panic() no matter what
func (l *withLogger) Panic(msg string, tags ...tag.Tag) {
	l.logger.Panic(msg, l.prependTags(tags)...)
}

// Fatal writes message to the log no matter what, then terminates the process
func (l *withLogger) Fatal(msg string, tags ...tag.Tag) {
	l.logger.Fatal(msg, l.prependTags(tags)...)
}
