package log

import (
	"sync"

	"go.temporal.io/server/common/log/tag"
)

var _ Logger = (*lazyLogger)(nil)
var _ WithLogger = (*lazyLogger)(nil)

type (
	// debugEnabledLogger can prove that Debug is disabled before lazy tags are
	// materialized. Implementations return true when they cannot safely decide.
	debugEnabledLogger interface {
		debugEnabled() bool
	}

	lazyLogger struct {
		logger       Logger
		debugChecker debugEnabledLogger // immutable: logger changes during materialization
		tagFn        func() []tag.Tag

		once sync.Once
	}
)

// NewLazyLogger defers adding tagFn's tags until an operation requires them.
// A disabled Debug call on a supported logger does not evaluate tagFn. Other
// operations retain their existing materialization behavior. tagFn runs at most once.
func NewLazyLogger(logger Logger, tagFn func() []tag.Tag) *lazyLogger {
	var checker debugEnabledLogger
	if supported, ok := logger.(debugEnabledLogger); ok {
		checker = supported
	}
	return &lazyLogger{
		logger:       logger,
		debugChecker: checker,
		tagFn:        tagFn,
	}
}

func (l *lazyLogger) debugEnabled() bool {
	return l.debugChecker == nil || l.debugChecker.debugEnabled()
}

func (l *lazyLogger) Debug(msg string, tags ...tag.Tag) {
	if !l.debugEnabled() {
		return
	}
	l.once.Do(l.tagLogger)
	l.logger.Debug(msg, tags...)
}

func (l *lazyLogger) Info(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Info(msg, tags...)
}

func (l *lazyLogger) Warn(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Warn(msg, tags...)
}

func (l *lazyLogger) Error(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Error(msg, tags...)
}

func (l *lazyLogger) DPanic(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.DPanic(msg, tags...)
}

func (l *lazyLogger) Panic(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Panic(msg, tags...)
}

func (l *lazyLogger) Fatal(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Fatal(msg, tags...)
}

func (l *lazyLogger) With(tags ...tag.Tag) Logger {
	l.once.Do(l.tagLogger)
	return With(l.logger, tags...)
}

func (l *lazyLogger) tagLogger() {
	l.logger = With(l.logger, l.tagFn()...)
}
