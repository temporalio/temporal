// Package testlogger adapts go.temporal.io/testx/testlogger to server loggers.
package testlogger

import (
	"os"
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	shared "go.temporal.io/testx/testlogger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	Level           = shared.Level
	Mode            = shared.Mode
	TestingT        = shared.TestingT
	CleanupCapableT = shared.CleanupCapableT
	Expectation     = shared.Expectation
)

const (
	Debug  = shared.Debug
	Info   = shared.Info
	Warn   = shared.Warn
	Error  = shared.Error
	Fatal  = shared.Fatal
	DPanic = shared.DPanic
	Panic  = shared.Panic

	FailOnAnyUnexpectedError = shared.FailOnAnyUnexpectedError
	FailOnExpectedErrorOnly  = shared.FailOnExpectedErrorOnly
)

// Failure describes a log call that shouldFailTest considered a test-failing log.
type Failure struct {
	Level Level
	Msg   string
	Tags  []tag.Tag
	Stack string
}

// TestLogger is a log.Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	*shared.TestLogger
	wrapped   log.Logger
	level     zapcore.Level
	logCaller bool
	failure   *atomic.Pointer[Failure]
}

type LoggerOption func(*TestLogger)

func DontFailOnError(t *TestLogger) *TestLogger {
	shared.DontFailOnError(t.TestLogger)
	return t
}

func DontFailOnDPanic(t *TestLogger) *TestLogger {
	shared.DontFailOnDPanic(t.TestLogger)
	return t
}

func LogExpectations(t *TestLogger) *TestLogger {
	shared.LogExpectations(t.TestLogger)
	return t
}

// WithLogTags adds tags to the logs emitted by the underlying logger
func WithLogTags(tags ...tag.Tag) LoggerOption {
	return func(t *TestLogger) { shared.WithFields(toFields(tags)...)(t.TestLogger) }
}

func WrapLogger(l log.Logger) LoggerOption {
	return func(t *TestLogger) { t.wrapped = l }
}

func LogLevel(level zapcore.Level) LoggerOption {
	return func(t *TestLogger) { t.level = level }
}

// WithoutCaller stops the TestLogger from logging the caller.
// This is primarily intended for use when parsing log.Logger-generated logs in files
// or coming from subprocesses.
func WithoutCaller() LoggerOption {
	return func(t *TestLogger) { t.logCaller = false }
}

// SetLogLevel overrides the temporal test log level during this test.
func SetLogLevel(tt CleanupCapableT, level zapcore.Level) LoggerOption {
	return func(t *TestLogger) {
		oldLevel := os.Getenv(log.TestLogLevelEnvVar)
		_ = os.Setenv(log.TestLogLevelEnvVar, level.String())
		tt.Cleanup(func() { _ = os.Setenv(log.TestLogLevelEnvVar, oldLevel) })
		t.level = level
	}
}

// NewTestLogger creates a new TestLogger that logs to the provided testing.T.
// Mode controls the behavior of the logger for when an expected or unexpected error is encountered.
func NewTestLogger(t TestingT, mode Mode, opts ...LoggerOption) *TestLogger {
	tl := &TestLogger{
		TestLogger: shared.NewTestLogger(t, mode, shared.WrapLogger(zap.NewNop())),
		level:      zapcore.DebugLevel,
		logCaller:  true,
		failure:    new(atomic.Pointer[Failure]),
	}
	for _, opt := range opts {
		opt(tl)
	}
	if tl.wrapped == nil {
		tl.configureLogger(t)
	} else if logger, ok := tl.wrapped.(log.SkipLogger); ok {
		tl.wrapped = logger.Skip(2)
	}
	shared.WrapLogger(loggerAdapter{tl.wrapped})(tl.TestLogger)
	return tl
}

// Expect instructs the logger to track matching log calls, as specified by the
// level, msg, and tag arguments. For error-level logs, depending on the Mode of
// the test logger, the expectation either acts as an entry in a blocklist
// (FailOnExpectedErrorOnly) or an allowlist (FailOnAnyUnexpectedError).
func (tl *TestLogger) Expect(level Level, msg string, tags ...tag.Tag) *Expectation {
	return tl.TestLogger.Expect(level, msg, toFields(tags)...)
}

// Failure returns the first Failure that shouldFailTest considered a test-failing
// log, or nil if no such log has been observed. Sticky: first failure wins.
func (tl *TestLogger) Failure() *Failure {
	if failure := tl.failure.Load(); failure != nil {
		return failure
	}
	if failure := tl.TestLogger.Failure(); failure != nil {
		tl.failure.CompareAndSwap(nil, &Failure{failure.Level, failure.Msg, toTags(failure.Fields), failure.Stack})
	}
	return tl.failure.Load()
}

// WithTags gives you a new logger, copying the tags of the source, appending the provided new Tags
func (tl *TestLogger) WithTags(tags ...tag.Tag) *TestLogger {
	clone := *tl
	clone.TestLogger = tl.WithFields(toFields(tags)...)
	return &clone
}

// With implements log.WithLogger
func (tl *TestLogger) With(tags ...tag.Tag) log.Logger {
	clone := *tl
	clone.TestLogger = tl.TestLogger.With(toFields(tags)...)
	return &clone
}

var _ log.Logger = (*TestLogger)(nil)
var _ log.WithLogger = (*TestLogger)(nil)

func toFields(tags []tag.Tag) []zap.Field {
	if tags == nil {
		return nil
	}
	fields := make([]zap.Field, len(tags))
	for i, t := range tags {
		if zt, ok := t.(tag.ZapTag); ok {
			fields[i] = zt.Field()
		} else {
			fields[i] = zap.Any(t.Key(), t.Value())
		}
	}
	return fields
}

func toTags(fields []zap.Field) []tag.Tag {
	if fields == nil {
		return nil
	}
	tags := make([]tag.Tag, len(fields))
	for i, field := range fields {
		tags[i] = tag.NewZapTag(field)
	}
	return tags
}

type loggerAdapter struct{ log.Logger }

// Debug implements log.Logger.
func (tl *TestLogger) Debug(msg string, tags ...tag.Tag) {
	tl.TestLogger.Debug(msg, toFields(tags)...)
}

func (l loggerAdapter) Debug(msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, toTags(fields)...)
}

// Info implements log.Logger.
func (tl *TestLogger) Info(msg string, tags ...tag.Tag) {
	tl.TestLogger.Info(msg, toFields(tags)...)
}

func (l loggerAdapter) Info(msg string, fields ...zap.Field) {
	l.Logger.Info(msg, toTags(fields)...)
}

// Warn implements log.Logger.
func (tl *TestLogger) Warn(msg string, tags ...tag.Tag) {
	tl.TestLogger.Warn(msg, toFields(tags)...)
}

func (l loggerAdapter) Warn(msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, toTags(fields)...)
}

// Error implements log.Logger.
func (tl *TestLogger) Error(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.TestLogger.Error(msg, toFields(tags)...)
}

func (l loggerAdapter) Error(msg string, fields ...zap.Field) {
	l.Logger.Error(msg, toTags(fields)...)
}

// DPanic implements log.Logger.
func (tl *TestLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.TestLogger.DPanic(msg, toFields(tags)...)
}

func (l loggerAdapter) DPanic(msg string, fields ...zap.Field) {
	l.Logger.DPanic(msg, toTags(fields)...)
}

// Panic implements log.Logger.
func (tl *TestLogger) Panic(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.TestLogger.Panic(msg, toFields(tags)...)
}

func (l loggerAdapter) Panic(msg string, fields ...zap.Field) {
	l.Logger.Panic(msg, toTags(fields)...)
}

// Fatal implements log.Logger.
func (tl *TestLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.TestLogger.Fatal(msg, toFields(tags)...)
}

func (l loggerAdapter) Fatal(msg string, fields ...zap.Field) {
	l.Logger.Fatal(msg, toTags(fields)...)
}
