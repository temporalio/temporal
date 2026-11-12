package testlogger

import (
	"os"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger/internal/engine"
	"go.uber.org/zap/zapcore"
)

type (
	Level           = engine.Level
	Mode            = engine.Mode
	TestingT        = engine.TestingT
	CleanupCapableT = engine.CleanupCapableT
	Expectation     = engine.Expectation[tag.Tag]
	Failure         = engine.Failure[tag.Tag]
)

const (
	Debug                    = engine.Debug
	Info                     = engine.Info
	Warn                     = engine.Warn
	Error                    = engine.Error
	Fatal                    = engine.Fatal
	DPanic                   = engine.DPanic
	Panic                    = engine.Panic
	FailOnAnyUnexpectedError = engine.FailOnAnyUnexpectedError
	FailOnExpectedErrorOnly  = engine.FailOnExpectedErrorOnly
)

// TestLogger is a log.Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	*engine.Engine[tag.Tag]
	wrapped   log.Logger
	level     zapcore.Level
	logCaller bool
}

type LoggerOption func(*TestLogger)

func DontFailOnError(t *TestLogger) *TestLogger {
	t.FailOnError(false)
	return t
}

func DontFailOnDPanic(t *TestLogger) *TestLogger {
	t.FailOnDPanic(false)
	return t
}

func LogExpectations(t *TestLogger) *TestLogger {
	engine.LogExpectations(t.Engine)
	return t
}

// WithLogTags adds tags to the logs emitted by the underlying logger
func WithLogTags(tags ...tag.Tag) LoggerOption {
	return func(t *TestLogger) {
		engine.WithLogTags(tags...)(t.Engine)
	}
}

func WrapLogger(l log.Logger) LoggerOption {
	return func(t *TestLogger) {
		t.wrapped = l
		engine.WrapLogger[tag.Tag](l)(t.Engine)
	}
}

func LogLevel(level zapcore.Level) LoggerOption {
	return func(t *TestLogger) {
		t.level = level
	}
}

// WithoutCaller stops the TestLogger from logging the caller.
// This is primarily intended for use when parsing log.Logger-generated logs in files
// or coming from subprocesses.
func WithoutCaller() LoggerOption {
	return func(t *TestLogger) {
		t.logCaller = false
	}
}

// SetLogLevel overrides the temporal test log level during this test.
func SetLogLevel(tt CleanupCapableT, level zapcore.Level) LoggerOption {
	return func(t *TestLogger) {
		oldLevel := os.Getenv(log.TestLogLevelEnvVar)
		_ = os.Setenv(log.TestLogLevelEnvVar, level.String())
		tt.Cleanup(func() {
			_ = os.Setenv(log.TestLogLevelEnvVar, oldLevel)
		})

		t.level = level
	}
}

// NewTestLogger creates a new TestLogger that logs to the provided testing.T.
// Mode controls the behavior of the logger for when an expected or unexpected error is encountered.
func NewTestLogger(t TestingT, mode Mode, opts ...LoggerOption) *TestLogger {
	tl := &TestLogger{level: zapcore.DebugLevel, logCaller: true}
	tl.Engine = engine.NewEngine[tag.Tag](t, mode, nil, engine.CallerSkip[tag.Tag](1), func(e *engine.Engine[tag.Tag]) {
		tl.Engine = e
		for _, opt := range opts {
			opt(tl)
		}
		if tl.wrapped == nil {
			tl.configureLogger(t)
		} else if logger, ok := tl.wrapped.(log.SkipLogger); ok {
			tl.wrapped = logger.Skip(1)
		}
		engine.WrapLogger[tag.Tag](tl.wrapped)(e)
	})
	return tl
}

// WithTags gives you a new logger, copying the tags of the source, appending the provided new Tags
func (tl *TestLogger) WithTags(tags ...tag.Tag) *TestLogger {
	clone := *tl
	clone.Engine = tl.Engine.WithTags(tags...)
	return &clone
}

// With implements log.WithLogger
func (tl *TestLogger) With(tags ...tag.Tag) log.Logger {
	clone := *tl
	clone.Engine = tl.Engine.With(tags...)
	return &clone
}

var _ log.Logger = (*TestLogger)(nil)
var _ log.WithLogger = (*TestLogger)(nil)

// Debug implements log.Logger.
func (tl *TestLogger) Debug(msg string, tags ...tag.Tag) {
	tl.Engine.Debug(msg, tags...)
}

// Info implements log.Logger.
func (tl *TestLogger) Info(msg string, tags ...tag.Tag) {
	tl.Engine.Info(msg, tags...)
}

// Warn implements log.Logger.
func (tl *TestLogger) Warn(msg string, tags ...tag.Tag) {
	tl.Engine.Warn(msg, tags...)
}

// Error implements log.Logger.
func (tl *TestLogger) Error(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.Engine.Error(msg, tags...)
}

// DPanic implements log.Logger.
func (tl *TestLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.Engine.DPanic(msg, tags...)
}

// Panic implements log.Logger.
func (tl *TestLogger) Panic(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.Engine.Panic(msg, tags...)
}

// Fatal implements log.Logger.
func (tl *TestLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.T().Helper()
	tl.Engine.Fatal(msg, tags...)
}

// Expect instructs the logger to track matching log calls, as specified by the
// level, msg, and tag arguments. For error-level logs, depending on the Mode of
// the test logger, the expectation either acts as an entry in a blocklist
// (FailOnExpectedErrorOnly) or an allowlist (FailOnAnyUnexpectedError).
func (tl *TestLogger) Expect(level Level, msg string, tags ...tag.Tag) *Expectation {
	return tl.Engine.Expect(level, msg, tags...)
}
