// Package zap adapts the testlogger engine to Zap loggers and fields.
package zap

import (
	"go.temporal.io/testx/testlogger"
	zaplog "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// Field preserves the original Zap field while satisfying testlogger.Field.
type Field struct{ zaplog.Field }

func (f Field) Key() string { return f.Field.Key }
func (f Field) Value() any {
	encoder := zapcore.NewMapObjectEncoder()
	f.AddTo(encoder)
	return encoder.Fields[f.Key()]
}

// Logger accepts keyed Zap fields and shares expectations and captures with derived loggers.
// Namespace and inline modifiers are unsupported; use keyed fields.
type Logger struct{ *testlogger.Engine[Field] }

// New creates a logger that writes to the test output.
func New(t testlogger.TestingT, mode testlogger.Mode, opts ...zaplog.Option) *Logger {
	writer := zaptest.NewTestingWriter(t)
	cfg := zaplog.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), writer, zaplog.DebugLevel)
	options := append([]zaplog.Option{
		zaplog.ErrorOutput(writer.WithMarkFailed(true)),
		zaplog.AddStacktrace(zaplog.ErrorLevel),
		zaplog.AddCaller(),
	}, opts...)
	return Wrap(t, mode, zaplog.New(core, options...))
}

// Wrap observes log calls before writing them to the supplied Zap logger.
func Wrap(t testlogger.TestingT, mode testlogger.Mode, logger *zaplog.Logger) *Logger {
	// Skip the public adapter, engine, and output adapter.
	return &Logger{testlogger.NewEngine[Field](t, mode,
		output{logger.WithOptions(zaplog.AddCallerSkip(3))}, testlogger.CallerSkip[Field](1))}
}

func fields(values []zaplog.Field) []Field {
	if values == nil {
		return nil
	}
	result := make([]Field, 0, len(values))
	for _, value := range values {
		switch value.Type {
		case zapcore.NamespaceType, zapcore.InlineMarshalerType:
			//nolint:forbidigo // Panics are fine in testing code.
			panic("testlogger/zap: namespace and inline modifiers are unsupported")
		case zapcore.SkipType:
			continue
		default:
			result = append(result, Field{value})
		}
	}
	return result
}

func zapFields(values []Field) []zaplog.Field {
	if values == nil {
		return nil
	}
	result := make([]zaplog.Field, len(values))
	for i, value := range values {
		result[i] = value.Field
	}
	return result
}

// Expect matches the level, message, and Zap fields using the engine's expectation rules.
func (l *Logger) Expect(level testlogger.Level, message string, values ...zaplog.Field) *testlogger.Expectation[Field] {
	return l.Engine.Expect(level, message, fields(values)...)
}

// StartCapture records log calls containing any supplied field, or all calls when none are supplied.
func (l *Logger) StartCapture(values ...zaplog.Field) *testlogger.Capture[Field] {
	return l.Engine.StartCapture(fields(values)...)
}

// With returns a logger with additional fields and the same expectations and captures.
func (l *Logger) With(values ...zaplog.Field) *Logger {
	return &Logger{l.Engine.With(fields(values)...)}
}

type output struct{ *zaplog.Logger }

func (l *Logger) Debug(message string, values ...zaplog.Field) {
	l.Engine.Debug(message, fields(values)...)
}

func (l output) Debug(message string, values ...Field) {
	l.Logger.Debug(message, zapFields(values)...)
}

func (l *Logger) Info(message string, values ...zaplog.Field) {
	l.Engine.Info(message, fields(values)...)
}

func (l output) Info(message string, values ...Field) {
	l.Logger.Info(message, zapFields(values)...)
}

func (l *Logger) Warn(message string, values ...zaplog.Field) {
	l.Engine.Warn(message, fields(values)...)
}

func (l output) Warn(message string, values ...Field) {
	l.Logger.Warn(message, zapFields(values)...)
}

func (l *Logger) Error(message string, values ...zaplog.Field) {
	l.T().Helper()
	l.Engine.Error(message, fields(values)...)
}

func (l output) Error(message string, values ...Field) {
	l.Logger.Error(message, zapFields(values)...)
}

func (l *Logger) DPanic(message string, values ...zaplog.Field) {
	l.T().Helper()
	l.Engine.DPanic(message, fields(values)...)
}

func (l output) DPanic(message string, values ...Field) {
	l.Logger.WithOptions(zaplog.WithPanicHook(dpanicHook{})).DPanic(message, zapFields(values)...)
}

func (l *Logger) Panic(message string, values ...zaplog.Field) {
	l.T().Helper()
	l.Engine.Panic(message, fields(values)...)
}

func (l output) Panic(message string, values ...Field) {
	l.Logger.Panic(message, zapFields(values)...)
}

func (l *Logger) Fatal(message string, values ...zaplog.Field) {
	l.T().Helper()
	l.Engine.Fatal(message, fields(values)...)
}

func (l output) Fatal(message string, values ...Field) {
	l.Logger.Fatal(message, zapFields(values)...)
}

// The engine decides whether DPanic fails the test; Zap's development-mode panic must not bypass it.
type dpanicHook struct{}

func (dpanicHook) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {}
