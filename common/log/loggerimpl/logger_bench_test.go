package loggerimpl

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/temporalio/temporal/common/log/tag"
)

/**
longer@~/gocode/src/github.com/temporalio/temporal/common/log:(log)$ go test -v -bench=. | egrep "(Bench)|(ns/op)"
BenchmarkZapLoggerWithFields-8      	{"level":"info","ts":1555094254.794006,"caller":"log/logger_bench_test.go:21","msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-namespace":"test-namespace"}
  200000	      8609 ns/op
BenchmarkLoggerWithFields-8         	{"level":"info","ts":1555094256.608516,"msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-namespace":"test-namespace","logging-call-at":"logger_bench_test.go:36"}
  200000	      8773 ns/op
BenchmarkZapLoggerWithoutFields-8   	{"level":"info","ts":1555094258.4521542,"caller":"log/logger_bench_test.go:49","msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-namespace":"test-namespace"}
  200000	      8535 ns/op
BenchmarkLoggerWithoutFields-8      	{"level":"info","ts":1555094260.2499342,"msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-namespace":"test-namespace","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","logging-call-at":"logger_bench_test.go:64"}
  200000	      8998 ns/op
*/

func BenchmarkZapLoggerWithFields(b *testing.B) {
	zapLogger, err := buildZapLoggerForZap()
	if err != nil {
		b.Fail()
	}

	for i := 0; i < b.N; i++ {
		lg := zapLogger.With(zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		lg.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.String("wf-namespace", "test-namespace"))
	}

}

func BenchmarkLoggerWithFields(b *testing.B) {
	zapLogger, err := buildZapLoggerForDefault()
	if err != nil {
		b.Fail()
	}
	logger := NewLogger(zapLogger)

	for i := 0; i < b.N; i++ {
		lg := logger.WithTags(tag.WorkflowScheduleID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		lg.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"))
	}

}

func BenchmarkZapLoggerWithoutFields(b *testing.B) {
	zapLogger, err := buildZapLoggerForZap()
	if err != nil {
		b.Fail()
	}

	for i := 0; i < b.N; i++ {
		zapLogger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"),
			zap.String("wf-namespace", "test-namespace"))
	}

}

func BenchmarkLoggerWithoutFields(b *testing.B) {
	zapLogger, err := buildZapLoggerForDefault()
	if err != nil {
		b.Fail()
	}
	logger := NewLogger(zapLogger)

	for i := 0; i < b.N; i++ {
		logger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"),
			tag.WorkflowScheduleID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
	}
}

func buildZapLoggerForZap() (*zap.Logger, error) {
	encConfig := zap.NewProductionEncoderConfig()
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:      false,
		Sampling:         nil,
		Encoding:         "json",
		EncoderConfig:    encConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	return cfg.Build()
}

// only difference here is we disable caller since we already have another one
func buildZapLoggerForDefault() (*zap.Logger, error) {
	encConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "", // disable caller here since we already have another one
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}
	cfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:      false,
		Sampling:         nil,
		Encoding:         "json",
		EncoderConfig:    encConfig,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	return cfg.Build()
}
