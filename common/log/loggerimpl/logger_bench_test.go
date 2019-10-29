// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
package loggerimpl

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uber/cadence/common/log/tag"
)

/**
longer@~/gocode/src/github.com/uber/cadence/common/log:(log)$ go test -v -bench=. | egrep "(Bench)|(ns/op)"
BenchmarkZapLoggerWithFields-8      	{"level":"info","ts":1555094254.794006,"caller":"log/logger_bench_test.go:21","msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-domain-name":"test-domain-name"}
  200000	      8609 ns/op
BenchmarkLoggerWithFields-8         	{"level":"info","ts":1555094256.608516,"msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-domain-name":"test-domain-name","logging-call-at":"logger_bench_test.go:36"}
  200000	      8773 ns/op
BenchmarkZapLoggerWithoutFields-8   	{"level":"info","ts":1555094258.4521542,"caller":"log/logger_bench_test.go:49","msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","wf-domain-name":"test-domain-name"}
  200000	      8535 ns/op
BenchmarkLoggerWithoutFields-8      	{"level":"info","ts":1555094260.2499342,"msg":"msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890","wf-domain-name":"test-domain-name","wf-schedule-id":0,"cluster-name":"this is a very long value: 1234567890 1234567890 1234567890 1234567890","logging-call-at":"logger_bench_test.go:64"}
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
			zap.String("wf-domain-name", "test-domain-name"))
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
			tag.WorkflowDomainName("test-domain-name"))
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
			zap.String("wf-domain-name", "test-domain-name"))
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
			tag.WorkflowDomainName("test-domain-name"),
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
