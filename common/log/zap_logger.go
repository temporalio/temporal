// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package log

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.temporal.io/server/common/log/tag"
)

const (
	skipForDefaultLogger = 3
	// we put a default message when it is empty so that the log can be searchable/filterable
	defaultMsgForEmpty = "none"
)

type (
	zapLogger struct {
		zl   *zap.Logger
		skip int
	}
)

var _ Logger = (*zapLogger)(nil)

// NewDevelopment returns a logger at debug level and log into STDERR
func NewDevelopment() *zapLogger {
	return NewLogger(&Config{
		Level: "debug",
	})
}

// NewLogger returns a new logger
func NewLogger(cfg *Config) *zapLogger {
	return newLogger(buildZapLogger(cfg))
}

func newLogger(zl *zap.Logger) *zapLogger {
	return &zapLogger{
		zl:   zl,
		skip: skipForDefaultLogger,
	}
}

func caller(skip int) string {
	_, path, lineno, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v:%v", filepath.Base(path), lineno)
}

func (l *zapLogger) buildFieldsWithCallat(tags []tag.Tag) []zap.Field {
	fs := l.buildFields(tags)
	fs = append(fs, zap.String(tag.LoggingCallAtKey, caller(l.skip)))
	return fs
}

func (l *zapLogger) buildFields(tags []tag.Tag) []zap.Field {
	fs := make([]zap.Field, 0, len(tags))
	for _, t := range tags {
		var f zap.Field
		if zt, ok := t.(tag.ZapTag); ok {
			f = zt.Field()
		} else {
			f = zap.Any(t.Key(), t.Value())
		}

		if f.Key == "" {
			continue
		}
		fs = append(fs, f)
	}
	return fs
}

func setDefaultMsg(msg string) string {
	if msg == "" {
		return defaultMsgForEmpty
	}
	return msg
}

func (l *zapLogger) Debug(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.DebugLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallat(tags)
		l.zl.Debug(msg, fields...)
	}
}

func (l *zapLogger) Info(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.InfoLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallat(tags)
		l.zl.Info(msg, fields...)
	}
}

func (l *zapLogger) Warn(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.WarnLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallat(tags)
		l.zl.Warn(msg, fields...)
	}
}

func (l *zapLogger) Error(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.ErrorLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallat(tags)
		l.zl.Error(msg, fields...)
	}
}

func (l *zapLogger) Fatal(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.FatalLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallat(tags)
		l.zl.Fatal(msg, fields...)
	}
}

func (l *zapLogger) With(tags ...tag.Tag) Logger {
	fields := l.buildFields(tags)
	zl := l.zl.With(fields...)
	return &zapLogger{
		zl:   zl,
		skip: l.skip,
	}
}

func (l *zapLogger) Skip(extraSkip int) Logger {
	return &zapLogger{
		zl:   l.zl,
		skip: l.skip + extraSkip,
	}
}

// buildZapLogger builds and returns a new zap.Logger for this logging configuration
func buildZapLogger(cfg *Config) *zap.Logger {
	encodeConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      zapcore.OmitKey, // we use our own caller
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}

	outputPath := "stderr"
	if len(cfg.OutputFile) > 0 {
		outputPath = cfg.OutputFile
	}
	if cfg.Stdout {
		outputPath = "stdout"
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(parseZapLevel(cfg.Level)),
		Development:      false,
		Sampling:         nil,
		Encoding:         "json",
		EncoderConfig:    encodeConfig,
		OutputPaths:      []string{outputPath},
		ErrorOutputPaths: []string{outputPath},
		DisableCaller:    true,
	}
	logger, _ := config.Build()
	return logger
}

func parseZapLevel(level string) zapcore.Level {
	switch strings.ToLower(level) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "fatal":
		return zap.FatalLevel
	default:
		return zap.InfoLevel
	}
}
