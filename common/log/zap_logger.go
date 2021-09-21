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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.temporal.io/server/common/log/tag"
)

const (
	skipForZapLogger = 3
	// we put a default message when it is empty so that the log can be searchable/filterable
	defaultMsgForEmpty = "none"
)

type (
	// zapLogger is logger backed up by zap.Logger.
	zapLogger struct {
		zl   *zap.Logger
		skip int
	}
)

var _ Logger = (*zapLogger)(nil)

// NewTestLogger returns a logger at debug level and log into STDERR
func NewTestLogger() *zapLogger {
	return NewZapLogger(BuildZapLogger(Config{
		Level: "debug",
	}))
}

// NewCLILogger returns a logger at debug level and log into STDERR for logging from within CLI tools
func NewCLILogger() *zapLogger {
	return NewZapLogger(buildCLIZapLogger())
}

// NewZapLogger returns a new zap based logger from zap.Logger
func NewZapLogger(zl *zap.Logger) *zapLogger {
	return &zapLogger{
		zl:   zl,
		skip: skipForZapLogger,
	}
}

// BuildZapLogger builds and returns a new zap.Logger for this logging configuration
func BuildZapLogger(cfg Config) *zap.Logger {
	return buildZapLogger(cfg, true)
}

func caller(skip int) string {
	_, path, line, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	return filepath.Base(path) + ":" + strconv.Itoa(line)
}

func (l *zapLogger) buildFieldsWithCallAt(tags []tag.Tag) []zap.Field {
	fields := make([]zap.Field, len(tags)+1)
	l.fillFields(tags, fields)
	fields[len(fields)-1] = zap.String(tag.LoggingCallAtKey, caller(l.skip))
	return fields
}

// fillFields fill fields parameter with fields read from tags. Optimized for performance.
func (l *zapLogger) fillFields(tags []tag.Tag, fields []zap.Field) {
	for i, t := range tags {
		if zt, ok := t.(tag.ZapTag); ok {
			fields[i] = zt.Field()
		} else {
			fields[i] = zap.Any(t.Key(), t.Value())
		}
	}
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
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Debug(msg, fields...)
	}
}

func (l *zapLogger) Info(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.InfoLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Info(msg, fields...)
	}
}

func (l *zapLogger) Warn(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.WarnLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Warn(msg, fields...)
	}
}

func (l *zapLogger) Error(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.ErrorLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Error(msg, fields...)
	}
}

func (l *zapLogger) Fatal(msg string, tags ...tag.Tag) {
	if l.zl.Core().Enabled(zap.FatalLevel) {
		msg = setDefaultMsg(msg)
		fields := l.buildFieldsWithCallAt(tags)
		l.zl.Fatal(msg, fields...)
	}
}

func (l *zapLogger) With(tags ...tag.Tag) Logger {
	fields := make([]zap.Field, len(tags))
	l.fillFields(tags, fields)
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

func buildZapLogger(cfg Config, disableCaller bool) *zap.Logger {
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
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	if disableCaller {
		encodeConfig.CallerKey = zapcore.OmitKey
		encodeConfig.EncodeCaller = nil
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
		DisableCaller:    disableCaller,
	}
	logger, _ := config.Build()
	return logger
}

func buildCLIZapLogger() *zap.Logger {
	encodeConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      zapcore.OmitKey, // we use our own caller
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   nil,
	}

	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:       false,
		DisableStacktrace: os.Getenv("TEMPORAL_CLI_SHOW_STACKS") == "",
		Sampling:          nil,
		Encoding:          "console",
		EncoderConfig:     encodeConfig,
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     true,
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
