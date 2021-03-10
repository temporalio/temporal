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

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"go.temporal.io/server/common/log/tag"
)

const (
	skipForDefaultLogger = 3
	// we put a default message when it is empty so that the log can be searchable/filterable
	defaultMsgForEmpty = "none"
)

type (
	loggerImpl struct {
		zapLogger *zap.Logger
		skip      int
	}
)

// NewNopLogger returns a no-op logger
func NewNopLogger() Logger {
	return &loggerImpl{
		zapLogger: zap.NewNop(),
	}
}

// TODO (alex): remove
// NewDevelopmentForTest is a helper to create new development logger in unit test
func NewDevelopmentForTest(s suite.Suite) Logger {
	logger, err := NewDevelopment()
	if err != nil {
		panic(err)
	}
	return logger
}

// TODO (alex): remove error
// NewDevelopment returns a logger at debug level and log into STDERR
func NewDevelopment() (Logger, error) {
	cfg := &Config{
		Level: "debug",
	}
	return NewLogger(cfg), nil
}

// NewLogger returns a new logger
func NewLogger(cfg *Config) Logger {
	return &loggerImpl{
		zapLogger: newZapLogger(cfg),
		skip:      skipForDefaultLogger,
	}
}

func caller(skip int) string {
	_, path, lineno, ok := runtime.Caller(skip)
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v:%v", filepath.Base(path), lineno)
}

func (l *loggerImpl) buildFieldsWithCallat(tags []tag.Tag) []zap.Field {
	fs := l.buildFields(tags)
	fs = append(fs, zap.String(tag.LoggingCallAtKey, caller(l.skip)))
	return fs
}

func (l *loggerImpl) buildFields(tags []tag.Tag) []zap.Field {
	fs := make([]zap.Field, 0, len(tags))
	for _, t := range tags {
		f := t.Field
		if f.Key == "" {
			// ignore empty field(which can be constructed manually)
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

func (l *loggerImpl) Debug(msg string, tags ...tag.Tag) {
	msg = setDefaultMsg(msg)
	fields := l.buildFieldsWithCallat(tags)
	l.zapLogger.Debug(msg, fields...)
}

func (l *loggerImpl) Info(msg string, tags ...tag.Tag) {
	msg = setDefaultMsg(msg)
	fields := l.buildFieldsWithCallat(tags)
	l.zapLogger.Info(msg, fields...)
}

func (l *loggerImpl) Warn(msg string, tags ...tag.Tag) {
	msg = setDefaultMsg(msg)
	fields := l.buildFieldsWithCallat(tags)
	l.zapLogger.Warn(msg, fields...)
}

func (l *loggerImpl) Error(msg string, tags ...tag.Tag) {
	msg = setDefaultMsg(msg)
	fields := l.buildFieldsWithCallat(tags)
	l.zapLogger.Error(msg, fields...)
}

func (l *loggerImpl) Fatal(msg string, tags ...tag.Tag) {
	msg = setDefaultMsg(msg)
	fields := l.buildFieldsWithCallat(tags)
	l.zapLogger.Fatal(msg, fields...)
}

func (l *loggerImpl) WithTags(tags ...tag.Tag) Logger {
	fields := l.buildFields(tags)
	zapLogger := l.zapLogger.With(fields...)
	return &loggerImpl{
		zapLogger: zapLogger,
		skip:      l.skip,
	}
}
