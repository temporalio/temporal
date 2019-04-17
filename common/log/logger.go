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

package log

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/uber/cadence/common/log/tag"
	"go.uber.org/zap"
)

// Logger is our abstraction for logging
// Usage examples:
//  import "github.com/uber/cadence/common/log/tag"
//  1) logger = logger.WithTags(
//          tag.WorkflowNextEventID( 123),
//          tag.WorkflowActionWorkflowStarted,
//          tag.WorkflowDomainID("test-domain-id"))
//     logger.Info("hello world")
//  2) logger.Info("hello world",
//          tag.WorkflowNextEventID( 123),
//          tag.WorkflowActionWorkflowStarted,
//          tag.WorkflowDomainID("test-domain-id"))
//	   )
//  Note: msg should be static, it is not recommended to use fmt.Sprintf() for msg.
//        Anything dynamic should be tagged.
type Logger interface {
	Debug(msg string, tags ...tag.Tag)
	Info(msg string, tags ...tag.Tag)
	Warn(msg string, tags ...tag.Tag)
	Error(msg string, tags ...tag.Tag)
	Fatal(msg string, tags ...tag.Tag)
	WithTags(tags ...tag.Tag) Logger
}

type loggerImpl struct {
	zapLogger *zap.Logger
	skip      int
}

var _ Logger = (*loggerImpl)(nil)

const skipForDefaultLogger = 3

// NewLogger returns a new logger
func NewLogger(zapLogger *zap.Logger) Logger {
	return &loggerImpl{
		zapLogger: zapLogger,
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

func (lg *loggerImpl) buildFieldsWithCallat(tags []tag.Tag) []zap.Field {
	fs := lg.buildFields(tags)
	fs = append(fs, zap.String(tag.LoggingCallAtKey, caller(lg.skip)))
	return fs
}

func (lg *loggerImpl) buildFields(tags []tag.Tag) []zap.Field {
	fs := make([]zap.Field, 0, len(tags))
	for _, t := range tags {
		f := t.Field()
		if f.Key == "" {
			// ignore empty field(which can be constructed manually)
			continue
		}
		fs = append(fs, f)
	}
	return fs
}

func (lg *loggerImpl) Debug(msg string, tags ...tag.Tag) {
	fields := lg.buildFieldsWithCallat(tags)
	lg.zapLogger.Debug(msg, fields...)
}

func (lg *loggerImpl) Info(msg string, tags ...tag.Tag) {
	fields := lg.buildFieldsWithCallat(tags)
	lg.zapLogger.Info(msg, fields...)
}

func (lg *loggerImpl) Warn(msg string, tags ...tag.Tag) {
	fields := lg.buildFieldsWithCallat(tags)
	lg.zapLogger.Warn(msg, fields...)
}

func (lg *loggerImpl) Error(msg string, tags ...tag.Tag) {
	fields := lg.buildFieldsWithCallat(tags)
	lg.zapLogger.Error(msg, fields...)
}

func (lg *loggerImpl) Fatal(msg string, tags ...tag.Tag) {
	fields := lg.buildFieldsWithCallat(tags)
	lg.zapLogger.Fatal(msg, fields...)
}

func (lg *loggerImpl) WithTags(tags ...tag.Tag) Logger {
	fields := lg.buildFields(tags)
	zapLogger := lg.zapLogger.With(fields...)
	return &loggerImpl{
		zapLogger: zapLogger,
		skip:      lg.skip,
	}
}
