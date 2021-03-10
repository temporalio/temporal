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
	"go.temporal.io/server/common/log/tag"
)

type withLogger struct {
	logger Logger
	tags   []tag.Tag
}

var _ Logger = (*withLogger)(nil)

// With returns Logger instance that prepend every log entry with tags. If logger implements WithLogger it is used, otherwise every log call will be intercepted.
func With(logger Logger, tags ...tag.Tag) Logger {
	if wl, ok := logger.(WithLogger); ok {
		return wl.With(tags...)
	}

	return newWithLogger(logger, tags...)
}

func newWithLogger(logger Logger, tags ...tag.Tag) *withLogger {
	return &withLogger{logger: logger, tags: tags}
}

func (l *withLogger) prependTags(tags []tag.Tag) []tag.Tag {
	return append(l.tags, tags...)
}

// Debug writes message to the log.
func (l *withLogger) Debug(msg string, tags ...tag.Tag) {
	l.logger.Debug(msg, l.prependTags(tags)...)
}

// Info writes message to the log.
func (l *withLogger) Info(msg string, tags ...tag.Tag) {
	l.logger.Info(msg, l.prependTags(tags)...)
}

// Warn writes message to the log.
func (l *withLogger) Warn(msg string, tags ...tag.Tag) {
	l.logger.Warn(msg, l.prependTags(tags)...)
}

// Error writes message to the log.
func (l *withLogger) Error(msg string, tags ...tag.Tag) {
	l.logger.Error(msg, l.prependTags(tags)...)
}

// Error writes message to the log.
func (l *withLogger) Fatal(msg string, tags ...tag.Tag) {
	l.logger.Fatal(msg, l.prependTags(tags)...)
}
