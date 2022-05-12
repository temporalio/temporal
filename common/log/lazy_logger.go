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
	"sync"

	"go.temporal.io/server/common/log/tag"
)

var _ Logger = (*lazyLogger)(nil)

type (
	lazyLogger struct {
		logger Logger
		tagFn  func() []tag.Tag

		once sync.Once
	}
)

func NewLazyLogger(logger Logger, tagFn func() []tag.Tag) *lazyLogger {
	return &lazyLogger{
		logger: logger,
		tagFn:  tagFn,
	}
}

func (l *lazyLogger) Debug(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Debug(msg, tags...)
}

func (l *lazyLogger) Info(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Info(msg, tags...)
}

func (l *lazyLogger) Warn(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Warn(msg, tags...)
}

func (l *lazyLogger) Error(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Error(msg, tags...)
}

func (l *lazyLogger) Fatal(msg string, tags ...tag.Tag) {
	l.once.Do(l.tagLogger)
	l.logger.Fatal(msg, tags...)
}

func (l *lazyLogger) tagLogger() {
	l.logger = With(l.logger, l.tagFn()...)
}
