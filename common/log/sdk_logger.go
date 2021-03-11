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

	"go.temporal.io/sdk/log"

	"go.temporal.io/server/common/log/tag"
)

const extraSkipForSdkLogger = 1

type SdkLogger struct {
	logger Logger
}

var _ log.Logger = (*SdkLogger)(nil)

func NewSdkLogger(logger Logger) *SdkLogger {
	if sl, ok := logger.(SkipLogger); ok {
		logger = sl.Skip(extraSkipForSdkLogger)
	}

	return &SdkLogger{
		logger: logger,
	}
}

func (l *SdkLogger) tags(keyvals []interface{}) []tag.Tag {
	if len(keyvals)%2 != 0 {
		return []tag.Tag{tag.Error(fmt.Errorf("odd number of keyvals pairs: %v", keyvals))}
	}

	var tags []tag.Tag
	for i := 0; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyvals[i])
		}
		tags = append(tags, tag.NewAnyTag(key, keyvals[i+1]))
	}

	return tags
}

func (l *SdkLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.Debug(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.Info(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Warn(msg string, keyvals ...interface{}) {
	l.logger.Warn(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.Error(msg, l.tags(keyvals)...)
}

func (l *SdkLogger) With(keyvals ...interface{}) log.Logger {
	return NewSdkLogger(
		With(l.logger, l.tags(keyvals)...))
}
