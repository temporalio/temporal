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
	"testing"

	"go.uber.org/zap"

	"go.temporal.io/server/common/log/tag"
)

/**
$ go test -v -bench=. | grep -E "(Bench)|(ns/op)"
BenchmarkZapLoggerWithFields
BenchmarkZapLoggerWithFields-4            152793              7200 ns/op
BenchmarkLoggerWithFields
BenchmarkLoggerWithFields-4               146850              8370 ns/op
BenchmarkZapLoggerWithoutFields
BenchmarkZapLoggerWithoutFields-4         192972              5885 ns/op
BenchmarkLoggerWithoutFields
BenchmarkLoggerWithoutFields-4            162109              7211 ns/op
*/

func BenchmarkZapLoggerWithFields(b *testing.B) {
	zLogger := buildZapLogger(Config{Level: "info"}, false)

	for i := 0; i < b.N; i++ {
		zLoggerWith := zLogger.With(zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		zLoggerWith.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.String("wf-namespace", "test-namespace"))
		zLoggerWith.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.String("wf-namespace", "test-namespace"))
	}
}

func BenchmarkLoggerWithFields(b *testing.B) {
	logger := NewZapLogger(buildZapLogger(Config{Level: "info"}, true))

	for i := 0; i < b.N; i++ {
		loggerWith := logger.With(tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		loggerWith.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"))
		loggerWith.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"))
	}
}

func BenchmarkZapLoggerWithoutFields(b *testing.B) {
	zLogger := buildZapLogger(Config{Level: "info"}, false)

	for i := 0; i < b.N; i++ {
		zLogger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"),
			zap.String("wf-namespace", "test-namespace"))
		zLogger.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			zap.Int64("wf-schedule-id", int64(i)), zap.String("cluster-name", "this is a very long value: 1234567890 1234567890 1234567890 1234567890"),
			zap.String("wf-namespace", "test-namespace"))
	}
}

func BenchmarkLoggerWithoutFields(b *testing.B) {
	logger := NewZapLogger(buildZapLogger(Config{Level: "info"}, true))

	for i := 0; i < b.N; i++ {
		logger.Info("msg to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"),
			tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
		logger.Debug("msg NOT to print log, 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890 1234567890",
			tag.WorkflowNamespace("test-namespace"),
			tag.WorkflowScheduledEventID(int64(i)), tag.ClusterName("this is a very long value: 1234567890 1234567890 1234567890 1234567890"))
	}
}
