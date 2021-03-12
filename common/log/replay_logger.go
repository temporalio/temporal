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
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
)

const extraSkipForReplayLogger = 1

type (
	replayLogger struct {
		logger            Logger
		ctx               workflow.Context
		enableLogInReplay bool
	}
)

var _ Logger = (*replayLogger)(nil)

// NewReplayLogger creates a logger which is aware of Temporal replay mode
func NewReplayLogger(logger Logger, ctx workflow.Context, enableLogInReplay bool) *replayLogger {
	if sl, ok := logger.(SkipLogger); ok {
		logger = sl.Skip(extraSkipForReplayLogger)
	}
	return &replayLogger{
		logger:            logger,
		ctx:               ctx,
		enableLogInReplay: enableLogInReplay,
	}
}

func (r *replayLogger) Debug(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Debug(msg, tags...)
}

func (r *replayLogger) Info(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Info(msg, tags...)
}

func (r *replayLogger) Warn(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Warn(msg, tags...)
}

func (r *replayLogger) Error(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Error(msg, tags...)
}

func (r *replayLogger) Fatal(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Fatal(msg, tags...)
}

func (r *replayLogger) With(tags ...tag.Tag) Logger {
	return &replayLogger{
		logger:            newWithLogger(r.logger, tags...),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}
