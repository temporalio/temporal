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

package archiver

import (
	"github.com/uber-common/bark"
	"go.uber.org/cadence/workflow"
)

type replayBarkLogger struct {
	logger            bark.Logger
	ctx               workflow.Context
	enableLogInReplay bool
}

// NewReplayBarkLogger creates a bark logger which is aware of cadence's replay mode
func NewReplayBarkLogger(logger bark.Logger, ctx workflow.Context, enableLogInReplay bool) bark.Logger {
	return &replayBarkLogger{
		logger:            logger,
		ctx:               ctx,
		enableLogInReplay: enableLogInReplay,
	}
}

// Debug logs at debug level
func (r *replayBarkLogger) Debug(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Debug(args)
}

// Debugf logs at debug level with fmt.Printf-like formatting
func (r *replayBarkLogger) Debugf(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Debugf(format, args)
}

// Info logs at info level
func (r *replayBarkLogger) Info(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Info(args)
}

// Infof logs at info level with fmt.Printf-like formatting
func (r *replayBarkLogger) Infof(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Infof(format, args)
}

// Warn logs at warn level
func (r *replayBarkLogger) Warn(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Warn(args)
}

// Warnf logs at warn level with fmt.Printf-like formatting
func (r *replayBarkLogger) Warnf(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Warnf(format, args)
}

// Error logs at error level
func (r *replayBarkLogger) Error(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Error(args)
}

// Errorf logs at error level with fmt.Printf-like formatting
func (r *replayBarkLogger) Errorf(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Errorf(format, args)
}

// Fatal logs at fatal level, then terminate process (irrecoverable)
func (r *replayBarkLogger) Fatal(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Fatal(args)
}

// Fatalf logs at fatal level with fmt.Printf-like formatting, then terminate process (irrecoverable)
func (r *replayBarkLogger) Fatalf(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Fatalf(format, args)
}

// Panic logs at panic level, then panic (recoverable)
func (r *replayBarkLogger) Panic(args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Panic(args)
}

// Panicf logs at panic level with fmt.Printf-like formatting, then panic (recoverable)
func (r *replayBarkLogger) Panicf(format string, args ...interface{}) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Panicf(format, args)
}

// WithField returns a logger with the specified key-value pair set, to be logged in a subsequent normal logging call
func (r *replayBarkLogger) WithField(key string, value interface{}) bark.Logger {
	return &replayBarkLogger{
		logger:            r.logger.WithField(key, value),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}

// WithFields returns a logger with the specified key-value pairs set, to be included in a subsequent normal logging call
func (r *replayBarkLogger) WithFields(keyValues bark.LogFields) bark.Logger {
	return &replayBarkLogger{
		logger:            r.logger.WithFields(keyValues),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}

// WithError returns a logger with the specified error set, to be included in a subsequent normal logging call
func (r *replayBarkLogger) WithError(err error) bark.Logger {
	return &replayBarkLogger{
		logger:            r.logger.WithError(err),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}

// Fields returns map fields associated with this logger, if any (i.e. if this logger was returned from WithField[s])
// If no fields are set, returns nil
func (r *replayBarkLogger) Fields() bark.Fields {
	return r.logger.Fields()
}
