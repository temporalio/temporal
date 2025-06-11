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

func (r *replayLogger) DPanic(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.DPanic(msg, tags...)
}

func (r *replayLogger) Panic(msg string, tags ...tag.Tag) {
	if workflow.IsReplaying(r.ctx) && !r.enableLogInReplay {
		return
	}
	r.logger.Panic(msg, tags...)
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
