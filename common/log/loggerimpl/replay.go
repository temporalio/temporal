package loggerimpl

import (
	"go.temporal.io/temporal/workflow"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

type replayLogger struct {
	logger            log.Logger
	ctx               workflow.Context
	enableLogInReplay bool
}

var _ log.Logger = (*replayLogger)(nil)

const skipForReplayLogger = skipForDefaultLogger + 1

// NewReplayLogger creates a logger which is aware of temporal's replay mode
func NewReplayLogger(logger log.Logger, ctx workflow.Context, enableLogInReplay bool) log.Logger {
	lg, ok := logger.(*loggerImpl)
	if ok {
		logger = &loggerImpl{
			zapLogger: lg.zapLogger,
			skip:      skipForReplayLogger,
		}
	} else {
		logger.Warn("ReplayLogger may not emit callat tag correctly because the logger passed in is not loggerImpl")
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

func (r *replayLogger) WithTags(tags ...tag.Tag) log.Logger {
	return &replayLogger{
		logger:            r.logger.WithTags(tags...),
		ctx:               r.ctx,
		enableLogInReplay: r.enableLogInReplay,
	}
}
