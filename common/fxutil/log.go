package fxutil

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

var LogAdapter = fx.WithLogger(func(logger log.Logger) fxevent.Logger {
	return &fxLogAdapter{logger: logger}
})

type fxLogAdapter struct {
	logger log.Logger
}

func (l *fxLogAdapter) LogEvent(e fxevent.Event) {
	switch e := e.(type) {
	case *fxevent.OnStartExecuting:
		l.logger.Debug("OnStart hook executing",
			tag.ComponentFX,
			tag.NewStringTag("callee", e.FunctionName),
			tag.NewStringTag("caller", e.CallerName),
		)
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.logger.Error("OnStart hook failed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.Error(e.Err),
			)
		} else {
			l.logger.Debug("OnStart hook executed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.NewStringerTag("runtime", e.Runtime),
			)
		}
	case *fxevent.OnStopExecuting:
		l.logger.Debug("OnStop hook executing",
			tag.ComponentFX,
			tag.NewStringTag("callee", e.FunctionName),
			tag.NewStringTag("caller", e.CallerName),
		)
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.logger.Error("OnStop hook failed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.Error(e.Err),
			)
		} else {
			l.logger.Debug("OnStop hook executed",
				tag.ComponentFX,
				tag.NewStringTag("callee", e.FunctionName),
				tag.NewStringTag("caller", e.CallerName),
				tag.NewStringerTag("runtime", e.Runtime),
			)
		}
	case *fxevent.Supplied:
		if e.Err != nil {
			l.logger.Error("supplied",
				tag.ComponentFX,
				tag.NewStringTag("type", e.TypeName),
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Provided:
		if e.Err != nil {
			l.logger.Error("error encountered while applying options",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Replaced:
		if e.Err != nil {
			l.logger.Error("error encountered while replacing",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Decorated:
		if e.Err != nil {
			l.logger.Error("error encountered while applying options",
				tag.ComponentFX,
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err))
		}
	case *fxevent.Run:
		if e.Err != nil {
			l.logger.Error("error returned",
				tag.ComponentFX,
				tag.NewStringTag("name", e.Name),
				tag.NewStringTag("kind", e.Kind),
				tag.NewStringTag("module", e.ModuleName),
				tag.Error(e.Err),
			)
		}
	case *fxevent.Invoking:
		// Do not log stack as it will make logs hard to read.
		l.logger.Debug("invoking",
			tag.ComponentFX,
			tag.NewStringTag("function", e.FunctionName),
			tag.NewStringTag("module", e.ModuleName),
		)
	case *fxevent.Invoked:
		if e.Err != nil {
			l.logger.Error("invoke failed",
				tag.ComponentFX,
				tag.Error(e.Err),
				tag.NewStringTag("stack", e.Trace),
				tag.NewStringTag("function", e.FunctionName),
				tag.NewStringTag("module", e.ModuleName),
			)
		}
	case *fxevent.Stopping:
		l.logger.Info("received signal",
			tag.ComponentFX,
			tag.NewStringerTag("signal", e.Signal))
	case *fxevent.Stopped:
		if e.Err != nil {
			l.logger.Error("stop failed", tag.ComponentFX, tag.Error(e.Err))
		}
	case *fxevent.RollingBack:
		l.logger.Error("start failed, rolling back", tag.ComponentFX, tag.Error(e.StartErr))
	case *fxevent.RolledBack:
		if e.Err != nil {
			l.logger.Error("rollback failed", tag.ComponentFX, tag.Error(e.Err))
		}
	case *fxevent.Started:
		if e.Err != nil {
			l.logger.Error("start failed", tag.ComponentFX, tag.Error(e.Err))
		} else {
			l.logger.Debug("started", tag.ComponentFX)
		}
	case *fxevent.LoggerInitialized:
		if e.Err != nil {
			l.logger.Error("custom logger initialization failed", tag.ComponentFX, tag.Error(e.Err))
		} else {
			l.logger.Debug("initialized custom fxevent.Logger",
				tag.ComponentFX,
				tag.NewStringTag("function", e.ConstructorName))
		}
	default:
		l.logger.Warn("unknown fx log type, update fxLogAdapter",
			tag.ComponentFX,
			tag.ValueType(e),
		)
	}
}
