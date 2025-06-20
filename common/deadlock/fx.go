package deadlock

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewDeadlockDetector),
	fx.Invoke(func(lc fx.Lifecycle, dd *deadlockDetector) {
		lc.Append(fx.StartStopHook(dd.Start, dd.Stop))
	}),
)
