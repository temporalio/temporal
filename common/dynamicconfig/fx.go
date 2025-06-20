package dynamicconfig

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/pingable"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func(client Client, logger log.Logger, lc fx.Lifecycle) *Collection {
		col := NewCollection(client, logger)
		lc.Append(fx.StartStopHook(col.Start, col.Stop))
		return col
	}),
	fx.Provide(fx.Annotate(
		func(c *Collection) pingable.Pingable { return c },
		fx.ResultTags(`group:"deadlockDetectorRoots"`),
	)),
)
