package dynamicconfig

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

// collectionParams are the inputs to the *Collection provider. Evaluator and ServiceName are
// optional because the expression evaluator is opt-in, and because Module is also used in the
// top level graph, which has no single service name.
type collectionParams struct {
	fx.In

	Client      Client
	Logger      log.Logger
	Lifecycle   fx.Lifecycle
	Evaluator   Evaluator              `optional:"true"`
	ServiceName primitives.ServiceName `optional:"true"`
}

var Module = fx.Options(
	fx.Provide(func(p collectionParams) *Collection {
		col := NewCollectionForService(p.Client, p.Logger, p.Evaluator, string(p.ServiceName))
		p.Lifecycle.Append(fx.StartStopHook(col.Start, col.Stop))

		// Route expression config reloads to this Collection's subscribers, the same way
		// Collection.Start routes file config reloads.
		if ne, ok := p.Evaluator.(NotifyingEvaluator); ok {
			cancel := ne.Subscribe(col.EvaluatorKeysChanged)
			p.Lifecycle.Append(fx.StopHook(cancel))
		}
		return col
	}),
	fx.Provide(fx.Annotate(
		func(c *Collection) pingable.Pingable { return c },
		fx.ResultTags(`group:"deadlockDetectorRoots"`),
	)),
)
