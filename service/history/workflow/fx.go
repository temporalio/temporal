package workflow

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func() TaskGeneratorProvider { return defaultTaskGeneratorProvider }),
	fx.Invoke(populateTaskGeneratorProvider),
	fx.Provide(RelocatableAttributesFetcherProvider),
	fx.Invoke(RegisterStateMachine),
)
