package workflow

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(GetTaskGeneratorProvider),
	fx.Invoke(populateTaskGeneratorProvider),
	fx.Provide(RelocatableAttributesFetcherProvider),
	fx.Invoke(RegisterStateMachine),
)
