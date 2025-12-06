package serialization

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(NewSerializer),
	fx.Provide(func(s Serializer) TaskSerializer { return s }),
)
