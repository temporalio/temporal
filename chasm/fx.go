package chasm

import "go.uber.org/fx"

var Module = fx.Module(
	"chasm",
	fx.Provide(NewRegistry),
)
