package grpcinject

import (
	"go.uber.org/fx"
)

var Module = fx.Provide(NewInterceptor)
