//go:build !gomad

package worker

import (
	wcicomponent "go.temporal.io/auto-scaled-workers/wci/workercomponent"
	"go.uber.org/fx"
)

var wciComponentModule fx.Option = wcicomponent.Module
