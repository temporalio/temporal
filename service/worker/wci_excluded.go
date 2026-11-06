//go:build temporal_minimal && !temporal_with_worker_controller

package worker

import (
	"fmt"

	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/fx"
)

var wciComponentModule = fx.Invoke(func(dc *dynamicconfig.Collection) error {
	if wciclient.WorkerControllerEnabled.Get(dc)("") {
		return fmt.Errorf("%s is enabled but worker controller is not compiled into this server; rebuild with the temporal_with_worker_controller build tag", wciclient.WorkerControllerEnabled.Key())
	}
	return nil
})
