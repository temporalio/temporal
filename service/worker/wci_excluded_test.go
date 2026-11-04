//go:build temporal_minimal && !temporal_with_worker_controller

package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

func TestWorkerControllerExcludedConfig(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{wciclient.WorkerControllerEnabled.Key(): enabled}, log.NewNoopLogger())
			app := fx.New(fx.NopLogger, fx.Supply(dc), wciComponentModule)
			if enabled {
				require.ErrorContains(t, app.Err(), "temporal_with_worker_controller")
			} else {
				require.NoError(t, app.Err())
			}
		})
	}
}
