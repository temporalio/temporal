package worker

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/dynamicconfig"
)

var WorkerPerNamespaceWorkerOptions = dynamicconfig.NewNamespaceTypedSetting(
	"worker.perNamespaceWorkerOptions",
	sdkworker.Options{},
	`WorkerPerNamespaceWorkerOptions are SDK worker options for per-namespace workers`,
)
