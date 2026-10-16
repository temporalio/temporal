package wci

import (
	"go.temporal.io/server/common/dynamicconfig"
)

var (
	InProcessComputeProviders = dynamicconfig.NewGlobalTypedSetting(
		"workercontroller.chasm.in_process_compute_providers",
		[]string{"test-invoke", "test-worker-set"},
		`InProcessComputeProviders are the provider types a CHASM WCI runs inside history. They must need no
assumed identity; "subprocess" executes on the history host and is for local development only.`,
	)
	ComputeConcurrencyPerNamespace = dynamicconfig.NewNamespaceIntSetting(
		"workercontroller.chasm.compute_concurrency_per_namespace",
		10,
		`ComputeConcurrencyPerNamespace caps concurrent provider calls per namespace on a history host, so a
slow provider cannot starve task processing.`,
	)
)
