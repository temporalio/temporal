package wci

import (
	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
)

var (
	InProcessComputeProviders = dynamicconfig.NewGlobalTypedSettingWithConverter(
		"workercontroller.chasm.in_process_compute_providers",
		convertStringList,
		[]string{"test-invoke", "test-worker-set"},
		`InProcessComputeProviders are the provider types a CHASM WCI runs inside history. They must need no
assumed identity; "subprocess" executes on the history host and is for local development only.`,
	)
	RemoteComputeProviders = dynamicconfig.NewGlobalTypedSetting(
		"workercontroller.chasm.remote_compute_providers",
		[]string(nil),
		`RemoteComputeProviders are the provider types a CHASM WCI calls on the WCI process, under its
cloud identity. In-process providers take precedence; a type in neither list stays a workflow.`,
	)
	ComputeConcurrencyPerNamespace = dynamicconfig.NewNamespaceIntSetting(
		"workercontroller.chasm.compute_concurrency_per_namespace",
		10,
		`ComputeConcurrencyPerNamespace caps concurrent provider calls per namespace on a history host, so a
slow provider cannot starve task processing.`,
	)
	WorkflowTaskQueue = dynamicconfig.NewGlobalStringSetting(
		"workercontroller.chasm.workflow_task_queue",
		primitives.PerNSWorkerTaskQueue,
		`WorkflowTaskQueue is where a WCI migrated back to the workflow backend is started. It must match
the WCI client's task queue (cloud uses temporal-sys-worker-controller-per-ns-tq).`,
	)
)

// convertStringList makes a configured list replace the default: NewGlobalTypedSetting merges over a
// copy of it, so ["subprocess"] would become [subprocess test-worker-set].
func convertStringList(in any) ([]string, error) {
	var out []string
	err := mapstructure.Decode(in, &out)
	return out, err
}
