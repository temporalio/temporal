package wci

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

func TestSearchAttributes(t *testing.T) {
	values := func(w *WorkerControllerInstance) map[string]string {
		out := map[string]string{}
		for _, kv := range w.SearchAttributes(nil) {
			out[kv.Alias] = fmt.Sprint(kv.Value)
		}
		return out
	}
	newInstance := func(status wcipb.WciStatus) *WorkerControllerInstance {
		return &WorkerControllerInstance{WorkerControllerInstanceState: &wcipb.WorkerControllerInstanceState{
			DeploymentName: "dep", BuildId: "b1", Status: status,
		}}
	}

	for status, want := range map[wcipb.WciStatus]string{
		wcipb.WCI_STATUS_RUNNING:               "Running",
		wcipb.WCI_STATUS_PENDING_ACTIVATION:    "PendingActivation",
		wcipb.WCI_STATUS_MIGRATING_TO_WORKFLOW: "MigratingToWorkflow",
		wcipb.WCI_STATUS_MIGRATED:              "Migrated",
		wcipb.WCI_STATUS_DELETED:               "Deleted",
		wcipb.WCI_STATUS_MIGRATION_ABORTED:     "MigrationAborted",
	} {
		require.Equal(t, want, values(newInstance(status))["WorkerControllerInstanceStatus"], status.String())
	}

	w := newInstance(wcipb.WCI_STATUS_RUNNING)
	got := values(w)
	require.Equal(t, "dep", got["WorkerDeploymentName"])
	require.Equal(t, "dep:b1", got["WorkerDeploymentVersion"])
	require.NotContains(t, got, "WCIValidationStatus", "unset before the first validation")
	require.NotContains(t, got, "WCIComputeProviders", "no groups, no providers")

	w.ValidationStatus = &wcipb.ValidationStatus{Result: wcipb.VALIDATION_RESULT_FAILED}
	w.ScalingGroupSpecs = map[string]*wcipb.ScalingGroupSpec{
		"a": {Compute: &wcipb.ComputeProviderSpec{ProviderType: "aws-lambda"}},
		"b": {Compute: &wcipb.ComputeProviderSpec{ProviderType: "aws-ecs"}},
		"c": {Compute: &wcipb.ComputeProviderSpec{ProviderType: "aws-lambda"}},
	}
	got = values(w)
	require.Equal(t, "failed", got["WCIValidationStatus"])
	require.Equal(t, "[aws-ecs aws-lambda]", got["WCIComputeProviders"], "deduplicated and sorted")
}

func TestRunningListQuery(t *testing.T) {
	require.Equal(t, "WorkerControllerInstanceStatus = 'Running'", RunningListQuery)
}
