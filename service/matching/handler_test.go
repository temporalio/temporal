package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkerHeartbeatToListInfo_AllFieldsSet(t *testing.T) {
	hb := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "instance-key-1",
		WorkerIdentity:    "worker-identity-1",
		HostInfo: &workerpb.WorkerHostInfo{
			HostName:          "host-1",
			WorkerGroupingKey: "grouping-key-1",
			ProcessId:         "pid-123",
		},
		TaskQueue:         "my-task-queue",
		DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "dep-1", BuildId: "build-1"},
		SdkName:           "temporal-go",
		SdkVersion:        "1.0.0",
		Status:            enumspb.WORKER_STATUS_RUNNING,
		StartTime:         timestamppb.Now(),
		Plugins:           []*workerpb.PluginInfo{{Name: "plugin-1"}},
		Drivers:           []*workerpb.StorageDriverInfo{{Type: "driver-1"}},
	}

	result := workerHeartbeatToListInfo(hb)
	require.NotNil(t, result)

	resultReflect := result.ProtoReflect()
	fields := resultReflect.Descriptor().Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		val := resultReflect.Get(fd)
		if fd.IsList() {
			assert.Falsef(t, val.List().Len() == 0,
				"WorkerListInfo repeated field %q is empty — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		} else {
			assert.Truef(t, resultReflect.Has(fd),
				"WorkerListInfo field %q is not set by workerHeartbeatToListInfo — "+
					"if you added a new field to WorkerListInfo, make sure to populate it from WorkerHeartbeat",
				fd.Name(),
			)
		}
	}
}
