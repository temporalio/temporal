package frontend

import (
	"testing"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestLivenessHeartbeatRequestOmitsMetadata(t *testing.T) {
	t.Parallel()
	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker-1",
		WorkerIdentity:    "worker-identity",
		TaskQueue:         "task-queue",
		HostInfo: &workerpb.WorkerHostInfo{
			WorkerGroupingKey: "group-1",
			HostName:          "host",
			ProcessId:         "process",
		},
	}
	request := &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: "namespace",
		Identity:  "client-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			worker,
			{WorkerInstanceKey: "worker-2", HostInfo: &workerpb.WorkerHostInfo{WorkerGroupingKey: "group-1"}},
		},
	}
	original := common.CloneProto(request)

	// Forward the worker keys and routing information for the whole batch.
	got := newLivenessHeartbeatRequest(request, "group-1")
	protorequire.ProtoEqual(t, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace:  request.Namespace,
		Identity:   request.Identity,
		ResourceId: "group-1",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker-1"},
			{WorkerInstanceKey: "worker-2"},
		},
	}, got)

	// Matching still needs the original metadata for every worker.
	protorequire.ProtoEqual(t, original, request)
}
