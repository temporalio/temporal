package persistence_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
)

// namespaceTestCases has a case for each request type of the stores.
var namespaceTestCases = []struct {
	name    string
	request any
	want    string
}{
	// ExecutionStore
	{"AddHistoryTasks", &persistence.InternalAddHistoryTasksRequest{ShardID: 1, NamespaceID: "ns"}, "ns"},
	{"ConflictResolveWorkflowExecution", &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ResetWorkflowSnapshot: persistence.InternalWorkflowSnapshot{NamespaceID: "ns"},
	}, "ns"},
	{"CreateWorkflowExecution", &persistence.InternalCreateWorkflowExecutionRequest{
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{NamespaceID: "ns"},
	}, "ns"},
	{"SetWorkflowExecution", &persistence.InternalSetWorkflowExecutionRequest{
		SetWorkflowSnapshot: persistence.InternalWorkflowSnapshot{NamespaceID: "ns"},
	}, "ns"},
	{"UpdateWorkflowExecution", &persistence.InternalUpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: persistence.InternalWorkflowMutation{NamespaceID: "ns"},
	}, "ns"},
	{"DeleteCurrentWorkflowExecution", &persistence.DeleteCurrentWorkflowExecutionRequest{NamespaceID: "ns"}, "ns"},
	{"DeleteWorkflowExecution", &persistence.DeleteWorkflowExecutionRequest{NamespaceID: "ns"}, "ns"},
	{"GetCurrentExecution", &persistence.GetCurrentExecutionRequest{NamespaceID: "ns"}, "ns"},
	{"GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{NamespaceID: "ns"}, "ns"},
	{"PutReplicationTaskToDLQ", &persistence.PutReplicationTaskToDLQRequest{
		TaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: "ns"},
	}, "ns"},
	{"PutReplicationTaskToDLQ without task info", &persistence.PutReplicationTaskToDLQRequest{ShardID: 1}, ""},
	{"AppendHistoryNodes", &persistence.InternalAppendHistoryNodesRequest{ShardID: 1}, ""},
	{"DeleteHistoryBranch", &persistence.InternalDeleteHistoryBranchRequest{ShardID: 1}, ""},
	{"DeleteHistoryNodes", &persistence.InternalDeleteHistoryNodesRequest{ShardID: 1}, ""},
	{"ForkHistoryBranch", &persistence.InternalForkHistoryBranchRequest{ShardID: 1}, ""},
	{"GetAllHistoryTreeBranches", &persistence.GetAllHistoryTreeBranchesRequest{PageSize: 1}, ""},
	{"GetHistoryTreeContainingBranch", &persistence.InternalGetHistoryTreeContainingBranchRequest{ShardID: 1}, ""},
	{"ReadHistoryBranch", &persistence.InternalReadHistoryBranchRequest{ShardID: 1}, ""},
	{"CompleteHistoryTask", &persistence.CompleteHistoryTaskRequest{ShardID: 1}, ""},
	{"GetHistoryTasks", &persistence.GetHistoryTasksRequest{ShardID: 1}, ""},
	{"RangeCompleteHistoryTasks", &persistence.RangeCompleteHistoryTasksRequest{ShardID: 1}, ""},
	{"ListConcreteExecutions", &persistence.ListConcreteExecutionsRequest{ShardID: 1}, ""},
	{"GetReplicationTasksFromDLQ", &persistence.GetReplicationTasksFromDLQRequest{SourceClusterName: "cluster"}, ""},
	{"DeleteReplicationTaskFromDLQ", &persistence.DeleteReplicationTaskFromDLQRequest{SourceClusterName: "cluster"}, ""},
	{"RangeDeleteReplicationTaskFromDLQ", &persistence.RangeDeleteReplicationTaskFromDLQRequest{SourceClusterName: "cluster"}, ""},

	// MetadataStore
	{"CreateNamespace", &persistence.InternalCreateNamespaceRequest{ID: "ns", Name: "name"}, "ns"},
	{"UpdateNamespace", &persistence.InternalUpdateNamespaceRequest{Id: "ns", Name: "name"}, "ns"},
	{"RenameNamespace", &persistence.InternalRenameNamespaceRequest{
		InternalUpdateNamespaceRequest: &persistence.InternalUpdateNamespaceRequest{Id: "ns", Name: "name"},
	}, "ns"},
	{"RenameNamespace without update request", &persistence.InternalRenameNamespaceRequest{PreviousName: "name"}, ""},
	{"DeleteNamespace", &persistence.DeleteNamespaceRequest{ID: "ns"}, "ns"},
	{"GetNamespace by ID", &persistence.GetNamespaceRequest{ID: "ns"}, "ns"},
	{"GetNamespace by name", &persistence.GetNamespaceRequest{Name: "name"}, ""},
	{"DeleteNamespaceByName", &persistence.DeleteNamespaceByNameRequest{Name: "name"}, ""},
	{"ListNamespaces", &persistence.InternalListNamespacesRequest{PageSize: 1}, ""},

	// TaskStore
	{"CreateTaskQueue", &persistence.InternalCreateTaskQueueRequest{NamespaceID: "ns"}, "ns"},
	{"GetTaskQueue", &persistence.InternalGetTaskQueueRequest{NamespaceID: "ns"}, "ns"},
	{"UpdateTaskQueue", &persistence.InternalUpdateTaskQueueRequest{NamespaceID: "ns"}, "ns"},
	{"DeleteTaskQueue", &persistence.DeleteTaskQueueRequest{TaskQueue: &persistence.TaskQueueKey{NamespaceID: "ns"}}, "ns"},
	{"DeleteTaskQueue without task queue", &persistence.DeleteTaskQueueRequest{RangeID: 1}, ""},
	{"CreateTasks", &persistence.InternalCreateTasksRequest{NamespaceID: "ns"}, "ns"},
	{"GetTasks", &persistence.GetTasksRequest{NamespaceID: "ns"}, "ns"},
	{"CompleteTasksLessThan", &persistence.CompleteTasksLessThanRequest{NamespaceID: "ns"}, "ns"},
	{"GetTaskQueueUserData", &persistence.GetTaskQueueUserDataRequest{NamespaceID: "ns"}, "ns"},
	{"UpdateTaskQueueUserData", &persistence.InternalUpdateTaskQueueUserDataRequest{NamespaceID: "ns"}, "ns"},
	{"ListTaskQueueUserDataEntries", &persistence.ListTaskQueueUserDataEntriesRequest{NamespaceID: "ns"}, "ns"},
	{"GetTaskQueuesByBuildId", &persistence.GetTaskQueuesByBuildIdRequest{NamespaceID: "ns"}, "ns"},
	{"CountTaskQueuesByBuildId", &persistence.CountTaskQueuesByBuildIdRequest{NamespaceID: "ns"}, "ns"},
	{"ListTaskQueue", &persistence.ListTaskQueueRequest{PageSize: 1}, ""},

	// ShardStore
	{"GetOrCreateShard", &persistence.InternalGetOrCreateShardRequest{ShardID: 1}, ""},
	{"UpdateShard", &persistence.InternalUpdateShardRequest{ShardID: 1}, ""},
	{"AssertShardOwnership", &persistence.AssertShardOwnershipRequest{ShardID: 1}, ""},

	// ClusterMetadataStore
	{"GetClusterMembers", &persistence.GetClusterMembersRequest{RoleEquals: persistence.History}, ""},
	{"UpsertClusterMembership", &persistence.UpsertClusterMembershipRequest{RPCPort: 1}, ""},
	{"PruneClusterMembership", &persistence.PruneClusterMembershipRequest{MaxRecordsPruned: 1}, ""},
	{"ListClusterMetadata", &persistence.InternalListClusterMetadataRequest{PageSize: 1}, ""},
	{"GetClusterMetadata", &persistence.InternalGetClusterMetadataRequest{ClusterName: "cluster"}, ""},
	{"SaveClusterMetadata", &persistence.InternalSaveClusterMetadataRequest{ClusterName: "cluster"}, ""},
	{"DeleteClusterMetadata", &persistence.InternalDeleteClusterMetadataRequest{ClusterName: "cluster"}, ""},

	// Queue
	{"Queue message ID", int64(1), ""},
	{"Queue blob", &commonpb.DataBlob{}, ""},
	{"Queue metadata", &persistence.InternalQueueMetadata{Version: 1}, ""},

	// QueueV2
	{"CreateQueue", &persistence.InternalCreateQueueRequest{QueueName: "queue"}, ""},
	{"EnqueueMessage", &persistence.InternalEnqueueMessageRequest{QueueName: "queue"}, ""},
	{"ReadMessages", &persistence.InternalReadMessagesRequest{QueueName: "queue"}, ""},
	{"RangeDeleteMessages", &persistence.InternalRangeDeleteMessagesRequest{QueueName: "queue"}, ""},
	{"ListQueues", &persistence.InternalListQueuesRequest{PageSize: 1}, ""},

	// NexusEndpointStore
	{"CreateOrUpdateNexusEndpoint", &persistence.InternalCreateOrUpdateNexusEndpointRequest{LastKnownTableVersion: 1}, ""},
	{"DeleteNexusEndpoint", &persistence.DeleteNexusEndpointRequest{ID: "endpoint"}, ""},
	{"GetNexusEndpoint", &persistence.GetNexusEndpointRequest{ID: "endpoint"}, ""},
	{"ListNexusEndpoints", &persistence.ListNexusEndpointsRequest{PageSize: 1}, ""},
}

func TestRequestNamespaceID(t *testing.T) {
	t.Parallel()

	for _, tc := range namespaceTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, persistence.RequestNamespaceID(tc.request))
		})
	}
}

// TestNamespaceTestCasesCoverStores makes sure that a request type added to a wrapped store gets a
// case in namespaceTestCases, so that it is decided whether its failpoints are namespace-scoped.
func TestNamespaceTestCasesCoverStores(t *testing.T) {
	t.Parallel()

	covered := make(map[reflect.Type]bool, len(namespaceTestCases))
	for _, tc := range namespaceTestCases {
		covered[reflect.TypeOf(tc.request)] = true
	}
	contextType := reflect.TypeFor[context.Context]()
	for _, store := range []reflect.Type{
		reflect.TypeFor[persistence.ExecutionStore](),
		reflect.TypeFor[persistence.MetadataStore](),
		reflect.TypeFor[persistence.TaskStore](),
		reflect.TypeFor[persistence.ShardStore](),
		reflect.TypeFor[persistence.ClusterMetadataStore](),
		reflect.TypeFor[persistence.Queue](),
		reflect.TypeFor[persistence.QueueV2](),
		reflect.TypeFor[persistence.NexusEndpointStore](),
	} {
		for method := range store.Methods() {
			// The wrappers pass the parameter after the context to the fault generator.
			if method.Type.NumIn() < 2 || method.Type.In(0) != contextType {
				continue
			}
			require.True(t, covered[method.Type.In(1)], "no case for %s.%s request %s", store.Name(), method.Name, method.Type.In(1))
		}
	}
}
