package replicator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

// TestRetryPolicyForTask verifies that the processor's per-task retry-policy
// selector hands out a distinct policy for namespace tasks (which need the
// wider CAS-tolerant budget) and a shared default policy for all other task
// types.
func TestRetryPolicyForTask(t *testing.T) {
	p := newReplicationMessageProcessor(
		"currentCluster",
		"sourceCluster",
		nil,                        // logger
		nil,                        // remotePeer
		metrics.NoopMetricsHandler, // metricsHandler — actually used by constructor
		nil,                        // namespaceTaskExecutor
		nil,                        // customTaskHandler
		nil,                        // hostInfo
		nil,                        // serviceResolver
		nil,                        // namespaceReplicationQueue
		nil,                        // matchingClient
		nil,                        // namespaceRegistry
	)

	nsTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK}
	tqTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA}
	historyTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK}
	unspecifiedTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_UNSPECIFIED}

	nsPolicy := p.retryPolicyForTask(nsTask)
	defaultPolicy := p.retryPolicyForTask(tqTask)

	require.NotSame(t, nsPolicy, defaultPolicy,
		"namespace task must use a different retry policy from other task types")

	// All non-namespace task types share the same default policy instance.
	require.Same(t, defaultPolicy, p.retryPolicyForTask(historyTask))
	require.Same(t, defaultPolicy, p.retryPolicyForTask(unspecifiedTask))

	// Selector is stable across calls.
	require.Same(t, nsPolicy, p.retryPolicyForTask(nsTask))
}

func TestHandleTaskQueueUserDataReplicationTask_LocalNamespace(t *testing.T) {
	controller := gomock.NewController(t)
	registry := namespace.NewMockRegistry(controller)
	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	p := &replicationMessageProcessor{
		namespaceRegistry: registry,
		matchingClient:    matchingClient,
		logger:            log.NewNoopLogger(),
		sourceCluster:     "source-cluster",
	}
	attrs := &replicationspb.TaskQueueUserDataAttributes{NamespaceId: "namespace-id"}
	registry.EXPECT().GetNamespaceByID(namespace.ID("namespace-id")).Return(
		namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
			nil,
			"current-cluster",
		),
		nil,
	)

	require.NoError(t, p.handleTaskQueueUserDataReplicationTask(context.Background(), attrs))
}
