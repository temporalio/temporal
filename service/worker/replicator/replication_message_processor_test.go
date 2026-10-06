package replicator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

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
