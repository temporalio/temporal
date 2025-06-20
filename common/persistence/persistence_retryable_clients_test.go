package persistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/mock"
	"go.uber.org/mock/gomock"
)

// Tests retries on data loss errors from the persistence layer. It configures the clients with
// client.IsPersistenceTransientError and asserts that the underlying persistence
// API is invoked exactly testMaxAttempts times on serviceerror.DataLoss errors.
func TestPersistence_RetryDataLossErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataLossErr := serviceerror.NewDataLoss("test")
	ctrl := gomock.NewController(t)
	testMaxAttempts := 2
	retryPolicy := backoff.NewConstantDelayRetryPolicy(time.Millisecond).WithMaximumAttempts(testMaxAttempts)

	t.Run("NewShardPersistenceRetryableClient", func(t *testing.T) {
		mockMgr := persistence.NewMockShardManager(ctrl)
		mockMgr.EXPECT().GetOrCreateShard(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewShardPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.GetOrCreateShard(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})

	t.Run("NewExecutionPersistenceRetryableClient", func(t *testing.T) {
		mockMgr := persistence.NewMockExecutionManager(ctrl)
		mockMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewExecutionPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.CreateWorkflowExecution(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})

	t.Run("NewTaskPersistenceRetryableClient", func(t *testing.T) {
		mockMgr := persistence.NewMockTaskManager(ctrl)
		mockMgr.EXPECT().CreateTasks(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewTaskPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.CreateTasks(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})
	t.Run("NewMetadataPersistenceRetryableClient", func(t *testing.T) {
		mockMgr := persistence.NewMockMetadataManager(ctrl)
		mockMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewMetadataPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.CreateNamespace(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})
	t.Run("NewClusterMetadataPersistenceRetryableClient", func(t *testing.T) {
		mockMgr := persistence.NewMockClusterMetadataManager(ctrl)
		mockMgr.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewClusterMetadataPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.GetClusterMembers(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})
	t.Run("NewQueuePersistenceRetryableClient(", func(t *testing.T) {
		mockQueue := mock.NewMockQueue(ctrl)
		mockQueue.EXPECT().EnqueueMessage(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(dataLossErr)

		retryablePersistenceClient := persistence.NewQueuePersistenceRetryableClient(mockQueue, retryPolicy, client.IsPersistenceTransientError)
		err := retryablePersistenceClient.EnqueueMessage(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
	})
	t.Run("NewNexusEndpointPersistenceRetryableClient(", func(t *testing.T) {
		mockMgr := persistence.NewMockNexusEndpointManager(ctrl)
		mockMgr.EXPECT().GetNexusEndpoint(gomock.Any(), gomock.Any()).Times(testMaxAttempts).Return(nil, dataLossErr)

		retryablePersistenceClient := persistence.NewNexusEndpointPersistenceRetryableClient(mockMgr, retryPolicy, client.IsPersistenceTransientError)
		resp, err := retryablePersistenceClient.GetNexusEndpoint(ctx, nil)
		require.ErrorIs(t, err, dataLossErr)
		require.Nil(t, resp)
	})

}
