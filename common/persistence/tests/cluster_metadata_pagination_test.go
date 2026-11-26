package tests

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
)

func TestSQLiteClusterMetadataPaginationRepeatsFirstPage(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	logger := log.NewNoopLogger()
	serializer := serialization.NewSerializer()
	factory := sql.NewFactory(
		*NewSQLiteMemoryConfig(), resolver.NewNoopResolver(), testSQLiteClusterName,
		logger, metrics.NoopMetricsHandler, serializer,
	)
	t.Cleanup(factory.Close)
	store, err := factory.NewClusterMetadataStore()
	require.NoError(t, err)
	manager := persistence.NewClusterMetadataManagerImpl(store, serializer, testSQLiteClusterName, logger)
	for index, name := range []string{"cluster-a", "cluster-b"} {
		applied, err := manager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				ClusterName:              name,
				ClusterId:                uuid.NewString(),
				HistoryShardCount:        4,
				ClusterAddress:           name + ":7233",
				FailoverVersionIncrement: 10,
				InitialFailoverVersion:   int64(index + 1),
				IsGlobalNamespaceEnabled: true,
				IsConnectionEnabled:      true,
			},
		})
		require.NoError(t, err)
		require.True(t, applied)
	}
	secondCluster, err := manager.GetClusterMetadata(ctx, &persistence.GetClusterMetadataRequest{ClusterName: "cluster-b"})
	require.NoError(t, err)
	require.Equal(t, "cluster-b", secondCluster.GetClusterName())

	first, err := manager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, first.ClusterMetadata, 1)
	require.Equal(t, "cluster-a", first.ClusterMetadata[0].GetClusterName())
	require.NotEmpty(t, first.NextPageToken)
	var cursor string
	require.NoError(t, gob.NewDecoder(bytes.NewReader(first.NextPageToken)).Decode(&cursor))
	require.Empty(t, cursor)

	// Characterize the current defect with bounded reads. A corrected traversal
	// must return cluster-b next and then terminate; no data changes between pages.
	for range 2 {
		next, err := manager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{
			PageSize: 1, NextPageToken: first.NextPageToken,
		})
		require.NoError(t, err)
		require.Len(t, next.ClusterMetadata, 1)
		require.Equal(t, "cluster-a", next.ClusterMetadata[0].GetClusterName())
		require.Equal(t, first.NextPageToken, next.NextPageToken)
	}
}
