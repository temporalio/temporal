package temporal

import (
	"context"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/mock/gomock"
)

func TestInitCurrentClusterMetadataRecord(t *testing.T) {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-cass-es", configDir, "")
	require.NoError(t, err)
	controller := gomock.NewController(t)

	mockClusterMetadataManager := persistence.NewMockClusterMetadataManager(controller)
	mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.SaveClusterMetadataRequest) (bool, error) {
			require.Equal(t, cfg.ClusterMetadata.EnableGlobalNamespace, request.IsGlobalNamespaceEnabled)
			require.Equal(t, cfg.ClusterMetadata.CurrentClusterName, request.ClusterName)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].RPCAddress, request.ClusterAddress)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].HTTPAddress, request.HttpAddress)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].InitialFailoverVersion, request.InitialFailoverVersion)
			require.Equal(t, cfg.Persistence.NumHistoryShards, request.HistoryShardCount)
			require.Equal(t, cfg.ClusterMetadata.FailoverVersionIncrement, request.FailoverVersionIncrement)
			require.Equal(t, int64(0), request.Version)
			return true, nil
		},
	)
	err = initCurrentClusterMetadataRecord(
		context.TODO(),
		mockClusterMetadataManager,
		cfg,
		nil,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
}

func TestUpdateCurrentClusterMetadataRecord(t *testing.T) {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-cluster-a", configDir, "")
	require.NoError(t, err)
	controller := gomock.NewController(t)

	mockClusterMetadataManager := persistence.NewMockClusterMetadataManager(controller)
	mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.SaveClusterMetadataRequest) (bool, error) {
			require.Equal(t, cfg.ClusterMetadata.EnableGlobalNamespace, request.IsGlobalNamespaceEnabled)
			require.Equal(t, "", request.ClusterName)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].RPCAddress, request.ClusterAddress)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].HTTPAddress, request.HttpAddress)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].InitialFailoverVersion, request.InitialFailoverVersion)
			require.Equal(t, int32(0), request.HistoryShardCount)
			require.Equal(t, cfg.ClusterMetadata.FailoverVersionIncrement, request.FailoverVersionIncrement)
			require.Equal(t, int64(1), request.Version)
			return true, nil
		},
	)
	updateRecord := &persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{},
		Version:         1,
	}
	err = updateCurrentClusterMetadataRecord(
		context.TODO(),
		mockClusterMetadataManager,
		cfg,
		nil,
		updateRecord,
	)
	require.NoError(t, err)
}

func TestOverwriteCurrentClusterMetadataWithDBRecord(t *testing.T) {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-cass-es", configDir, "")
	require.NoError(t, err)

	dbRecord := &persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			HistoryShardCount:        1024,
			FailoverVersionIncrement: 10000,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 1,
	}
	overwriteCurrentClusterMetadataWithDBRecord(
		cfg,
		dbRecord,
		log.NewNoopLogger(),
	)
	require.Equal(t, int64(10000), cfg.ClusterMetadata.FailoverVersionIncrement)
	require.True(t, cfg.ClusterMetadata.EnableGlobalNamespace)
	require.Equal(t, int32(1024), cfg.Persistence.NumHistoryShards)
}

func TestTaskCategoryRegistryProvider(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		historyState           archiver.ArchivalState
		visibilityState        archiver.ArchivalState
		expectArchivalCategory bool
	}{
		{
			name:                   "both disabled",
			historyState:           archiver.ArchivalDisabled,
			visibilityState:        archiver.ArchivalDisabled,
			expectArchivalCategory: false,
		},
		{
			name:                   "history enabled",
			historyState:           archiver.ArchivalEnabled,
			visibilityState:        archiver.ArchivalDisabled,
			expectArchivalCategory: true,
		},
		{
			name:                   "visibility enabled",
			historyState:           archiver.ArchivalDisabled,
			visibilityState:        archiver.ArchivalEnabled,
			expectArchivalCategory: true,
		},
		{
			name:                   "both enabled",
			historyState:           archiver.ArchivalEnabled,
			visibilityState:        archiver.ArchivalEnabled,
			expectArchivalCategory: true,
		},
		{
			name:                   "callbacks enabled, archival disabled",
			historyState:           archiver.ArchivalDisabled,
			visibilityState:        archiver.ArchivalDisabled,
			expectArchivalCategory: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			archivalMetadata := archiver.NewMockArchivalMetadata(ctrl)
			historyArchivalConfig := archiver.NewMockArchivalConfig(ctrl)
			historyArchivalConfig.EXPECT().StaticClusterState().Return(tc.historyState).AnyTimes()
			archivalMetadata.EXPECT().GetHistoryConfig().Return(historyArchivalConfig).AnyTimes()
			visibilityArchivalConfig := archiver.NewMockArchivalConfig(ctrl)
			visibilityArchivalConfig.EXPECT().StaticClusterState().Return(tc.visibilityState).AnyTimes()
			archivalMetadata.EXPECT().GetVisibilityConfig().Return(visibilityArchivalConfig).AnyTimes()
			registry := TaskCategoryRegistryProvider(archivalMetadata)
			_, ok := registry.GetCategoryByID(tasks.CategoryIDArchival)
			if tc.expectArchivalCategory {
				require.True(t, ok)
			} else {
				require.False(t, ok)
			}
		})
	}
}
