// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package temporal

import (
	"context"
	"path"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/tests/testutils"
)

func TestInitCurrentClusterMetadataRecord(t *testing.T) {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-cass", configDir, "")
	require.NoError(t, err)
	controller := gomock.NewController(t)

	mockClusterMetadataManager := persistence.NewMockClusterMetadataManager(controller)
	mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.SaveClusterMetadataRequest) (bool, error) {
			require.Equal(t, cfg.ClusterMetadata.EnableGlobalNamespace, request.IsGlobalNamespaceEnabled)
			require.Equal(t, cfg.ClusterMetadata.CurrentClusterName, request.ClusterName)
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].RPCAddress, request.ClusterAddress)
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
			require.Equal(t, cfg.ClusterMetadata.ClusterInformation[cfg.ClusterMetadata.CurrentClusterName].InitialFailoverVersion, request.InitialFailoverVersion)
			require.Equal(t, int32(0), request.HistoryShardCount)
			require.Equal(t, cfg.ClusterMetadata.FailoverVersionIncrement, request.FailoverVersionIncrement)
			require.Equal(t, int64(1), request.Version)
			return true, nil
		},
	)
	updateRecord := &persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{},
		Version:         1,
	}
	err = updateCurrentClusterMetadataRecord(
		context.TODO(),
		mockClusterMetadataManager,
		cfg,
		updateRecord,
	)
	require.NoError(t, err)
}

func TestOverwriteCurrentClusterMetadataWithDBRecord(t *testing.T) {
	configDir := path.Join(testutils.GetRepoRootDirectory(), "config")
	cfg, err := config.LoadConfig("development-cass", configDir, "")
	require.NoError(t, err)

	dbRecord := &persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
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
