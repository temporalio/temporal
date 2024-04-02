// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package temporal_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/temporal"
)

func TestNewClusterMetadataLoader(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockClusterMetadataManager(ctrl)
	logger := log.NewNoopLogger()
	loader := temporal.NewClusterMetadataLoader(metadataManager, logger)
	ctx := context.Background()
	svc := &config.Config{
		ClusterMetadata: &cluster.Config{
			CurrentClusterName: "current_cluster",
			ClusterInformation: map[string]cluster.ClusterInformation{
				"current_cluster": {
					RPCAddress:  "rpc_old",
					HTTPAddress: "http_old",
				},
				"remote_cluster1": {
					RPCAddress:  "rpc_old",
					HTTPAddress: "http_old",
				},
			},
		},
	}

	metadataManager.EXPECT().ListClusterMetadata(gomock.Any(), gomock.Any()).Return(&persistence.ListClusterMetadataResponse{
		ClusterMetadata: []*persistence.GetClusterMetadataResponse{
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "current_cluster",
					ClusterAddress: "rpc_new",
					HttpAddress:    "http_new",
				},
			},
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "remote_cluster1",
					ClusterAddress: "rpc_new",
					HttpAddress:    "http_new",
				},
			},
			{
				ClusterMetadata: &persistencespb.ClusterMetadata{
					ClusterName:    "remote_cluster2",
					ClusterAddress: "rpc_new",
					HttpAddress:    "http_new",
				},
			},
		},
	}, nil)

	err := loader.LoadAndMergeWithStaticConfig(ctx, svc)
	require.NoError(t, err)

	assert.Equal(t, map[string]cluster.ClusterInformation{
		"current_cluster": {
			RPCAddress:  "rpc_old",
			HTTPAddress: "http_new",
		},
		"remote_cluster1": {
			RPCAddress:  "rpc_new",
			HTTPAddress: "http_new",
		},
		"remote_cluster2": {
			RPCAddress:  "rpc_new",
			HTTPAddress: "http_new",
		},
	}, svc.ClusterMetadata.ClusterInformation)
}
