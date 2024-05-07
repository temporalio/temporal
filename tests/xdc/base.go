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

//go:build !race

// need to run xdc tests with race detector off because of ringpop bug causing data race issue

package xdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/server/common/testing/historyrequire"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/environment"
	"go.temporal.io/server/tests"
)

type (
	xdcBaseSuite struct {
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		clusterNames []string
		suite.Suite

		testClusterFactory tests.TestClusterFactory

		cluster1               *tests.TestCluster
		cluster2               *tests.TestCluster
		logger                 log.Logger
		dynamicConfigOverrides map[dynamicconfig.Key]interface{}

		startTime time.Time
	}
)

func (s *xdcBaseSuite) clusterReplicationConfig() []*replicationpb.ClusterReplicationConfig {
	config := make([]*replicationpb.ClusterReplicationConfig, len(s.clusterNames))
	for i, clusterName := range s.clusterNames {
		config[i] = &replicationpb.ClusterReplicationConfig{
			ClusterName: clusterName,
		}
	}
	return config
}

func (s *xdcBaseSuite) setupSuite(clusterNames []string, opts ...tests.Option) {
	s.testClusterFactory = tests.NewTestClusterFactory()

	params := tests.ApplyTestClusterParams(opts)

	s.clusterNames = clusterNames
	if s.logger == nil {
		s.logger = log.NewTestLogger()
	}
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}

	fileName := "../testdata/xdc_clusters.yaml"
	if tests.TestFlags.TestClusterConfigFile != "" {
		fileName = tests.TestFlags.TestClusterConfigFile
	}
	environment.SetupEnv()

	confContent, err := os.ReadFile(fileName)
	s.Require().NoError(err)
	confContent = []byte(os.ExpandEnv(string(confContent)))

	var clusterConfigs []*tests.TestClusterConfig
	s.Require().NoError(yaml.Unmarshal(confContent, &clusterConfigs))
	for i, config := range clusterConfigs {
		config.DynamicConfigOverrides = s.dynamicConfigOverrides
		clusterConfigs[i].ClusterMetadata.MasterClusterName = s.clusterNames[i]
		clusterConfigs[i].ClusterMetadata.CurrentClusterName = s.clusterNames[i]
		clusterConfigs[i].Persistence.DBName = "func_" + s.clusterNames[i]
		clusterConfigs[i].ClusterMetadata.ClusterInformation = make(map[string]cluster.ClusterInformation)
		// TODO: make tests.temporalImpl actually use these ports. Right now, we're just setting these to the values
		// that the tests.temporalImpl uses by default, so that our info here is right, but these aren't actually used
		// by NewCluster.
		clusterConfigs[i].ClusterMetadata.ClusterInformation[s.clusterNames[i]] = cluster.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: int64(i + 1),
			RPCAddress:             fmt.Sprintf("127.0.0.1:%d134", 7+i),
			HTTPAddress:            fmt.Sprintf("http://127.0.0.1:%d144", 7+i),
		}
		clusterConfigs[i].ServiceFxOptions = params.ServiceOptions
	}

	c, err := s.testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(s.clusterNames[0])))
	s.Require().NoError(err)
	s.cluster1 = c

	c, err = s.testClusterFactory.NewCluster(s.T(), clusterConfigs[1], log.With(s.logger, tag.ClusterName(s.clusterNames[1])))
	s.Require().NoError(err)
	s.cluster2 = c

	s.startTime = time.Now()

	cluster1Info := clusterConfigs[0].ClusterMetadata.ClusterInformation[clusterConfigs[0].ClusterMetadata.CurrentClusterName]
	cluster2Info := clusterConfigs[1].ClusterMetadata.ClusterInformation[clusterConfigs[1].ClusterMetadata.CurrentClusterName]
	_, err = s.cluster1.GetAdminClient().AddOrUpdateRemoteCluster(
		tests.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               cluster2Info.RPCAddress,
			FrontendHttpAddress:           cluster2Info.HTTPAddress,
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)

	_, err = s.cluster2.GetAdminClient().AddOrUpdateRemoteCluster(
		tests.NewContext(),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               cluster1Info.RPCAddress,
			FrontendHttpAddress:           cluster1Info.HTTPAddress,
			EnableRemoteClusterConnection: true,
		})
	s.Require().NoError(err)
	// Wait for cluster metadata to refresh new added clusters
	time.Sleep(time.Millisecond * 200)
}

func (s *xdcBaseSuite) waitForClusterConnected() {
	s.logger.Debug("wait for cluster to be connected")
	s.EventuallyWithT(func(c *assert.CollectT) {
		s.logger.Debug("check if stream is established")
		resp, err := s.cluster1.GetHistoryClient().GetReplicationStatus(context.Background(), &historyservice.GetReplicationStatusRequest{})
		if !(assert.NoError(c, err) &&
			assert.Equal(c, 1, len(resp.Shards))) { // test cluster has only one history shard
			return
		}
		shard := resp.Shards[0]
		if !(assert.NotNil(c, shard) &&
			assert.True(c, shard.MaxReplicationTaskId > 0) &&
			assert.NotNil(c, shard.ShardLocalTime) &&
			assert.True(c, shard.ShardLocalTime.AsTime().Before(time.Now())) &&
			assert.True(c, shard.ShardLocalTime.AsTime().After(s.startTime)) &&
			assert.NotNil(c, shard.RemoteClusters)) {
			return
		}
		standbyAckInfo, ok := shard.RemoteClusters[s.clusterNames[1]]
		if !(assert.True(c, ok) &&
			assert.NotNil(c, standbyAckInfo) &&
			assert.NotNil(c, standbyAckInfo.AckedTaskVisibilityTime) &&
			assert.True(c, standbyAckInfo.AckedTaskVisibilityTime.AsTime().Before(time.Now())) &&
			assert.True(c, standbyAckInfo.AckedTaskVisibilityTime.AsTime().After(s.startTime))) {
			return
		}
		s.logger.Debug("cluster connected")
	}, 60*time.Second, 1*time.Second)
}

func (s *xdcBaseSuite) tearDownSuite() {
	s.NoError(s.cluster1.TearDownCluster())
	s.NoError(s.cluster2.TearDownCluster())
}

func (s *xdcBaseSuite) setupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())

	s.waitForClusterConnected()
}
