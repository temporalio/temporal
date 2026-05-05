package xdc

import (
	"cmp"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/tests/testcore"
)

type RemoteClusterAuthSuite struct {
	xdcBaseSuite
	capturedTokens sync.Map
}

func TestRemoteClusterAuthSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &RemoteClusterAuthSuite{})
}

func (s *RemoteClusterAuthSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	s.dynamicConfigOverrides[dynamicconfig.ClusterMetadataRefreshInterval.Key()] = time.Second * 5
	s.dynamicConfigOverrides[dynamicconfig.NamespaceCacheRefreshInterval.Key()] = testcore.NamespaceCacheRefreshInterval
	s.dynamicConfigOverrides[dynamicconfig.SendRawHistoryBetweenInternalServices.Key()] = true
	s.dynamicConfigOverrides[dynamicconfig.TransferProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.TimerProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.VisibilityProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.OutboundProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.ArchivalProcessorUpdateAckInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.TransferProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.TimerProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.VisibilityProcessorMaxPollInterval.Key()] = time.Second * 3
	s.dynamicConfigOverrides[dynamicconfig.OutboundProcessorMaxPollInterval.Key()] = time.Second * 3

	tokenFile := filepath.Join(s.T().TempDir(), "remote-cluster.jwt")
	s.Require().NoError(os.WriteFile(tokenFile, []byte("test-xdc-jwt-token"), 0600))

	tokenProvider := &auth.FileTokenProvider{
		TokenFiles: map[string]string{
			"127.0.0.1": tokenFile,
		},
	}

	persistenceDefaults := testcore.GetPersistenceTestDefaults()
	clusterConfigs := []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{NumHistoryShards: 1},
			Persistence:   persistenceDefaults,
			TokenProvider: tokenProvider,
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig: testcore.HistoryConfig{NumHistoryShards: 1},
			Persistence:   persistenceDefaults,
			TokenProvider: tokenProvider,
		},
	}

	s.clusters = make([]*testcore.TestCluster, len(clusterConfigs))
	suffix := common.GenerateRandomString(5)

	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range []string{"active_" + cmp.Or(suffix), "standby_" + cmp.Or(suffix)} {
		clusterConfigs[clusterIndex].DynamicConfigOverrides = s.dynamicConfigOverrides
		clusterConfigs[clusterIndex].ClusterMetadata.MasterClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.CurrentClusterName = clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.EnableGlobalNamespace = true
		clusterConfigs[clusterIndex].Persistence.DBName += "_" + clusterName
		clusterConfigs[clusterIndex].ClusterMetadata.ClusterInformation = map[string]cluster.ClusterInformation{
			clusterName: {
				Enabled:                true,
				InitialFailoverVersion: int64(clusterIndex + 1),
			},
		}
		clusterConfigs[clusterIndex].EnableMetricsCapture = true

		var err error
		s.clusters[clusterIndex], err = testClusterFactory.NewCluster(s.T(), clusterConfigs[clusterIndex], log.With(s.logger, tag.ClusterName(clusterName)))
		s.Require().NoError(err)
	}

	s.startTime = time.Now()

	for _, c := range s.clusters {
		clusterName := c.ClusterName()
		c.Host().SetOnGetClaims(func(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
			if authInfo != nil && authInfo.AuthToken != "" {
				s.capturedTokens.Store(clusterName, authInfo.AuthToken)
			}
			return &authorization.Claims{System: authorization.RoleAdmin}, nil
		})
	}

	for ci, c := range s.clusters {
		for remoteCi, remoteC := range s.clusters {
			if ci != remoteCi {
				_, err := c.AdminClient().AddOrUpdateRemoteCluster(
					testcore.NewContext(),
					&adminservice.AddOrUpdateRemoteClusterRequest{
						FrontendAddress:               remoteC.Host().RemoteFrontendGRPCAddress(),
						FrontendHttpAddress:           remoteC.Host().FrontendHTTPAddress(),
						EnableRemoteClusterConnection: true,
						EnableReplication:             true,
					})
				s.Require().NoError(err)
			}
		}
	}
	time.Sleep(time.Millisecond * 200)
}

func (s *RemoteClusterAuthSuite) SetupTest() {
	s.setupTest()
}

func (s *RemoteClusterAuthSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *RemoteClusterAuthSuite) TestAuthTokenSentOnRemoteClusterConnection() {
	var found bool
	s.capturedTokens.Range(func(key, value any) bool {
		s.Equal("Bearer test-xdc-jwt-token", value.(string))
		found = true
		return true
	})
	s.True(found, "expected auth token to be captured on at least one cluster")
}

func (s *RemoteClusterAuthSuite) TestAuthTokenSentDuringReplication() {
	s.capturedTokens = sync.Map{}

	ns := s.createGlobalNamespace()
	s.NotEmpty(ns)

	s.Eventually(func() bool {
		var found bool
		s.capturedTokens.Range(func(_, _ any) bool {
			found = true
			return false
		})
		return found
	}, 10*time.Second, 500*time.Millisecond, "expected auth token during replication")

	s.capturedTokens.Range(func(key, value any) bool {
		s.Equal("Bearer test-xdc-jwt-token", value.(string))
		return true
	})
}
