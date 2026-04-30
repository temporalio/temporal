package xdc

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	"google.golang.org/grpc/metadata"
)

const expectedXDCToken = "test-xdc-jwt-token"

type capturedCall struct {
	cluster string
	api     string
	token   string
}

type RemoteClusterAuthSuite struct {
	xdcBaseSuite
	captured sync.Map // map[string][]capturedCall keyed by cluster name
}

func TestRemoteClusterAuthSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &RemoteClusterAuthSuite{})
}

func (s *RemoteClusterAuthSuite) record(call capturedCall) {
	v, _ := s.captured.LoadOrStore(call.cluster, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	listV, _ := s.captured.LoadOrStore(call.cluster+":calls", &[]capturedCall{})
	list := listV.(*[]capturedCall)
	*list = append(*list, call)
}

func (s *RemoteClusterAuthSuite) callsFor(clusterName string) []capturedCall {
	listV, ok := s.captured.Load(clusterName + ":calls")
	if !ok {
		return nil
	}
	v, _ := s.captured.LoadOrStore(clusterName, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	list := listV.(*[]capturedCall)
	out := make([]capturedCall, len(*list))
	copy(out, *list)
	return out
}

func (s *RemoteClusterAuthSuite) clearCaptures() {
	s.captured = sync.Map{}
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
	s.Require().NoError(os.WriteFile(tokenFile, []byte(expectedXDCToken), 0600))

	suffix := common.GenerateRandomString(5)
	clusterNames := []string{"active_" + suffix, "standby_" + suffix}

	tokenProvider := &auth.FileTokenProvider{
		TokenFiles: map[string]string{
			clusterNames[0]: tokenFile,
			clusterNames[1]: tokenFile,
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

	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range clusterNames {
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
			return &authorization.Claims{System: authorization.RoleAdmin}, nil
		})
		c.Host().SetOnAuthorize(func(ctx context.Context, _ *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
			token := ""
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if vals := md.Get("authorization"); len(vals) > 0 {
					token = vals[0]
				}
			}
			if token != "" {
				s.record(capturedCall{cluster: clusterName, api: target.APIName, token: token})
			}
			return authorization.Result{Decision: authorization.DecisionAllow}, nil
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
	s.Require().Eventually(func() bool {
		for _, c := range s.clusters {
			resp, err := c.AdminClient().ListClusters(testcore.NewContext(), &adminservice.ListClustersRequest{})
			if err != nil {
				return false
			}
			seen := map[string]struct{}{}
			for _, m := range resp.GetClusters() {
				seen[m.GetClusterName()] = struct{}{}
			}
			for _, peer := range s.clusters {
				if _, ok := seen[peer.ClusterName()]; !ok {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "expected ListClusters on every cluster to include every peer after AddOrUpdateRemoteCluster")
}

func (s *RemoteClusterAuthSuite) SetupTest() {
	s.setupTest()
}

func (s *RemoteClusterAuthSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *RemoteClusterAuthSuite) TestAuthTokenSentOnRemoteClusterConnection() {
	var sawToken bool
	for _, c := range s.clusters {
		for _, call := range s.callsFor(c.ClusterName()) {
			if call.token == "Bearer "+expectedXDCToken {
				sawToken = true
				break
			}
		}
	}
	s.True(sawToken, "expected at least one cross-cluster RPC to carry the auth token")
}

// Asserts the bearer reaches the streaming replication path, not just unary RPCs.
func (s *RemoteClusterAuthSuite) TestAuthTokenSentOnStreamingRPC() {
	s.clearCaptures()

	ns := s.createGlobalNamespace()
	s.NotEmpty(ns)

	s.Eventually(func() bool {
		for _, c := range s.clusters {
			for _, call := range s.callsFor(c.ClusterName()) {
				if strings.Contains(call.api, "Stream") && call.token == "Bearer "+expectedXDCToken {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "expected streaming RPC to carry the auth token after global namespace creation")
}

// Receiver-side rejection is covered by TestTokenAuthHeader_ReceiverRejectsWrongToken in common/rpc/test;
// asserting it cleanly here would require driving a post-discovery RPC, and replication's async retries make that flaky.
