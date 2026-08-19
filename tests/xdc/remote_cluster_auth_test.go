package xdc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"slices"
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
	"go.temporal.io/server/common/rpc/auth/authtest"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/testutils"
	"google.golang.org/grpc/metadata"
)

const expectedXDCToken = "test-xdc-jwt-token"

// newSharedTLSProvider generates one cert chain for 127.0.0.1 and returns a TLS provider
// usable as both server and client by every cluster in the suite. The same chain on both
// sides means cluster A trusts cluster B's cert and vice versa without further wiring.
func newSharedTLSProvider(t *testing.T) *encryption.FixedTLSConfigProvider {
	t.Helper()
	chain, err := testutils.GenerateTestChain(t.TempDir(), "127.0.0.1")
	if err != nil {
		t.Fatalf("generate test chain: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(chain.CertPubFile, chain.CertKeyFile)
	if err != nil {
		t.Fatalf("load cert/key: %v", err)
	}
	caBytes, err := os.ReadFile(chain.CaPubFile)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		t.Fatal("append CA to pool")
	}
	serverCfg := &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	clientCfg := &tls.Config{ServerName: "127.0.0.1", Certificates: []tls.Certificate{cert}, RootCAs: pool}
	return &encryption.FixedTLSConfigProvider{
		InternodeServerConfig:      serverCfg,
		InternodeClientConfig:      clientCfg,
		FrontendServerConfig:       serverCfg,
		FrontendClientConfig:       clientCfg,
		RemoteClusterClientConfigs: map[string]*tls.Config{"127.0.0.1": clientCfg},
	}
}

type capturedCall struct {
	cluster string
	api     string
	token   string
}

type RemoteClusterAuthSuite struct {
	xdcBaseSuite
	capturedMu sync.Mutex
	captured   map[string][]capturedCall
}

func TestRemoteClusterAuthSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &RemoteClusterAuthSuite{})
}

func (s *RemoteClusterAuthSuite) record(call capturedCall) {
	s.capturedMu.Lock()
	defer s.capturedMu.Unlock()
	if s.captured == nil {
		s.captured = make(map[string][]capturedCall)
	}
	s.captured[call.cluster] = append(s.captured[call.cluster], call)
}

func (s *RemoteClusterAuthSuite) callsFor(clusterName string) []capturedCall {
	s.capturedMu.Lock()
	defer s.capturedMu.Unlock()
	return slices.Clone(s.captured[clusterName])
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

	tokenProvider := authtest.StaticTokenProvider(expectedXDCToken)

	// Production TokenCredentials require TLS; both clusters share one cert chain so they trust each other's cert.
	tlsProvider := newSharedTLSProvider(s.T())

	persistenceDefaults := testcore.GetPersistenceTestDefaults()
	clusterConfigs := []*testcore.TestClusterConfig{
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig:     testcore.HistoryConfig{NumHistoryShards: 1},
			Persistence:       persistenceDefaults,
			TokenProvider:     tokenProvider,
			TLSConfigProvider: tlsProvider,
		},
		{
			ClusterMetadata: cluster.Config{
				EnableGlobalNamespace:    true,
				FailoverVersionIncrement: 10,
			},
			HistoryConfig:     testcore.HistoryConfig{NumHistoryShards: 1},
			Persistence:       persistenceDefaults,
			TokenProvider:     tokenProvider,
			TLSConfigProvider: tlsProvider,
		},
	}

	s.clusters = make([]*testcore.TestCluster, len(clusterConfigs))

	suffix := common.GenerateRandomString(5)
	testClusterFactory := testcore.NewTestClusterFactory()
	for clusterIndex, clusterName := range []string{"active_" + suffix, "standby_" + suffix} {
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
	await.RequireTruef(s.T(), func() bool {
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
	// s.captured is populated by background goroutines (cluster metadata refresh, replication setup);
	// a single-shot read can race their first invocation, especially under CI load.
	await.RequireTruef(s.T(), func() bool {
		for _, c := range s.clusters {
			for _, call := range s.callsFor(c.ClusterName()) {
				if call.token == "Bearer "+expectedXDCToken {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 250*time.Millisecond, "expected at least one cross-cluster RPC to carry the auth token")
}

// Asserts the bearer reaches the streaming replication path, not just unary RPCs.
// Replication streams are opened once at peer-registration time and reused; we don't clear
// captures here so the initial open is observable.
func (s *RemoteClusterAuthSuite) TestAuthTokenSentOnStreamingRPC() {
	await.RequireTruef(s.T(), func() bool {
		for _, c := range s.clusters {
			for _, call := range s.callsFor(c.ClusterName()) {
				if strings.Contains(call.api, "StreamWorkflowReplicationMessages") && call.token == "Bearer "+expectedXDCToken {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 250*time.Millisecond, "expected StreamWorkflowReplicationMessages to carry the auth token")
}

// Receiver-side rejection is covered by TestTokenAuthHeader_ReceiverRejectsWrongToken in common/rpc/test;
// asserting it cleanly here would require driving a post-discovery RPC, and replication's async retries make that flaky.
