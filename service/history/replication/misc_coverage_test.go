package replication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/quotas"
	"go.uber.org/mock/gomock"
)

type miscCoverageSuite struct {
	suite.Suite
	*require.Assertions

	controller      *gomock.Controller
	clusterMetadata *cluster.MockMetadata
	clientBean      *client.MockBean
}

func TestMiscCoverageSuite(t *testing.T) {
	suite.Run(t, new(miscCoverageSuite))
}

func (s *miscCoverageSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
}

func (s *miscCoverageSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *miscCoverageSuite) TestRateLimiterProviders() {
	s.Equal(quotas.NoopRequestRateLimiter, quotas.RequestRateLimiter(ClientSchedulerRateLimiterProvider()))
	s.Equal(quotas.NoopRequestRateLimiter, quotas.RequestRateLimiter(ServerSchedulerRateLimiterProvider()))
	s.Equal(quotas.NoopRequestRateLimiter, quotas.RequestRateLimiter(PersistenceRateLimiterProvider()))
}

func (s *miscCoverageSuite) TestNoopDLQWriter() {
	var w DLQWriter = NoopDLQWriter{}
	s.NoError(w.WriteTaskToDLQ(context.Background(), DLQWriteRequest{}))
}

func (s *miscCoverageSuite) TestGRPCStreamClientProvider_Get_Success() {
	clientShardKey := ClusterShardKey{ClusterID: 1, ShardID: 2}
	serverShardKey := ClusterShardKey{ClusterID: 7, ShardID: 3}
	allClusterInfo := map[string]cluster.ClusterInformation{
		"remote-cluster": {
			Enabled:                true,
			InitialFailoverVersion: 7,
			ShardCount:             4,
		},
	}
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(allClusterInfo)
	adminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	s.clientBean.EXPECT().GetRemoteAdminClient("remote-cluster").Return(adminClient, nil)
	adminClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(nil, nil)

	provider := NewStreamBiDirectionStreamClientProvider(s.clusterMetadata, s.clientBean)
	_, err := provider.Get(context.Background(), clientShardKey, serverShardKey)
	s.NoError(err)
}

func (s *miscCoverageSuite) TestGRPCStreamClientProvider_Get_UnknownCluster() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	provider := NewStreamBiDirectionStreamClientProvider(s.clusterMetadata, s.clientBean)
	_, err := provider.Get(context.Background(), ClusterShardKey{}, ClusterShardKey{ClusterID: 99})
	s.Error(err)
	s.ErrorAs(err, new(*serviceerror.Internal))
}

func (s *miscCoverageSuite) TestGRPCStreamClientProvider_Get_AdminClientError() {
	allClusterInfo := map[string]cluster.ClusterInformation{
		"remote-cluster": {Enabled: true, InitialFailoverVersion: 7, ShardCount: 4},
	}
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(allClusterInfo)
	s.clientBean.EXPECT().GetRemoteAdminClient("remote-cluster").Return(nil, serviceerror.NewUnavailable("no client"))

	provider := NewStreamBiDirectionStreamClientProvider(s.clusterMetadata, s.clientBean)
	_, err := provider.Get(context.Background(), ClusterShardKey{}, ClusterShardKey{ClusterID: 7})
	s.Error(err)
}
