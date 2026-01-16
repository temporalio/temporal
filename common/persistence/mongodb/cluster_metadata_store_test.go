package mongodb_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb"
)

type ClusterMetadataStoreSuite struct {
	suite.Suite
	factory *mongodb.Factory
	store   persistence.ClusterMetadataStore
	ctx     context.Context
	cancel  context.CancelFunc
	dbName  string
}

func TestClusterMetadataStoreSuite(t *testing.T) {
	suite.Run(t, new(ClusterMetadataStoreSuite))
}

func (s *ClusterMetadataStoreSuite) SetupSuite() {
	s.dbName = fmt.Sprintf("temporal_test_cluster_metadata_%d", time.Now().UnixNano())
	cfg := newMongoTestConfig(s.dbName)
	cfg.ConnectTimeout = 10 * time.Second

	logger := log.NewTestLogger()
	var err error
	s.factory, err = mongodb.NewFactory(cfg, "test-cluster", logger, metrics.NoopMetricsHandler)
	if err != nil {
		s.T().Skipf("Skipping test suite: %v", err)
	}
	s.Require().NoError(err)

	s.store, err = s.factory.NewClusterMetadataStore()
	s.Require().NoError(err)
}

func (s *ClusterMetadataStoreSuite) TearDownSuite() {
	if s.factory != nil {
		s.factory.Close()
	}
}

func (s *ClusterMetadataStoreSuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *ClusterMetadataStoreSuite) TearDownTest() {
	s.cancel()
}

func (s *ClusterMetadataStoreSuite) TestSaveAndGetClusterMetadata() {
	clusterName := "test-cluster-1"
	data := []byte("test-metadata-data")
	metadata := &commonpb.DataBlob{
		Data:         data,
		EncodingType: 1,
	}

	applied, err := s.store.SaveClusterMetadata(s.ctx, &persistence.InternalSaveClusterMetadataRequest{
		ClusterName:     clusterName,
		ClusterMetadata: metadata,
		Version:         0,
	})
	s.Require().NoError(err)
	s.Require().True(applied)

	resp, err := s.store.GetClusterMetadata(s.ctx, &persistence.InternalGetClusterMetadataRequest{
		ClusterName: clusterName,
	})
	s.Require().NoError(err)
	s.Require().Equal(data, resp.ClusterMetadata.Data)
	s.Require().Equal(int64(1), resp.Version)
}

func (s *ClusterMetadataStoreSuite) TestUpsertClusterMembership() {
	hostID := []byte("test-host-id-1")
	rpcAddress := net.ParseIP("127.0.0.1")

	err := s.store.UpsertClusterMembership(s.ctx, &persistence.UpsertClusterMembershipRequest{
		Role:         persistence.Frontend,
		HostID:       hostID,
		RPCAddress:   rpcAddress,
		RPCPort:      7233,
		SessionStart: time.Now().UTC(),
		RecordExpiry: 1 * time.Hour,
	})
	s.Require().NoError(err)

	resp, err := s.store.GetClusterMembers(s.ctx, &persistence.GetClusterMembersRequest{
		PageSize: 10,
	})
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(len(resp.ActiveMembers), 1)
}
