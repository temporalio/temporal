package testcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestPreseededNamespaceCreateRequest(t *testing.T) {
	seed := newPreseededNamespace(namespace.Name("preseeded"))

	request := seed.createRequest(cluster.TestCurrentClusterName)

	require.Equal(t, seed.id.String(), request.Namespace.Info.Id)
	require.Equal(t, "preseeded", request.Namespace.Info.Name)
	require.Equal(t, cluster.TestCurrentClusterName, request.Namespace.ReplicationConfig.ActiveClusterName)
	expectedSearchAttributes := searchattribute.TestSearchAttributesToRegister()
	require.Len(t, request.Namespace.Config.CustomSearchAttributeAliases, len(expectedSearchAttributes))
	for field, alias := range searchattribute.TestAliases {
		if _, ok := expectedSearchAttributes[alias]; ok {
			require.Equal(t, alias, request.Namespace.Config.CustomSearchAttributeAliases[field])
		}
	}
}

type FunctionalTestBaseSuite struct {
	FunctionalTestBase
}

func TestFunctionalTestBaseSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &FunctionalTestBaseSuite{})
}

func (s *FunctionalTestBaseSuite) SetupSuite() {
	s.SetupSuiteWithCluster()
}

func (s *FunctionalTestBaseSuite) TearDownSuite() {
	s.TearDownCluster()
}

func (s *FunctionalTestBaseSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

func (s *FunctionalTestBaseSuite) TestWorkerServiceHealthCheck() {
	// This test verifies that the worker service exposes a working gRPC health check endpoint.
	conn, err := grpc.NewClient(
		s.WorkerGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.NoError(err)
	defer func() { _ = conn.Close() }()

	healthClient := healthpb.NewHealthClient(conn)
	s.Eventually(
		func() bool {
			resp, err := healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{
				Service: worker.ServiceName,
			})
			return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
		},
		10*time.Second,
		100*time.Millisecond,
	)
}
