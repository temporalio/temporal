package testcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type FunctionalTestBaseSuite struct {
	FunctionalTestBase

	frontendServiceName primitives.ServiceName
	matchingServiceName primitives.ServiceName
	historyServiceName  primitives.ServiceName
	workerServiceName   primitives.ServiceName
}

func TestFunctionalTestBaseSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &FunctionalTestBaseSuite{})
}

func (s *FunctionalTestBaseSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		WithFxOptionsForService(primitives.FrontendService, fx.Populate(&s.frontendServiceName)),
		WithFxOptionsForService(primitives.MatchingService, fx.Populate(&s.matchingServiceName)),
		WithFxOptionsForService(primitives.HistoryService, fx.Populate(&s.historyServiceName)),
		WithFxOptionsForService(primitives.WorkerService, fx.Populate(&s.workerServiceName)),
	)
}

func (s *FunctionalTestBaseSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownCluster()
}

func (s *FunctionalTestBaseSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

func (s *FunctionalTestBaseSuite) TestWithFxOptionsForService() {
	// This test works by using the WithFxOptionsForService option to obtain the ServiceName from the graph, and then
	// it verifies that the ServiceName is correct. It does this because we are targeting the fx.App for a particular
	// service, so we'll know our fx options were provided to the right service if, when we use them to get the current
	// service name, it matches the target service. A more realistic example would use the option to obtain an actual
	// useful object like a history shard controller, or do some graph modifications with fx.Decorate.

	s.Equal(primitives.FrontendService, s.frontendServiceName)
	s.Equal(primitives.MatchingService, s.matchingServiceName)
	s.Equal(primitives.HistoryService, s.historyServiceName)
	s.Equal(primitives.WorkerService, s.workerServiceName)
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
