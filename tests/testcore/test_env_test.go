package testcore

import (
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type TestEnvSuite struct {
	parallelsuite.Suite[*TestEnvSuite]
}

func TestTestEnvSuite(t *testing.T) {
	parallelsuite.Run(t, &TestEnvSuite{})
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorWithoutExplicitRequest() {
	guard := newDedicatedClusterGuard(false)

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_FailsWhenUnused() {
	guard := newDedicatedClusterGuard(true)

	s.EqualError(guard.validate(),
		`testcore.WithDedicatedCluster() was requested but no dedicated-cluster-only feature was used`)
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_NoErrorAfterUse() {
	guard := newDedicatedClusterGuard(true)
	guard.record("global hook")

	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestDedicatedClusterGuard_ConcurrentRecord() {
	guard := newDedicatedClusterGuard(true)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			guard.record("reason")
		})
	}
	wg.Wait()
	s.NoError(guard.validate())
}

func (s *TestEnvSuite) TestWorkerServiceStartsByDefault() {
	env := NewEnv(s.T())
	conn, err := grpc.NewClient(
		env.WorkerGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(conn.Close()) }()

	healthClient := healthpb.NewHealthClient(conn)
	s.Await(
		func(s *TestEnvSuite) {
			resp, err := healthClient.Check(s.Context(), &healthpb.HealthCheckRequest{
				Service: worker.ServiceName,
			})
			s.NoError(err)
			s.Equal(healthpb.HealthCheckResponse_SERVING, resp.Status)
		},
		10*time.Second,
		100*time.Millisecond,
	)
}
