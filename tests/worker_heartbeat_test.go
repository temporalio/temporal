package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	chasmworker "go.temporal.io/server/chasm/lib/worker"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type workerHeartbeatTestSuite struct {
	testcore.FunctionalTestBase

	chasmEngine chasm.Engine
}

func TestWorkerHeartbeatTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(workerHeartbeatTestSuite))
}

func (s *workerHeartbeatTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():               true,
			dynamicconfig.EnableWorkerStateTracking.Key(): true,
		}),
	)

	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *workerHeartbeatTestSuite) TestRecordHeartbeat() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	tv := testvars.New(t)
	workerInstanceKey := tv.Any().String()

	resp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify the worker entity was created in CHASM
	engineCtx := chasm.NewEngineContext(ctx, s.chasmEngine)
	worker, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, worker)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Equal(t, workerInstanceKey, worker.GetWorkerHeartbeat().GetWorkerInstanceKey())
	require.NotNil(t, worker.GetLeaseExpirationTime())
}

func (s *workerHeartbeatTestSuite) TestRecordMultipleHeartbeats() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	tv := testvars.New(t)
	workerInstanceKey := tv.Any().String()

	req := &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	}

	// Send first heartbeat
	resp1, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp1)

	// Read worker state after first heartbeat
	engineCtx := chasm.NewEngineContext(ctx, s.chasmEngine)
	worker1, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, worker1)
	firstLeaseExpiration := worker1.GetLeaseExpirationTime().AsTime()

	// Wait a bit to ensure lease expiration time will be different
	time.Sleep(100 * time.Millisecond)

	// Send second heartbeat for the same worker
	resp2, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Verify that the second heartbeat updated the existing worker entity
	worker2, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, worker2)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker2.Status)
	require.Equal(t, workerInstanceKey, worker2.GetWorkerHeartbeat().GetWorkerInstanceKey())

	// Verify lease expiration time was updated (extended)
	secondLeaseExpiration := worker2.GetLeaseExpirationTime().AsTime()
	require.True(t, secondLeaseExpiration.After(firstLeaseExpiration),
		"Second heartbeat should extend the lease expiration time")
}
