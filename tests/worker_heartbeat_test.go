package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type workerHeartbeatTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkerHeartbeatTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(workerHeartbeatTestSuite))
}

func (s *workerHeartbeatTestSuite) TestRecordHeartbeat() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		dynamicconfig.EnableWorkerStateTracking,
		true,
	)

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

	// TODO: Add verification that the worker entity was created in CHASM tree
	// Once the frontend handler is hooked up to the CHASM handler, this will
	// actually create/update worker entities. For now, this verifies the API works.
}

func (s *workerHeartbeatTestSuite) TestRecordMultipleHeartbeats() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(
		dynamicconfig.EnableChasm,
		true,
	)
	s.OverrideDynamicConfig(
		dynamicconfig.EnableWorkerStateTracking,
		true,
	)

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

	// Send second heartbeat for the same worker
	resp2, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// TODO: Verify that the second heartbeat updated the existing worker entity
	// rather than creating a new one. This would require querying the CHASM tree.
}
