package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WorkerRegistryTestSuite struct {
	testcore.FunctionalTestBase
	tv *testvars.TestVars
}

func TestWorkerRegistryTestSuite(t *testing.T) {
	s := new(WorkerRegistryTestSuite)
	suite.Run(t, s)
}

func (s *WorkerRegistryTestSuite) SetupTest() {
	s.OverrideDynamicConfig(dynamicconfig.ListWorkersEnabled, true)
	s.OverrideDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true)
	s.FunctionalTestBase.SetupTest()

	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(s.Namespace())
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_DescribeWorker() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker1", TaskQueue: "taskQueue1"},
			{WorkerInstanceKey: "worker2", TaskQueue: "taskQueue2"},
		},
	})
	s.NoError(err)

	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         s.Namespace().String(),
		WorkerInstanceKey: "worker0",
	})
	s.Error(err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         "unknown-namespace",
		WorkerInstanceKey: "worker1",
	})
	s.Error(err)
	var namespaceNotFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &namespaceNotFound)

	resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         s.Namespace().String(),
		WorkerInstanceKey: "worker1",
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkers() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{WorkerInstanceKey: "worker1", TaskQueue: "taskQueueTest"},
			{WorkerInstanceKey: "worker2", TaskQueue: "taskQueueTest"},
		},
	})
	s.NoError(err)

	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkerInstanceKey='worker1'",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 1, "Expected one worker with WorkerInstanceKey 'worker1'")
		s.Equal("worker1", resp.GetWorkersInfo()[0].GetWorkerHeartbeat().WorkerInstanceKey)
	}
	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "TaskQueue='taskQueueTest'",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 2, "Expected two workers with TaskQueue 'taskQueueTest'")
		s.Equal("taskQueueTest", resp.GetWorkersInfo()[0].GetWorkerHeartbeat().TaskQueue)
		s.Equal("taskQueueTest", resp.GetWorkersInfo()[1].GetWorkerHeartbeat().TaskQueue)
	}
	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkerInstanceKey='worker0'",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 0, "Expected no workers with WorkerInstanceKey 'worker0'")
	}
}
