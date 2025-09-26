package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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

	// Record heartbeat for 2 workers
	hbResp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey:   "worker1",
				TaskQueue:           "taskQueue1",
				TotalStickyCacheHit: 1,
			},
			{
				WorkerInstanceKey:   "worker2",
				TaskQueue:           "taskQueue2",
				TotalStickyCacheHit: 2,
			},
		},
	})
	s.NoError(err)
	s.NotNil(hbResp)

	// Test error case - worker that doesn't exist
	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         s.Namespace().String(),
		WorkerInstanceKey: "worker0",
	})
	s.Error(err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	// Test error case - unknown namespace
	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         "unknown-namespace",
		WorkerInstanceKey: "worker1",
	})
	s.Error(err)
	var namespaceNotFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &namespaceNotFound)

	// Test success case - verify worker1 heartbeat data
	{
		resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         s.Namespace().String(),
			WorkerInstanceKey: "worker1",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.GetWorkerInfo())

		workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
		s.NotNil(workerHeartbeat)
		s.Equal("worker1", workerHeartbeat.GetWorkerInstanceKey())
		s.Equal("taskQueue1", workerHeartbeat.GetTaskQueue())
		s.Equal(int32(1), workerHeartbeat.GetTotalStickyCacheHit())
	}
	// Test success case - verify worker2 heartbeat data
	{
		resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         s.Namespace().String(),
			WorkerInstanceKey: "worker2",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.GetWorkerInfo())

		workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
		s.NotNil(workerHeartbeat)
		s.Equal("worker2", workerHeartbeat.GetWorkerInstanceKey())
		s.Equal("taskQueue2", workerHeartbeat.GetTaskQueue())
		s.Equal(int32(2), workerHeartbeat.GetTotalStickyCacheHit())
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkers() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Record heartbeats for 2 workers
	hbResp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey:   "worker1",
				TaskQueue:           "taskQueue",
				TotalStickyCacheHit: 1,
			},
			{
				WorkerInstanceKey:   "worker2",
				TaskQueue:           "taskQueue",
				TotalStickyCacheHit: 2,
			},
		},
	})
	s.NoError(err)
	s.NotNil(hbResp)

	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkerInstanceKey='worker1'",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 1)

		workerHeartbeat := resp.GetWorkersInfo()[0].GetWorkerHeartbeat()
		s.Equal("worker1", workerHeartbeat.WorkerInstanceKey)
		s.Equal("taskQueue", workerHeartbeat.TaskQueue)
		s.Equal(int32(1), workerHeartbeat.TotalStickyCacheHit)
	}
	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "TaskQueue='taskQueue'",
		})
		s.NoError(err)
		s.NotNil(resp)

		workers := resp.GetWorkersInfo()
		// Collect workers by their instance key
		workersByKey := make(map[string]*workerpb.WorkerHeartbeat)
		for _, workerInfo := range workers {
			heartbeat := workerInfo.GetWorkerHeartbeat()
			workersByKey[heartbeat.WorkerInstanceKey] = heartbeat
		}

		// Verify we have exactly the workers we expect
		s.Len(workersByKey, 2)

		// Verify worker1
		worker1, exists := workersByKey["worker1"]
		s.True(exists, "worker1 should exist")
		s.Equal("taskQueue", worker1.TaskQueue)
		s.Equal(int32(1), worker1.TotalStickyCacheHit)

		// Verify worker2
		worker2, exists := workersByKey["worker2"]
		s.True(exists, "worker2 should exist")
		s.Equal("taskQueue", worker2.TaskQueue)
		s.Equal(int32(2), worker2.TotalStickyCacheHit)
	}
	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkerInstanceKey='worker0'",
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 0)
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_SendHeartbeatViaPollNexusTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	heartbeat := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:   "nexus-worker1",
		TaskQueue:           "nexusTaskQueue",
		TotalStickyCacheHit: 3,
	}

	// Send worker heartbeat via PollNexusTaskQueue in a goroutine.
	// This is because PollNexusTaskQueue is a blocking call and we need to wait for the heartbeat to be registered.
	go func() {
		_, _ = s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace:       s.Namespace().String(),
			TaskQueue:       &taskqueuepb.TaskQueue{Name: "nexusTaskQueue"},
			Identity:        "nexus-worker",
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{heartbeat},
		})
	}()

	// Use Eventually to verify heartbeat registration
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     "WorkerInstanceKey='nexus-worker1'",
		})
		if err != nil {
			fmt.Printf("ListWorkers error: %v\n", err)
			return false
		}

		workers := resp.GetWorkersInfo()
		if len(workers) != 1 {
			fmt.Printf("Expected 1 worker, got count %d\n", len(workers))
			return false
		}

		workerHeartbeat := workers[0].GetWorkerHeartbeat()

		// Check each field and log mismatches for easier debugging
		matches := workerHeartbeat.WorkerInstanceKey == heartbeat.WorkerInstanceKey &&
			workerHeartbeat.TaskQueue == heartbeat.TaskQueue &&
			workerHeartbeat.TotalStickyCacheHit == heartbeat.TotalStickyCacheHit

		if !matches {
			fmt.Printf("Worker heartbeat mismatch:\n  Expected: %+v\n  Got: %+v\n", heartbeat, workerHeartbeat)
		}
		return matches
	}, 2*time.Minute, 100*time.Millisecond, "Worker heartbeat should be registered via PollNexusTaskQueue")
}
