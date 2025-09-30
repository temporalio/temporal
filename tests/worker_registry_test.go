package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	worker1Key := s.tv.WorkerIdentity()
	worker2Key := s.tv.WorkerIdentity() + "_2"
	taskQueue1 := s.tv.WithTaskQueueNumber(1).TaskQueue().Name
	taskQueue2 := s.tv.WithTaskQueueNumber(2).TaskQueue().Name

	hbResp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey:   worker1Key,
				TaskQueue:           taskQueue1,
				TotalStickyCacheHit: 1,
			},
			{
				WorkerInstanceKey:   worker2Key,
				TaskQueue:           taskQueue2,
				TotalStickyCacheHit: 2,
			},
		},
	})
	s.NoError(err)
	s.NotNil(hbResp)

	// Test error case - worker that doesn't exist
	nonExistentWorkerKey := s.tv.WorkerIdentity() + "_nonexistent"
	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         s.Namespace().String(),
		WorkerInstanceKey: nonExistentWorkerKey,
	})
	s.Error(err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	// Test error case - unknown namespace
	unknownNamespace := s.tv.NamespaceName().String() + "_unknown"
	_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
		Namespace:         unknownNamespace,
		WorkerInstanceKey: worker1Key,
	})
	s.Error(err)
	var namespaceNotFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &namespaceNotFound)

	// Test success case - verify worker1 heartbeat data
	{
		resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         s.Namespace().String(),
			WorkerInstanceKey: worker1Key,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.GetWorkerInfo())

		workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
		s.NotNil(workerHeartbeat)
		s.Equal(worker1Key, workerHeartbeat.GetWorkerInstanceKey())
		s.Equal(taskQueue1, workerHeartbeat.GetTaskQueue())
		s.Equal(int32(1), workerHeartbeat.GetTotalStickyCacheHit())
	}
	// Test success case - verify worker2 heartbeat data
	{
		resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         s.Namespace().String(),
			WorkerInstanceKey: worker2Key,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.GetWorkerInfo())

		workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
		s.NotNil(workerHeartbeat)
		s.Equal(worker2Key, workerHeartbeat.GetWorkerInstanceKey())
		s.Equal(taskQueue2, workerHeartbeat.GetTaskQueue())
		s.Equal(int32(2), workerHeartbeat.GetTotalStickyCacheHit())
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkers() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Record heartbeats for 2 workers
	worker1Key := s.tv.WorkerIdentity()
	worker2Key := s.tv.WorkerIdentity() + "_2"
	sharedTaskQueue := s.tv.TaskQueue().Name

	hbResp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey:   worker1Key,
				TaskQueue:           sharedTaskQueue,
				TotalStickyCacheHit: 1,
			},
			{
				WorkerInstanceKey:   worker2Key,
				TaskQueue:           sharedTaskQueue,
				TotalStickyCacheHit: 2,
			},
		},
	})
	s.NoError(err)
	s.NotNil(hbResp)

	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("WorkerInstanceKey='%s'", worker1Key),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 1)

		workerHeartbeat := resp.GetWorkersInfo()[0].GetWorkerHeartbeat()
		s.Equal(worker1Key, workerHeartbeat.WorkerInstanceKey)
		s.Equal(sharedTaskQueue, workerHeartbeat.TaskQueue)
		s.Equal(int32(1), workerHeartbeat.TotalStickyCacheHit)
	}
	{
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue),
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
		worker1, exists := workersByKey[worker1Key]
		s.True(exists, "worker1 should exist")
		s.Equal(sharedTaskQueue, worker1.TaskQueue)
		s.Equal(int32(1), worker1.TotalStickyCacheHit)

		// Verify worker2
		worker2, exists := workersByKey[worker2Key]
		s.True(exists, "worker2 should exist")
		s.Equal(sharedTaskQueue, worker2.TaskQueue)
		s.Equal(int32(2), worker2.TotalStickyCacheHit)
	}
	{
		nonExistentWorkerKey := s.tv.WorkerIdentity() + "_nonexistent"
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nonExistentWorkerKey),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 0)
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_SendHeartbeatViaPollNexusTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nexusWorkerKey := s.tv.WorkerIdentity() + "_nexus"
	nexusTaskQueue := s.tv.TaskQueue().Name

	heartbeat := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:   nexusWorkerKey,
		TaskQueue:           nexusTaskQueue,
		TotalStickyCacheHit: 3,
	}

	// Send worker heartbeat via PollNexusTaskQueue in a goroutine.
	// This is because PollNexusTaskQueue is a blocking call and we need to wait for the heartbeat to be registered.
	go func() {
		_, _ = s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace:       s.Namespace().String(),
			TaskQueue:       &taskqueuepb.TaskQueue{Name: nexusTaskQueue},
			Identity:        nexusWorkerKey,
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{heartbeat},
		})
	}()

	// Verify heartbeat was registered.
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nexusWorkerKey),
		})
		require.NoError(t, err)

		workers := resp.GetWorkersInfo()
		require.Len(t, workers, 1)

		workerHeartbeat := workers[0].GetWorkerHeartbeat()
		require.Equal(t, heartbeat.WorkerInstanceKey, workerHeartbeat.WorkerInstanceKey)
		require.Equal(t, heartbeat.TaskQueue, workerHeartbeat.TaskQueue)
		require.Equal(t, heartbeat.TotalStickyCacheHit, workerHeartbeat.TotalStickyCacheHit)
	}, 2*time.Minute, 100*time.Millisecond, "Worker heartbeat should be registered via PollNexusTaskQueue")
}
