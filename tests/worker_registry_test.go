package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkerRegistryTestSuite struct {
	parallelsuite.Suite[*WorkerRegistryTestSuite]
}

func TestWorkerRegistryTestSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                                //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
	parallelsuite.RunLegacySequential(t, &WorkerRegistryTestSuite{}) //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
}

func (s *WorkerRegistryTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_DescribeWorker() {
	env := s.newTestEnv()

	// Record heartbeat for 2 workers
	worker1Key := env.Tv().WorkerIdentity()
	worker2Key := env.Tv().WorkerIdentity() + "_2"
	taskQueue1 := env.Tv().WithTaskQueueNumber(1).TaskQueue().Name
	taskQueue2 := env.Tv().WithTaskQueueNumber(2).TaskQueue().Name

	hbResp, err := env.FrontendClient().RecordWorkerHeartbeat(s.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: env.Namespace().String(),
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
	nonExistentWorkerKey := env.Tv().WorkerIdentity() + "_nonexistent"
	_, err = env.FrontendClient().DescribeWorker(s.Context(), &workflowservice.DescribeWorkerRequest{
		Namespace:         env.Namespace().String(),
		WorkerInstanceKey: nonExistentWorkerKey,
	})
	s.Error(err)
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	// Test error case - unknown namespace
	unknownNamespace := env.Tv().NamespaceName().String() + "_unknown"
	_, err = env.FrontendClient().DescribeWorker(s.Context(), &workflowservice.DescribeWorkerRequest{
		Namespace:         unknownNamespace,
		WorkerInstanceKey: worker1Key,
	})
	s.Error(err)
	var namespaceNotFound *serviceerror.NamespaceNotFound
	s.ErrorAs(err, &namespaceNotFound)

	// Test success case - verify worker1 heartbeat data
	{
		resp, err := env.FrontendClient().DescribeWorker(s.Context(), &workflowservice.DescribeWorkerRequest{
			Namespace:         env.Namespace().String(),
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
		resp, err := env.FrontendClient().DescribeWorker(s.Context(), &workflowservice.DescribeWorkerRequest{
			Namespace:         env.Namespace().String(),
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

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkersWithNoHeartbeats() {
	env := s.newTestEnv()

	resp, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
		Namespace: env.Namespace().String(),
		Query:     "TaskQueue='no-heartbeats-recorded-on-this-queue'",
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Empty(resp.GetWorkersInfo()) //nolint:staticcheck // testing deprecated field
	s.Empty(resp.GetWorkers())
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_DescribeWorkerWithNoHeartbeats() {
	env := s.newTestEnv()

	_, err := env.FrontendClient().DescribeWorker(s.Context(), &workflowservice.DescribeWorkerRequest{
		Namespace:         env.Namespace().String(),
		WorkerInstanceKey: "nonexistent-worker",
	})
	s.Error(err)
	var notFound *serviceerror.NotFound
	var namespaceNotFound *serviceerror.NamespaceNotFound
	s.True(
		errors.As(err, &notFound) || errors.As(err, &namespaceNotFound),
		"expected NotFound or NamespaceNotFound, got: %v", err,
	)
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkers() {
	env := s.newTestEnv()

	// Record heartbeats for 2 workers
	worker1Key := env.Tv().WorkerIdentity()
	worker2Key := env.Tv().WorkerIdentity() + "_2"
	sharedTaskQueue := env.Tv().TaskQueue().Name

	hbResp, err := env.FrontendClient().RecordWorkerHeartbeat(s.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: env.Namespace().String(),
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
		resp, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("WorkerInstanceKey='%s'", worker1Key),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetWorkersInfo(), 1)

		// Verify deprecated WorkersInfo field (backward compatibility)
		workerHeartbeat := resp.GetWorkersInfo()[0].GetWorkerHeartbeat()
		s.Equal(worker1Key, workerHeartbeat.WorkerInstanceKey)
		s.Equal(sharedTaskQueue, workerHeartbeat.TaskQueue)
		s.Equal(int32(1), workerHeartbeat.TotalStickyCacheHit)

		// Verify new Workers field (WorkerListInfo)
		s.Len(resp.GetWorkers(), 1)
		workerListInfo := resp.GetWorkers()[0]
		s.Equal(worker1Key, workerListInfo.GetWorkerInstanceKey())
		s.Equal(sharedTaskQueue, workerListInfo.GetTaskQueue())
	}
	{
		resp, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue),
		})
		s.NoError(err)
		s.NotNil(resp)

		// Verify deprecated WorkersInfo field
		workers := resp.GetWorkersInfo()
		workersByKey := make(map[string]*workerpb.WorkerHeartbeat)
		for _, workerInfo := range workers {
			heartbeat := workerInfo.GetWorkerHeartbeat()
			workersByKey[heartbeat.WorkerInstanceKey] = heartbeat
		}
		s.Len(workersByKey, 2)

		worker1, exists := workersByKey[worker1Key]
		s.True(exists, "worker1 should exist")
		s.Equal(sharedTaskQueue, worker1.TaskQueue)
		s.Equal(int32(1), worker1.TotalStickyCacheHit)

		worker2, exists := workersByKey[worker2Key]
		s.True(exists, "worker2 should exist")
		s.Equal(sharedTaskQueue, worker2.TaskQueue)
		s.Equal(int32(2), worker2.TotalStickyCacheHit)

		// Verify new Workers field (WorkerListInfo)
		listInfos := resp.GetWorkers()
		s.Len(listInfos, 2)
		listInfoByKey := make(map[string]*workerpb.WorkerListInfo)
		for _, info := range listInfos {
			listInfoByKey[info.GetWorkerInstanceKey()] = info
		}

		listInfo1, exists := listInfoByKey[worker1Key]
		s.True(exists, "worker1 list info should exist")
		s.Equal(sharedTaskQueue, listInfo1.GetTaskQueue())

		listInfo2, exists := listInfoByKey[worker2Key]
		s.True(exists, "worker2 list info should exist")
		s.Equal(sharedTaskQueue, listInfo2.GetTaskQueue())
	}
	{
		nonExistentWorkerKey := env.Tv().WorkerIdentity() + "_nonexistent"
		resp, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nonExistentWorkerKey),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Empty(resp.GetWorkersInfo()) //nolint:staticcheck // testing deprecated field
		s.Empty(resp.GetWorkers())
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_CountWorkers() {
	env := s.newTestEnv()

	worker1Key := env.Tv().WorkerIdentity()
	worker2Key := env.Tv().WorkerIdentity() + "_2"
	sysWorkerKey := env.Tv().WorkerIdentity() + "_sys"
	sharedTaskQueue := env.Tv().TaskQueue().Name
	otherTaskQueue := env.Tv().WithTaskQueueNumber(2).TaskQueue().Name

	hbResp, err := env.FrontendClient().RecordWorkerHeartbeat(s.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: env.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: worker1Key,
				TaskQueue:         sharedTaskQueue,
			},
			{
				WorkerInstanceKey: worker2Key,
				TaskQueue:         otherTaskQueue,
			},
			{
				WorkerInstanceKey: sysWorkerKey,
				TaskQueue:         "temporal-sys-per-ns-tq",
			},
		},
	})
	s.NoError(err)
	s.NotNil(hbResp)

	// Count all user workers (excludes system workers by default)
	{
		resp, err := env.FrontendClient().CountWorkers(s.Context(), &workflowservice.CountWorkersRequest{
			Namespace: env.Namespace().String(),
		})
		s.NoError(err)
		s.Equal(int64(2), resp.GetCount())
	}

	// Count all workers including system workers
	{
		resp, err := env.FrontendClient().CountWorkers(s.Context(), &workflowservice.CountWorkersRequest{
			Namespace:            env.Namespace().String(),
			IncludeSystemWorkers: true,
		})
		s.NoError(err)
		s.Equal(int64(3), resp.GetCount())
	}

	// Count with query filter
	{
		resp, err := env.FrontendClient().CountWorkers(s.Context(), &workflowservice.CountWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue),
		})
		s.NoError(err)
		s.Equal(int64(1), resp.GetCount())
	}

	// Count with query that matches no workers
	{
		resp, err := env.FrontendClient().CountWorkers(s.Context(), &workflowservice.CountWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     "WorkerInstanceKey='nonexistent'",
		})
		s.NoError(err)
		s.Equal(int64(0), resp.GetCount())
	}

	// Count with unknown field in query matches nothing (no error)
	{
		resp, err := env.FrontendClient().CountWorkers(s.Context(), &workflowservice.CountWorkersRequest{
			Namespace: env.Namespace().String(),
			Query:     "InvalidField='foo'",
		})
		s.NoError(err)
		s.Equal(int64(0), resp.GetCount())
	}
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_ListWorkersPagination() {
	env := s.newTestEnv()

	sharedTaskQueue := env.Tv().TaskQueue().Name

	// Create 5 workers with predictable keys for pagination testing
	workerKeys := make([]string, 5)
	heartbeats := make([]*workerpb.WorkerHeartbeat, 5)
	for i := range 5 {
		workerKeys[i] = fmt.Sprintf("%s_worker_%02d", env.Tv().WorkerIdentity(), i)
		heartbeats[i] = &workerpb.WorkerHeartbeat{
			WorkerInstanceKey:   workerKeys[i],
			TaskQueue:           sharedTaskQueue,
			TotalStickyCacheHit: int32(i),
		}
	}

	// Record all worker heartbeats
	hbResp, err := env.FrontendClient().RecordWorkerHeartbeat(s.Context(), &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace:       env.Namespace().String(),
		WorkerHeartbeat: heartbeats,
	})
	s.NoError(err)
	s.NotNil(hbResp)

	query := fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue)

	// Page 1: Request first 2 workers
	resp1, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
		Namespace: env.Namespace().String(),
		Query:     query,
		PageSize:  2,
	})
	s.NoError(err)
	s.Len(resp1.GetWorkersInfo(), 2)
	s.NotEmpty(resp1.GetNextPageToken(), "should have next page token")
	s.Equal(workerKeys[0], resp1.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
	s.Equal(workerKeys[1], resp1.GetWorkersInfo()[1].GetWorkerHeartbeat().GetWorkerInstanceKey())

	// Page 2: Request next 2 workers using token from page 1
	resp2, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
		Namespace:     env.Namespace().String(),
		Query:         query,
		PageSize:      2,
		NextPageToken: resp1.GetNextPageToken(),
	})
	s.NoError(err)
	s.Len(resp2.GetWorkersInfo(), 2)
	s.NotEmpty(resp2.GetNextPageToken(), "should have next page token")
	s.Equal(workerKeys[2], resp2.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
	s.Equal(workerKeys[3], resp2.GetWorkersInfo()[1].GetWorkerHeartbeat().GetWorkerInstanceKey())

	// Page 3: Request remaining workers using token from page 2
	resp3, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
		Namespace:     env.Namespace().String(),
		Query:         query,
		PageSize:      2,
		NextPageToken: resp2.GetNextPageToken(),
	})
	s.NoError(err)
	s.Len(resp3.GetWorkersInfo(), 1, "last page should have 1 worker")
	s.Empty(resp3.GetNextPageToken(), "should not have next page token on last page")
	s.Equal(workerKeys[4], resp3.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
}

func (s *WorkerRegistryTestSuite) TestWorkerRegistry_SendHeartbeatViaPollNexusTask() {
	env := s.newTestEnv()

	nexusWorkerKey := env.Tv().WorkerIdentity() + "_nexus"
	nexusTaskQueue := env.Tv().TaskQueue().Name

	heartbeat := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:   nexusWorkerKey,
		TaskQueue:           nexusTaskQueue,
		TotalStickyCacheHit: 3,
	}

	// Send worker heartbeat via PollNexusTaskQueue in a goroutine.
	// This is because PollNexusTaskQueue is a blocking call and we need to wait for the heartbeat to be registered.
	go func() {
		_, _ = env.FrontendClient().PollNexusTaskQueue(s.Context(), &workflowservice.PollNexusTaskQueueRequest{
			Namespace:       env.Namespace().String(),
			TaskQueue:       &taskqueuepb.TaskQueue{Name: nexusTaskQueue},
			Identity:        nexusWorkerKey,
			WorkerHeartbeat: []*workerpb.WorkerHeartbeat{heartbeat},
		})
	}()
	// Verify heartbeat was registered.
	s.Awaitf(
		func(s *WorkerRegistryTestSuite) {
			resp, err := env.FrontendClient().ListWorkers(s.Context(), &workflowservice.ListWorkersRequest{
				Namespace: env.Namespace().String(),
				Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nexusWorkerKey),
			})
			s.NoError(err)

			workers := resp.GetWorkersInfo() //nolint:staticcheck // SA1019: old worker registry API
			s.Len(workers, 1)

			workerHeartbeat := workers[0].GetWorkerHeartbeat()
			s.Equal(heartbeat.WorkerInstanceKey, workerHeartbeat.WorkerInstanceKey)
			s.Equal(heartbeat.TaskQueue, workerHeartbeat.TaskQueue)
			s.Equal(heartbeat.TotalStickyCacheHit, workerHeartbeat.TotalStickyCacheHit)
		}, 2*time.Minute, 100*time.Millisecond, "Worker heartbeat should be registered via PollNexusTaskQueue")
}
