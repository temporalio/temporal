package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

func TestWorkerRegistry(t *testing.T) {
	t.Run("DescribeWorker", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.ListWorkersEnabled, true),
			testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tv := s.Tv()

		// Record heartbeat for 2 workers
		worker1Key := tv.WorkerIdentity()
		worker2Key := tv.WorkerIdentity() + "_2"
		taskQueue1 := tv.WithTaskQueueNumber(1).TaskQueue().Name
		taskQueue2 := tv.WithTaskQueueNumber(2).TaskQueue().Name

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
		require.NoError(t, err)
		require.NotNil(t, hbResp)

		// Test error case - worker that doesn't exist
		nonExistentWorkerKey := tv.WorkerIdentity() + "_nonexistent"
		_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         s.Namespace().String(),
			WorkerInstanceKey: nonExistentWorkerKey,
		})
		require.Error(t, err)
		var notFound *serviceerror.NotFound
		require.ErrorAs(t, err, &notFound)

		// Test error case - unknown namespace
		unknownNamespace := s.Namespace().String() + "_unknown"
		_, err = s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
			Namespace:         unknownNamespace,
			WorkerInstanceKey: worker1Key,
		})
		require.Error(t, err)
		var namespaceNotFound *serviceerror.NamespaceNotFound
		require.ErrorAs(t, err, &namespaceNotFound)

		// Test success case - verify worker1 heartbeat data
		{
			resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
				Namespace:         s.Namespace().String(),
				WorkerInstanceKey: worker1Key,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetWorkerInfo())

			workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
			require.NotNil(t, workerHeartbeat)
			require.Equal(t, worker1Key, workerHeartbeat.GetWorkerInstanceKey())
			require.Equal(t, taskQueue1, workerHeartbeat.GetTaskQueue())
			require.Equal(t, int32(1), workerHeartbeat.GetTotalStickyCacheHit())
		}
		// Test success case - verify worker2 heartbeat data
		{
			resp, err := s.FrontendClient().DescribeWorker(ctx, &workflowservice.DescribeWorkerRequest{
				Namespace:         s.Namespace().String(),
				WorkerInstanceKey: worker2Key,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.GetWorkerInfo())

			workerHeartbeat := resp.GetWorkerInfo().GetWorkerHeartbeat()
			require.NotNil(t, workerHeartbeat)
			require.Equal(t, worker2Key, workerHeartbeat.GetWorkerInstanceKey())
			require.Equal(t, taskQueue2, workerHeartbeat.GetTaskQueue())
			require.Equal(t, int32(2), workerHeartbeat.GetTotalStickyCacheHit())
		}
	})

	t.Run("ListWorkers", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.ListWorkersEnabled, true),
			testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tv := s.Tv()

		// Record heartbeats for 2 workers
		worker1Key := tv.WorkerIdentity()
		worker2Key := tv.WorkerIdentity() + "_2"
		sharedTaskQueue := tv.TaskQueue().Name

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
		require.NoError(t, err)
		require.NotNil(t, hbResp)

		{
			resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("WorkerInstanceKey='%s'", worker1Key),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.GetWorkersInfo(), 1)

			workerHeartbeat := resp.GetWorkersInfo()[0].GetWorkerHeartbeat()
			require.Equal(t, worker1Key, workerHeartbeat.WorkerInstanceKey)
			require.Equal(t, sharedTaskQueue, workerHeartbeat.TaskQueue)
			require.Equal(t, int32(1), workerHeartbeat.TotalStickyCacheHit)
		}
		{
			resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)

			workers := resp.GetWorkersInfo()
			// Collect workers by their instance key
			workersByKey := make(map[string]*workerpb.WorkerHeartbeat)
			for _, workerInfo := range workers {
				heartbeat := workerInfo.GetWorkerHeartbeat()
				workersByKey[heartbeat.WorkerInstanceKey] = heartbeat
			}

			// Verify we have exactly the workers we expect
			require.Len(t, workersByKey, 2)

			// Verify worker1
			worker1, exists := workersByKey[worker1Key]
			require.True(t, exists, "worker1 should exist")
			require.Equal(t, sharedTaskQueue, worker1.TaskQueue)
			require.Equal(t, int32(1), worker1.TotalStickyCacheHit)

			// Verify worker2
			worker2, exists := workersByKey[worker2Key]
			require.True(t, exists, "worker2 should exist")
			require.Equal(t, sharedTaskQueue, worker2.TaskQueue)
			require.Equal(t, int32(2), worker2.TotalStickyCacheHit)
		}
		{
			nonExistentWorkerKey := tv.WorkerIdentity() + "_nonexistent"
			resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nonExistentWorkerKey),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.GetWorkersInfo(), 0)
		}
	})

	t.Run("ListWorkersPagination", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.ListWorkersEnabled, true),
			testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tv := s.Tv()
		sharedTaskQueue := tv.TaskQueue().Name

		// Create 5 workers with predictable keys for pagination testing
		workerKeys := make([]string, 5)
		heartbeats := make([]*workerpb.WorkerHeartbeat, 5)
		for i := 0; i < 5; i++ {
			workerKeys[i] = fmt.Sprintf("%s_worker_%02d", tv.WorkerIdentity(), i)
			heartbeats[i] = &workerpb.WorkerHeartbeat{
				WorkerInstanceKey:   workerKeys[i],
				TaskQueue:           sharedTaskQueue,
				TotalStickyCacheHit: int32(i),
			}
		}

		// Record all worker heartbeats
		hbResp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
			Namespace:       s.Namespace().String(),
			WorkerHeartbeat: heartbeats,
		})
		require.NoError(t, err)
		require.NotNil(t, hbResp)

		query := fmt.Sprintf("TaskQueue='%s'", sharedTaskQueue)

		// Page 1: Request first 2 workers
		resp1, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace: s.Namespace().String(),
			Query:     query,
			PageSize:  2,
		})
		require.NoError(t, err)
		require.Len(t, resp1.GetWorkersInfo(), 2)
		require.NotEmpty(t, resp1.GetNextPageToken(), "should have next page token")
		require.Equal(t, workerKeys[0], resp1.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
		require.Equal(t, workerKeys[1], resp1.GetWorkersInfo()[1].GetWorkerHeartbeat().GetWorkerInstanceKey())

		// Page 2: Request next 2 workers using token from page 1
		resp2, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace:     s.Namespace().String(),
			Query:         query,
			PageSize:      2,
			NextPageToken: resp1.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp2.GetWorkersInfo(), 2)
		require.NotEmpty(t, resp2.GetNextPageToken(), "should have next page token")
		require.Equal(t, workerKeys[2], resp2.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
		require.Equal(t, workerKeys[3], resp2.GetWorkersInfo()[1].GetWorkerHeartbeat().GetWorkerInstanceKey())

		// Page 3: Request remaining workers using token from page 2
		resp3, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
			Namespace:     s.Namespace().String(),
			Query:         query,
			PageSize:      2,
			NextPageToken: resp2.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Len(t, resp3.GetWorkersInfo(), 1, "last page should have 1 worker")
		require.Empty(t, resp3.GetNextPageToken(), "should not have next page token on last page")
		require.Equal(t, workerKeys[4], resp3.GetWorkersInfo()[0].GetWorkerHeartbeat().GetWorkerInstanceKey())
	})

	t.Run("SendHeartbeatViaPollNexusTask", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.ListWorkersEnabled, true),
			testcore.WithDynamicConfig(dynamicconfig.WorkerHeartbeatsEnabled, true),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		tv := s.Tv()
		nexusWorkerKey := tv.WorkerIdentity() + "_nexus"
		nexusTaskQueue := tv.TaskQueue().Name

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
		s.EventuallyWithT(func(ct *assert.CollectT) {
			resp, err := s.FrontendClient().ListWorkers(ctx, &workflowservice.ListWorkersRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("WorkerInstanceKey='%s'", nexusWorkerKey),
			})
			require.NoError(ct, err)

			workers := resp.GetWorkersInfo()
			require.Len(ct, workers, 1)

			workerHeartbeat := workers[0].GetWorkerHeartbeat()
			require.Equal(ct, heartbeat.WorkerInstanceKey, workerHeartbeat.WorkerInstanceKey)
			require.Equal(ct, heartbeat.TaskQueue, workerHeartbeat.TaskQueue)
			require.Equal(ct, heartbeat.TotalStickyCacheHit, workerHeartbeat.TotalStickyCacheHit)
		}, 2*time.Minute, 100*time.Millisecond, "Worker heartbeat should be registered via PollNexusTaskQueue")
	})
}
