package tests

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
)

// This file tests the HistoryService's AddTasks API. It does this by starting a workflow, skipping its workflow task,
// and then adding the workflow task back to the queue via the AddTasks API. This should cause the workflow to be
// retried.

type (
	AddTasksSuite struct {
		parallelsuite.Suite[*AddTasksSuite]
	}
)

func TestAddTasksSuite(t *testing.T) {
	parallelsuite.Run(t, &AddTasksSuite{})
}

func (s *AddTasksSuite) TestAddTasks_Ok() {
	for _, tc := range []struct {
		name               string
		shouldCallAddTasks bool
	}{
		{
			name:               "CallAddTasks",
			shouldCallAddTasks: true,
		},
		{
			name:               "DontCallAddTasks",
			shouldCallAddTasks: false,
		},
	} {
		s.Run(tc.name, func(s *AddTasksSuite) {
			env := testcore.NewEnv(s.T())

			// Inject a history task interceptor that skips transfer workflow tasks.
			var shouldSkip atomic.Bool
			skippedTasks := make(chan tasks.Task)
			env.InjectHook(testhooks.NewHook(
				testhooks.HistoryTransferTaskInterceptor,
				func(task tasks.Task, execute func()) {
					if task.GetType() != enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK || !shouldSkip.Load() {
						execute()
						return
					}
					skippedTasks <- task
				},
			))

			// Register a workflow which does nothing.
			w := worker.New(env.SdkClient(), env.WorkerTaskQueue(), worker.Options{DeadlockDetectionTimeout: 0})
			myWorkflow := func(ctx workflow.Context) error {
				return nil
			}
			s.NoError(w.Start())
			defer w.Stop()
			w.RegisterWorkflow(myWorkflow)

			// Execute that workflow.
			workflowID := uuid.NewString()
			shouldSkip.Store(true)

			run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: env.WorkerTaskQueue(),
			}, myWorkflow)
			s.NoError(err)

			// Get the task that we skipped, and add it back
			var task tasks.Task
			select {
			case task = <-skippedTasks:
			case <-s.Context().Done():
				s.FailNow("timed out waiting for skipped task")
			}

			shouldSkip.Store(false)
			blob, err := serialization.NewSerializer().SerializeTask(task)
			s.NoError(err)
			shardID := tasks.GetShardIDForTask(task, int(env.GetTestClusterConfig().HistoryConfig.NumHistoryShards))
			request := &adminservice.AddTasksRequest{
				ShardId: int32(shardID),
				Tasks: []*adminservice.AddTasksRequest_Task{
					{
						CategoryId: int32(task.GetCategory().ID()),
						Blob:       blob,
					},
				},
			}
			if tc.shouldCallAddTasks {
				_, err = env.GetTestCluster().AdminClient().AddTasks(s.Context(), request)
				s.NoError(err)
			}

			// Wait for the workflow to complete
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = run.Get(ctx, nil)
			if tc.shouldCallAddTasks {
				s.NoError(err, "workflow task should be retried if we call AddTasks")
			} else {
				s.Error(err, "workflow should not complete if we don't call AddTasks")
			}
		})
	}
}

func (s *AddTasksSuite) TestAddTasks_ErrGetShardByID() {
	env := testcore.NewEnv(s.T())
	_, err := env.GetTestCluster().HistoryClient().AddTasks(s.Context(), &historyservice.AddTasksRequest{
		ShardId: 0,
	})
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "invalid shardid")
}
