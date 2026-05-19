package tests

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
)

// This file tests the HistoryService's AddTasks API. It does this by starting a workflow, skipping its workflow task,
// and then adding the workflow task back to the queue via the AddTasks API. This should cause the workflow to be
// retried.

type (
	// AddTasksSuite is a separate suite because we need to override the history service's executable wrapper.
	AddTasksSuite struct {
		testcore.FunctionalTestBase

		worker       worker.Worker
		skippedTasks chan tasks.Task

		shouldSkip atomic.Bool
		workflowID atomic.Pointer[string]
	}
)

func TestAddTasksSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AddTasksSuite))
}

func (s *AddTasksSuite) SetupSuite() {
	s.SetupSuiteWithCluster()
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
		s.Run(tc.name, func() {
			// Register a workflow which does nothing.
			w := worker.New(s.SdkClient(), s.TaskQueue(), worker.Options{DeadlockDetectionTimeout: 0})
			myWorkflow := func(ctx workflow.Context) error {
				return nil
			}
			s.NoError(w.Start())
			defer w.Stop()
			w.RegisterWorkflow(myWorkflow)

			// Execute that workflow
			// We need to track the workflow ID so that we can filter out tasks from this test suite
			workflowID := uuid.NewString()
			s.workflowID.Store(&workflowID)
			s.shouldSkip.Store(true)
			s.InjectHook(testhooks.NewHook(testhooks.HistoryTaskInterceptor, func(executable any) (any, bool) {
				e, ok := executable.(queues.Executable)
				if !ok {
					return nil, false
				}
				task := e.GetTask()
				suiteWorkflowID := s.workflowID.Load()
				shouldExecute := (suiteWorkflowID != nil && task.GetWorkflowID() != *suiteWorkflowID) ||
					task.GetType() != enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK ||
					!s.shouldSkip.Load()
				if shouldExecute {
					return nil, false
				}
				s.skippedTasks <- task
				return queues.ExecuteResponse{}, true
			}))
			s.skippedTasks = make(chan tasks.Task)
			ctx := context.Background()
			timeout := 5 * debug.TimeoutMultiplier * time.Second
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: s.TaskQueue(),
			}, myWorkflow)
			s.NoError(err)

			// Get the task that we skipped, and add it back
			var task tasks.Task
			select {
			case task = <-s.skippedTasks:
			case <-ctx.Done():
				s.FailNow("timed out waiting for skipped task")
			}

			s.shouldSkip.Store(false)
			blob, err := serialization.NewSerializer().SerializeTask(task)
			s.NoError(err)
			shardID := tasks.GetShardIDForTask(task, int(s.GetTestClusterConfig().HistoryConfig.NumHistoryShards))
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
				_, err = s.GetTestCluster().AdminClient().AddTasks(ctx, request)
				s.NoError(err)
			}

			// Wait for the workflow to complete
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
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
	_, err := s.GetTestCluster().HistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
		ShardId: 0,
	})
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "invalid shardid")
}
