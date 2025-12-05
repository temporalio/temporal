package tests

import (
	"context"
	"errors"
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
	"go.temporal.io/server/common/primitives"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
)

// This file tests the HistoryService's AddTasks API. It does this by starting a workflow, skipping its workflow task,
// and then adding the workflow task back to the queue via the AddTasks API. This should cause the workflow to be
// retried.

type (
	// AddTasksSuite is a separate suite because we need to override the history service's executable wrapper.
	AddTasksSuite struct {
		testcore.FunctionalTestBase

		shardController *faultyShardController
		worker          worker.Worker
		skippedTasks    chan tasks.Task

		shouldSkip   atomic.Bool
		getEngineErr atomic.Pointer[error]
		workflowID   atomic.Pointer[string]
	}
	faultyShardController struct {
		shard.Controller
		s *AddTasksSuite
	}
	faultyShardContext struct {
		historyi.ShardContext
		suite *AddTasksSuite
	}
	// executorWrapper is used to wrap any [queues.Executable] that the history service makes so that we can intercept
	// workflow tasks.
	executorWrapper struct {
		s *AddTasksSuite
	}
	// noopExecutor skips any workflow task which meets the criteria specified in shouldExecute and records them to
	// the tasks channel.
	noopExecutor struct {
		base  queues.Executor
		suite *AddTasksSuite
	}
)

func TestAddTasksSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AddTasksSuite))
}

func (c *faultyShardController) GetShardByID(shardID int32) (historyi.ShardContext, error) {
	ctx, err := c.Controller.GetShardByID(shardID)
	if err != nil {
		return nil, err
	}
	return &faultyShardContext{ShardContext: ctx, suite: c.s}, nil
}

func (c *faultyShardContext) GetEngine(ctx context.Context) (historyi.Engine, error) {
	err := c.suite.getEngineErr.Load()
	if err != nil && *err != nil {
		return nil, *err
	}
	return c.ShardContext.GetEngine(ctx)
}

// Wrap a [queues.Executable] with the noopExecutor.
func (w *executorWrapper) Wrap(e queues.Executor) queues.Executor {
	return &noopExecutor{
		base:  e,
		suite: w.s,
	}
}

// Execute will skip any workflow task initiated by this test suite, so that we can add it back to the queue to see if
// that workflow task is retried.
func (e *noopExecutor) Execute(ctx context.Context, executable queues.Executable) queues.ExecuteResponse {
	task := executable.GetTask()
	if e.shouldExecute(task) {
		return e.base.Execute(ctx, executable)
	}
	// If we don't execute the task, just record it.
	e.suite.skippedTasks <- task
	return queues.ExecuteResponse{}
}

// shouldExecute returns true if the task is not a workflow task, or if the workflow task is not from this test suite
// (e.g. from the history scanner), or if we've turned off skipping (which we do when we re-add the task).
func (e *noopExecutor) shouldExecute(task tasks.Task) bool {
	suiteWorkflowID := e.suite.workflowID.Load()
	return (suiteWorkflowID != nil && task.GetWorkflowID() != *suiteWorkflowID) ||
		task.GetType() != enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK ||
		!e.suite.shouldSkip.Load()
}

// SetupSuite creates the test cluster and registers the executorWrapper with the history service.
func (s *AddTasksSuite) SetupSuite() {
	// Set up the test cluster and register our executable wrapper.
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithFxOptionsForService(
		primitives.HistoryService,
		fx.Provide(
			func() queues.ExecutorWrapper {
				return &executorWrapper{s: s}
			},
		),
		fx.Decorate(
			func(c shard.Controller) shard.Controller {
				s.shardController = &faultyShardController{Controller: c, s: s}
				return s.shardController
			},
		),
	),
	)
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
			blob, err := serialization.NewTaskSerializer().SerializeTask(task)
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

var ExampleShardEngineErr = errors.New("example shard engine error")

func (s *AddTasksSuite) TestAddTasks_ErrGetShardByID() {
	_, err := s.GetTestCluster().HistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
		ShardId: 0,
	})
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "invalid shardid")
}

func (s *AddTasksSuite) TestAddTasks_GetEngineErr() {
	defer func() {
		s.getEngineErr.Store(nil)
	}()
	s.getEngineErr.Store(&ExampleShardEngineErr)
	_, err := s.GetTestCluster().HistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
		ShardId: 1,
	})
	s.Error(err)
	s.ErrorContains(err, (*s.getEngineErr.Load()).Error())
}
