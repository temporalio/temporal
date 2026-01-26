package tests

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
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
	// addTasksState holds the state needed for the add tasks tests
	addTasksState struct {
		shardController *faultyShardController
		skippedTasks    chan tasks.Task

		shouldSkip   atomic.Bool
		getEngineErr atomic.Pointer[error]
		workflowID   atomic.Pointer[string]
	}
	faultyShardController struct {
		shard.Controller
		state *addTasksState
	}
	faultyShardContext struct {
		historyi.ShardContext
		state *addTasksState
	}
	// executorWrapper is used to wrap any [queues.Executable] that the history service makes so that we can intercept
	// workflow tasks.
	executorWrapper struct {
		state *addTasksState
	}
	// noopExecutor skips any workflow task which meets the criteria specified in shouldExecute and records them to
	// the tasks channel.
	noopExecutor struct {
		base  queues.Executor
		state *addTasksState
	}
)

func (c *faultyShardController) GetShardByID(shardID int32) (historyi.ShardContext, error) {
	ctx, err := c.Controller.GetShardByID(shardID)
	if err != nil {
		return nil, err
	}
	return &faultyShardContext{ShardContext: ctx, state: c.state}, nil
}

func (c *faultyShardContext) GetEngine(ctx context.Context) (historyi.Engine, error) {
	err := c.state.getEngineErr.Load()
	if err != nil && *err != nil {
		return nil, *err
	}
	return c.ShardContext.GetEngine(ctx)
}

// Wrap a [queues.Executable] with the noopExecutor.
func (w *executorWrapper) Wrap(e queues.Executor) queues.Executor {
	return &noopExecutor{
		base:  e,
		state: w.state,
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
	e.state.skippedTasks <- task
	return queues.ExecuteResponse{}
}

// shouldExecute returns true if the task is not a workflow task, or if the workflow task is not from this test suite
// (e.g. from the history scanner), or if we've turned off skipping (which we do when we re-add the task).
func (e *noopExecutor) shouldExecute(task tasks.Task) bool {
	suiteWorkflowID := e.state.workflowID.Load()
	return (suiteWorkflowID != nil && task.GetWorkflowID() != *suiteWorkflowID) ||
		task.GetType() != enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK ||
		!e.state.shouldSkip.Load()
}

var ExampleShardEngineErr = errors.New("example shard engine error")

func TestAddTasks(t *testing.T) {
	// Create shared state for the test
	state := &addTasksState{}

	// Set up the test cluster with dedicated cluster and register our executable wrapper and shard controller
	s := testcore.NewEnv(t,
		testcore.WithDedicatedCluster(),
		testcore.WithClusterOptions(
			testcore.WithFxOptionsForService(
				primitives.HistoryService,
				fx.Provide(
					func() queues.ExecutorWrapper {
						return &executorWrapper{state: state}
					},
				),
				fx.Decorate(
					func(c shard.Controller) shard.Controller {
						state.shardController = &faultyShardController{Controller: c, state: state}
						return state.shardController
					},
				),
			),
		),
	)

	// Set up SDK client and worker
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
		Logger:    log.NewSdkLogger(s.Logger),
	})
	require.NoError(t, err)
	defer sdkClient.Close()

	t.Run("Ok", func(t *testing.T) {
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
			t.Run(tc.name, func(t *testing.T) {
				// Register a workflow which does nothing.
				taskQueue := s.Tv().TaskQueue().Name
				w := worker.New(sdkClient, taskQueue, worker.Options{DeadlockDetectionTimeout: 0})
				myWorkflow := func(ctx workflow.Context) error {
					return nil
				}
				require.NoError(t, w.Start())
				defer w.Stop()
				w.RegisterWorkflow(myWorkflow)

				// Execute that workflow
				// We need to track the workflow ID so that we can filter out tasks from this test suite
				workflowID := uuid.NewString()
				state.workflowID.Store(&workflowID)
				state.shouldSkip.Store(true)
				state.skippedTasks = make(chan tasks.Task)
				ctx := context.Background()
				timeout := 5 * debug.TimeoutMultiplier * time.Second
				ctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        workflowID,
					TaskQueue: taskQueue,
				}, myWorkflow)
				require.NoError(t, err)

				// Get the task that we skipped, and add it back
				var task tasks.Task
				select {
				case task = <-state.skippedTasks:
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for skipped task")
				}

				state.shouldSkip.Store(false)
				blob, err := serialization.NewSerializer().SerializeTask(task)
				require.NoError(t, err)
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
					require.NoError(t, err)
				}

				// Wait for the workflow to complete
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = run.Get(ctx, nil)
				if tc.shouldCallAddTasks {
					require.NoError(t, err, "workflow task should be retried if we call AddTasks")
				} else {
					require.Error(t, err, "workflow should not complete if we don't call AddTasks")
				}
			})
		}
	})

	t.Run("ErrGetShardByID", func(t *testing.T) {
		_, err := s.GetTestCluster().HistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
			ShardId: 0,
		})
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "invalid shardid")
	})

	t.Run("GetEngineErr", func(t *testing.T) {
		defer func() {
			state.getEngineErr.Store(nil)
		}()
		state.getEngineErr.Store(&ExampleShardEngineErr)
		_, err := s.GetTestCluster().HistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
			ShardId: 1,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, (*state.getEngineErr.Load()).Error())
	})
}
