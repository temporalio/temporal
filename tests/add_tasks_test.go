// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	enumspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

// This file tests the HistoryService's AddTasks API. It does this by starting a workflow, skipping its workflow task,
// and then adding the workflow task back to the queue via the AddTasks API. This should cause the workflow to be
// retried.

type (
	// addTasksSuite is a separate suite because we need to override the history service's executable wrapper.
	addTasksSuite struct {
		FunctionalTestBase
		*require.Assertions
		shardController *faultyShardController
		worker          worker.Worker
		sdkClient       sdkclient.Client
		skippedTasks    chan tasks.Task
		shouldSkip      bool
		getEngineErr    error
		workflowID      string
	}
	faultyShardController struct {
		shard.Controller
		s *addTasksSuite
	}
	faultyShardContext struct {
		shard.Context
		s *addTasksSuite
	}
	// executableWrapper is used to wrap any [queues.Executable] that the history service makes so that we can intercept
	// workflow tasks.
	executableWrapper struct {
		w queues.ExecutableWrapper
		s *addTasksSuite
	}
	// noopExecutable skips any workflow task which meets the criteria specified in shouldExecute and records them to
	// the tasks channel.
	noopExecutable struct {
		queues.Executable
		s *addTasksSuite
	}
)

func (c *faultyShardController) GetShardByID(shardID int32) (shard.Context, error) {
	ctx, err := c.Controller.GetShardByID(shardID)
	if err != nil {
		return nil, err
	}
	return &faultyShardContext{Context: ctx, s: c.s}, nil
}

func (c *faultyShardContext) GetEngine(ctx context.Context) (shard.Engine, error) {
	if c.s.getEngineErr != nil {
		return nil, c.s.getEngineErr
	}
	return c.Context.GetEngine(ctx)
}

// Wrap a [queues.Executable] with the noopExecutable.
func (w *executableWrapper) Wrap(e queues.Executable) queues.Executable {
	e = w.w.Wrap(e)
	return &noopExecutable{Executable: e, s: w.s}
}

// Execute will skip any workflow task initiated by this test suite, so that we can add it back to the queue to see if
// that workflow task is retried.
func (e *noopExecutable) Execute() error {
	task := e.GetTask()
	if e.shouldExecute(task) {
		return e.Executable.Execute()
	}
	// If we don't execute the task, just record it.
	e.s.skippedTasks <- task
	return nil
}

// shouldExecute returns true if the task is not a workflow task, or if the workflow task is not from this test suite
// (e.g. from the history scanner), or if we've turned off skipping (which we do when we re-add the task).
func (e *noopExecutable) shouldExecute(task tasks.Task) bool {
	return task.GetWorkflowID() != e.s.workflowID ||
		task.GetType() != enumspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK ||
		!e.s.shouldSkip
}

// SetupSuite creates the test cluster and registers the executableWrapper with the history service.
func (s *addTasksSuite) SetupSuite() {
	// We do this here and in SetupTest because we need assertions in the SetupSuite method as well as the individual
	// tests, but this is called before SetupTest, and the s.T() value will change when SetupTest is called.
	s.Assertions = require.New(s.T())
	// Set up the test cluster and register our executable wrapper.
	s.setupSuite("testdata/cluster.yaml",
		WithFxOptionsForService(
			primitives.HistoryService,
			fx.Decorate(
				func(w queues.ExecutableWrapper) queues.ExecutableWrapper {
					return &executableWrapper{w: w, s: s}
				},
				func(c shard.Controller) shard.Controller {
					s.shardController = &faultyShardController{Controller: c, s: s}
					return s.shardController
				},
			),
		),
	)
	// Get an SDK client so that we can call ExecuteWorkflow.
	s.sdkClient = s.newSDKClient()
}

func (s *addTasksSuite) TearDownSuite() {
	s.sdkClient.Close()
	s.tearDownSuite()
}

func (s *addTasksSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func TestAddTasksSuite(t *testing.T) {
	suite.Run(t, new(addTasksSuite))
}

func (s *addTasksSuite) TestAddTasks_Ok() {
	for _, tc := range []struct {
		name               string
		shouldCallAddTasks bool
	}{
		{
			name:               "call AddTasks",
			shouldCallAddTasks: true,
		},
		{
			name:               "don't call AddTasks",
			shouldCallAddTasks: false,
		},
	} {
		s.Run(tc.name, func() {
			// Register a workflow which does nothing.
			taskQueue := s.randomizeStr("add-tasks-test-queue")
			w := worker.New(s.sdkClient, taskQueue, worker.Options{DeadlockDetectionTimeout: 0})
			myWorkflow := func(ctx workflow.Context) error {
				return nil
			}
			s.NoError(w.Start())
			defer w.Stop()
			w.RegisterWorkflow(myWorkflow)

			// Execute that workflow
			// We need to track the workflow ID so that we can filter out tasks from this test suite
			s.workflowID = uuid.New()
			s.shouldSkip = true
			s.skippedTasks = make(chan tasks.Task)
			ctx := context.Background()
			timeout := 5 * debug.TimeoutMultiplier * time.Second
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			workflowID := s.workflowID
			run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: taskQueue,
			}, myWorkflow)
			s.NoError(err)

			// Get the task that we skipped, and add it back
			var task tasks.Task
			select {
			case task = <-s.skippedTasks:
			case <-ctx.Done():
				s.FailNow("timed out waiting for skipped task")
			}

			s.shouldSkip = false
			blob, err := serialization.NewTaskSerializer().SerializeTask(task)
			s.NoError(err)
			shardID := tasks.GetShardIDForTask(task, int(s.testClusterConfig.HistoryConfig.NumHistoryShards))
			request := &historyservice.AddTasksRequest{
				ShardId: int32(shardID),
				Tasks: []*historyservice.AddTasksRequest_Task{
					{
						Category: task.GetCategory().Name(),
						Blob:     &blob,
					},
				},
			}
			if tc.shouldCallAddTasks {
				_, err = s.testCluster.GetHistoryClient().AddTasks(ctx, request)
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

func (s *addTasksSuite) TestAddTasks_ErrGetShardByID() {
	_, err := s.testCluster.GetHistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
		ShardId: 0,
	})
	s.Error(err)
	s.Contains(strings.ToLower(err.Error()), "shard id")
}

func (s *addTasksSuite) TestAddTasks_GetEngineErr() {
	defer func() {
		s.getEngineErr = nil
	}()
	s.getEngineErr = errors.New("example shard engine error")
	_, err := s.testCluster.GetHistoryClient().AddTasks(context.Background(), &historyservice.AddTasksRequest{
		ShardId: 1,
	})
	s.Error(err)
	s.ErrorContains(err, s.getEngineErr.Error())
}

func (s *addTasksSuite) newSDKClient() sdkclient.Client {
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	s.NoError(err)
	return client
}
