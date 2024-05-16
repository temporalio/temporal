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

package addtasks_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/api/addtasks"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	// testParams contains the arguments for invoking addtasks.Invoke.
	testParams struct {
		shardContext shard.Context
		deserializer addtasks.TaskDeserializer
		numShards    int
		req          *historyservice.AddTasksRequest
		// expectation is invoked with the result of addtasks.Invoke.
		expectation func(*historyservice.AddTasksResponse, error)
	}
	testCase struct {
		name string
		// configure is invoked with the result of getDefaultTestParams. The default params are set up to invoke the
		// API successfully, so configure can be used to change the params to test different failure modes.
		configure func(t *testing.T, params *testParams)
	}
	// faultyDeserializer is a TaskDeserializer that always returns an error.
	faultyDeserializer struct {
		err error
	}
)

func (d faultyDeserializer) DeserializeTask(
	tasks.Category,
	*commonpb.DataBlob,
) (tasks.Task, error) {
	return nil, d.err
}

func TestInvoke(t *testing.T) {
	for _, tc := range []testCase{
		{
			name: "happy path",
			configure: func(t *testing.T, params *testParams) {
				params.shardContext.(*shard.MockContext).EXPECT().AddTasks(
					gomock.Any(),
					gomock.Any(),
				).Return(nil)
				params.expectation = func(
					resp *historyservice.AddTasksResponse,
					err error,
				) {
					require.NoError(t, err)
					assert.NotNil(t, resp)
				}
			},
		},
		{
			name: "tasks for multiple workflows",
			configure: func(t *testing.T, params *testParams) {
				numWorkflows := 2
				requests := make([]*persistence.AddHistoryTasksRequest, 0, numWorkflows)
				params.shardContext.(*shard.MockContext).EXPECT().AddTasks(
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(_ context.Context, req *persistence.AddHistoryTasksRequest) error {
					requests = append(requests, req)
					return nil
				}).Times(2)
				params.req.Tasks = nil
				for i := 0; i < numWorkflows; i++ {
					workflowKey := definition.NewWorkflowKey(
						string(tests.NamespaceID),
						strconv.Itoa(i),
						uuid.New(),
					)
					// each workflow has two transfer tasks and one timer task
					for _, task := range []tasks.Task{
						&tasks.WorkflowTask{
							WorkflowKey: workflowKey,
						},
						&tasks.WorkflowTask{
							WorkflowKey: workflowKey,
						},
						&tasks.UserTimerTask{
							WorkflowKey: workflowKey,
						},
					} {
						serializer := serialization.NewTaskSerializer()
						blob, err := serializer.SerializeTask(task)
						require.NoError(t, err)
						params.req.Tasks = append(params.req.Tasks, &historyservice.AddTasksRequest_Task{
							CategoryId: int32(task.GetCategory().ID()),
							Blob:       blob,
						})
					}
				}
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.NoError(t, err)
					assert.NotNil(t, resp)
					require.Len(t, requests, numWorkflows, "We should send one request for each workflow")
					workflowIDs := make([]string, numWorkflows)
					for i, request := range requests {
						workflowIDs[i] = request.WorkflowID
						assert.Len(t, request.Tasks[tasks.CategoryTransfer], 2,
							"There were two transfer tasks for each workflow")
						assert.Len(t, request.Tasks[tasks.CategoryTimer], 1,
							"There was one timer task for each workflow")
					}
					// The requests could go in any order because we do map iteration, so compare the elements while
					// ignoring their order.
					assert.ElementsMatch(t, []string{"0", "1"}, workflowIDs,
						"The requests should be for the expected workflowIDs")
				}
			},
		},
		{
			name: "too many tasks",
			configure: func(t *testing.T, params *testParams) {
				params.req.Tasks = make([]*historyservice.AddTasksRequest_Task, 1001)
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "Too many tasks")
					assert.ErrorContains(t, err, "1001")
				}
			},
		},
		{
			name: "no tasks",
			configure: func(t *testing.T, params *testParams) {
				params.req.Tasks = make([]*historyservice.AddTasksRequest_Task, 0)
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "No tasks")
				}
			},
		},
		{
			name: "nil task",
			configure: func(t *testing.T, params *testParams) {
				params.req.Tasks[0] = nil
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "Nil task")
				}
			},
		},
		{
			name: "invalid task category",
			configure: func(t *testing.T, params *testParams) {
				params.req.Tasks[0].CategoryId = -1
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "Invalid task category")
					assert.ErrorContains(t, err, "-1")
				}
			},
		},
		{
			name: "nil task blob",
			configure: func(t *testing.T, params *testParams) {
				params.req.Tasks[0].Blob = nil
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "Task blob is nil")
				}
			},
		},
		{
			name: "deserializer error",
			configure: func(t *testing.T, params *testParams) {
				params.deserializer = faultyDeserializer{
					err: assert.AnError,
				}
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					assert.ErrorIs(t, err, assert.AnError)
				}
			},
		},
		{
			name: "wrong shard",
			configure: func(t *testing.T, params *testParams) {
				params.req.ShardId = 1
				params.numShards = 2
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					assert.ErrorContains(t, err, "Task is for wrong shard")
					assert.ErrorContains(t, err, "task shard = 2")
					assert.ErrorContains(t, err, "request shard = 1")
				}
			},
		},
		{
			name: "add tasks error",
			configure: func(t *testing.T, params *testParams) {
				params.shardContext.(*shard.MockContext).EXPECT().AddTasks(
					gomock.Any(),
					gomock.Any(),
				).Return(assert.AnError)
				params.expectation = func(resp *historyservice.AddTasksResponse, err error) {
					assert.ErrorIs(t, err, assert.AnError)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := getDefaultTestParams(t)
			tc.configure(t, params)
			resp, err := addtasks.Invoke(
				context.Background(),
				params.shardContext,
				params.deserializer,
				params.numShards,
				params.req,
				tasks.NewDefaultTaskCategoryRegistry(),
			)
			params.expectation(resp, err)
		})
	}
}

func getDefaultTestParams(t *testing.T) *testParams {
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(string(tests.NamespaceID), tests.WorkflowID, tests.RunID),
	}
	serializer := serialization.NewTaskSerializer()
	blob, err := serializer.SerializeTask(task)
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	shardContext := shard.NewMockContext(ctrl)
	shardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	shardContext.EXPECT().GetRangeID().Return(int64(1)).AnyTimes()
	params := &testParams{
		shardContext: shardContext,
		numShards:    1,
		deserializer: serializer,
		req: &historyservice.AddTasksRequest{
			ShardId: 1,
			Tasks: []*historyservice.AddTasksRequest_Task{
				{
					CategoryId: int32(tasks.CategoryTransfer.ID()),
					Blob:       blob,
				},
			},
		},
		expectation: func(response *historyservice.AddTasksResponse, err error) {},
	}

	return params
}
