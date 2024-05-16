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

package dlq_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.temporal.io/server/service/worker/dlq"
)

type (
	testCase struct {
		name string
		// configure the test to override the default params
		configure func(t *testing.T, params *testParams)
	}
	testParams struct {
		workflowParams     dlq.WorkflowParams
		client             *testHistoryClient
		taskClientDialer   dlq.TaskClientDialer
		currentClusterName string
		// expectation is run with the result of the workflow execution
		expectation       func(err error)
		expectedQueryResp dlq.ProgressQueryResponse
		queryExpectation  func(response dlq.ProgressQueryResponse)
	}
	// This client allows the test to set custom functions for each of its methods.
	testHistoryClient struct {
		getTasksFn    func(req *historyservice.GetDLQTasksRequest) (*historyservice.GetDLQTasksResponse, error)
		deleteTasksFn func(req *historyservice.DeleteDLQTasksRequest) (*historyservice.DeleteDLQTasksResponse, error)
	}
)

// TestModule tests the [dlq.Module] instead of a constructor because we only export the module, and that implicitly
// tests the constructor.
func TestModule(t *testing.T) {
	for _, tc := range []testCase{
		{
			name: "delete",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultDeleteParams(t)
			},
		},
		{
			name: "invalid_workflow_type",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultDeleteParams(t)
				params.workflowParams.WorkflowType = "my-invalid-workflow-type"
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Invalid workflow type should be non-retryable")
					assert.ErrorContains(t, err, "my-invalid-workflow-type")
				}
				params.queryExpectation = func(response dlq.ProgressQueryResponse) {
					assert.NotNil(t, response)
					assert.Equal(t, params.workflowParams.WorkflowType, response.WorkflowType)
				}
			},
		},
		{
			name: "invalid_argument_error_when_deleting",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultDeleteParams(t)
				clientErr := new(serviceerror.InvalidArgument)
				params.client.deleteTasksFn = func(
					req *historyservice.DeleteDLQTasksRequest,
				) (*historyservice.DeleteDLQTasksResponse, error) {
					return nil, clientErr
				}
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError

					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable())
				}
			},
		},
		{
			name: "not_found_error_when_deleting",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultDeleteParams(t)
				clientErr := new(serviceerror.NotFound)
				params.client.deleteTasksFn = func(
					*historyservice.DeleteDLQTasksRequest,
				) (*historyservice.DeleteDLQTasksResponse, error) {
					return nil, clientErr
				}
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError

					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable())
				}
			},
		},
		{
			name: "some_other_error_when_deleting",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultDeleteParams(t)
				clientErr := assert.AnError
				params.client.deleteTasksFn = func(
					*historyservice.DeleteDLQTasksRequest,
				) (*historyservice.DeleteDLQTasksResponse, error) {
					return nil, clientErr
				}
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError

					require.ErrorAs(t, err, &applicationErr)
					assert.False(t, applicationErr.NonRetryable())
				}
			},
		},
		{
			name: "merge",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
			},
		},
		{
			name: "merge_negative_batch_size",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.BatchSize = -1
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Negative batch size should be non-retryable")
					assert.ErrorContains(t, err, "BatchSize")
				}
			},
		},
		{
			name: "merge_batch_size_too_large",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.BatchSize = dlq.MaxMergeBatchSize + 1
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Batch size too large should be non-retryable")
					assert.ErrorContains(t, err, "BatchSize")
				}
			},
		},
		{
			name: "merge_get_tasks_non-retryable_error",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.client.getTasksFn = func(
					*historyservice.GetDLQTasksRequest,
				) (*historyservice.GetDLQTasksResponse, error) {
					return nil, new(serviceerror.InvalidArgument)
				}
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Not found error should be non-retryable")
					assert.ErrorContains(t, err, "GetDLQTasks")
				}
			},
		},
		{
			name: "merge_no_next_page_token",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.MaxMessageID = 2
				params.expectedQueryResp.MaxMessageIDToProcess = 2
				params.expectedQueryResp.NumberOfMessagesProcessed = 1
				params.expectedQueryResp.LastProcessedMessageID = 0
				var (
					getRequests []*historyservice.GetDLQTasksRequest
					addRequests []*adminservice.AddTasksRequest
				)
				params.client.getTasksFn = func(
					req *historyservice.GetDLQTasksRequest,
				) (*historyservice.GetDLQTasksResponse, error) {
					getRequests = append(getRequests, req)
					return &historyservice.GetDLQTasksResponse{
						DlqTasks: []*commonspb.HistoryDLQTask{
							{
								Metadata: &commonspb.HistoryDLQTaskMetadata{
									MessageId: 0,
								},
								Payload: &commonspb.HistoryTask{
									ShardId: 1,
								},
							},
						},
						NextPageToken: nil,
					}, nil
				}
				params.taskClientDialer = dlq.TaskClientDialerFn(func(ctx context.Context, address string) (dlq.TaskClient, error) {
					return dlq.AddTasksFn(func(ctx context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
						addRequests = append(addRequests, req)
						return nil, nil
					}), nil
				})
				params.expectation = func(err error) {
					require.NoError(t, err)
					assert.Len(t, getRequests, 1)
					require.Len(t, addRequests, 1)
					requestsByShardID := make(map[int32]*adminservice.AddTasksRequest)
					for _, request := range addRequests {
						requestsByShardID[request.GetShardId()] = request
					}
					assert.Len(t, requestsByShardID[1].GetTasks(), 1)
				}
				params.expectedQueryResp.LastProcessedMessageID = 0
			},
		},
		{
			name: "merge_multiple_pages",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.MaxMessageID = 3
				params.expectedQueryResp.MaxMessageIDToProcess = 3
				params.expectedQueryResp.LastProcessedMessageID = 3
				params.expectedQueryResp.NumberOfMessagesProcessed = 4
				params.client.getTasksFn = func(
					req *historyservice.GetDLQTasksRequest,
				) (*historyservice.GetDLQTasksResponse, error) {
					return getPaginatedResponse(req)
				}
				var (
					addRequests []*adminservice.AddTasksRequest
				)
				params.taskClientDialer = dlq.TaskClientDialerFn(func(ctx context.Context, address string) (dlq.TaskClient, error) {
					return dlq.AddTasksFn(func(ctx context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
						addRequests = append(addRequests, req)
						return nil, nil
					}), nil
				})
				params.expectation = func(err error) {
					require.NoError(t, err)
					require.Len(t, addRequests, 3)
					requestsByShardID := make(map[int32]*adminservice.AddTasksRequest)
					for _, request := range addRequests {
						requestsByShardID[request.GetShardId()] = request
					}
					assert.Len(t, requestsByShardID[1].GetTasks(), 1)
					assert.Len(t, requestsByShardID[2].GetTasks(), 2)
					assert.Len(t, requestsByShardID[3].GetTasks(), 1)
				}
			},
		},
		{
			name: "merge_add_tasks_non-retryable_error",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.MaxMessageID = 1
				params.expectedQueryResp.MaxMessageIDToProcess = 1
				res := &historyservice.GetDLQTasksResponse{
					DlqTasks: []*commonspb.HistoryDLQTask{
						{
							Metadata: &commonspb.HistoryDLQTaskMetadata{
								MessageId: 0,
							},
							Payload: &commonspb.HistoryTask{
								ShardId: 1,
							},
						},
					},
				}
				params.client.getTasksFn = func(
					*historyservice.GetDLQTasksRequest,
				) (*historyservice.GetDLQTasksResponse, error) {
					return res, nil
				}
				var addTasksRequests []*adminservice.AddTasksRequest
				params.taskClientDialer = dlq.TaskClientDialerFn(func(
					ctx context.Context,
					address string,
				) (dlq.TaskClient, error) {
					return dlq.AddTasksFn(func(
						ctx context.Context,
						req *adminservice.AddTasksRequest,
					) (*adminservice.AddTasksResponse, error) {
						addTasksRequests = append(addTasksRequests, req)
						return nil, new(serviceerror.InvalidArgument)
					}), nil
				})
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Not found error should be non-retryable")
					assert.ErrorContains(t, err, "AddTasks")
					require.Len(t, addTasksRequests, 1)
				}
			},
		},
		{
			name: "merge_delete_tasks_non-retryable_error",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.MaxMessageID = 1
				params.expectedQueryResp.MaxMessageIDToProcess = 1
				res := &historyservice.GetDLQTasksResponse{
					DlqTasks: []*commonspb.HistoryDLQTask{
						{
							Metadata: &commonspb.HistoryDLQTaskMetadata{
								MessageId: 0,
							},
							Payload: &commonspb.HistoryTask{
								ShardId: 1,
							},
						},
					},
				}
				params.client.getTasksFn = func(
					*historyservice.GetDLQTasksRequest,
				) (*historyservice.GetDLQTasksResponse, error) {
					return res, nil
				}
				var (
					addRequests    []*adminservice.AddTasksRequest
					deleteRequests []*historyservice.DeleteDLQTasksRequest
				)
				params.taskClientDialer = dlq.TaskClientDialerFn(func(
					ctx context.Context,
					address string,
				) (dlq.TaskClient, error) {
					return dlq.AddTasksFn(func(
						ctx context.Context,
						req *adminservice.AddTasksRequest,
					) (*adminservice.AddTasksResponse, error) {
						addRequests = append(addRequests, req)
						return nil, nil
					}), nil
				})
				params.client.deleteTasksFn = func(
					req *historyservice.DeleteDLQTasksRequest,
				) (*historyservice.DeleteDLQTasksResponse, error) {
					deleteRequests = append(deleteRequests, req)
					return nil, new(serviceerror.InvalidArgument)
				}
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Not found error should be non-retryable")
					assert.ErrorContains(t, err, "DeleteDLQTasks")
					require.Len(t, addRequests, 1)
					require.Len(t, deleteRequests, 1)
				}
			},
		},
		{
			name: "merge_replication_tasks_dial_error",
			configure: func(t *testing.T, params *testParams) {
				params.setDefaultMergeParams(t)
				params.workflowParams.MergeParams.Key.SourceCluster = "source-cluster"
				params.workflowParams.MergeParams.Key.TargetCluster = "current-cluster"
				params.currentClusterName = "current-cluster"
				params.workflowParams.MergeParams.Key.TaskCategoryID = tasks.CategoryIDReplication
				params.expectedQueryResp.DlqKey = params.workflowParams.MergeParams.Key
				var replicationTask tasks.HistoryReplicationTask
				blob, err := serialization.NewTaskSerializer().SerializeTask(&replicationTask)
				require.NoError(t, err)
				params.client.getTasksFn = func(req *historyservice.GetDLQTasksRequest) (*historyservice.GetDLQTasksResponse, error) {
					return &historyservice.GetDLQTasksResponse{
						DlqTasks: []*commonspb.HistoryDLQTask{
							{
								Metadata: &commonspb.HistoryDLQTaskMetadata{
									MessageId: 0,
								},
								Payload: &commonspb.HistoryTask{
									ShardId: 1,
									Blob:    blob,
								},
							},
						},
					}, nil
				}
				params.taskClientDialer = dlq.TaskClientDialerFn(func(ctx context.Context, address string) (dlq.TaskClient, error) {
					return nil, assert.AnError
				})
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.False(t, applicationErr.NonRetryable())
					msg := strings.ToLower(err.Error())
					assert.Contains(t, msg, "unable to dial admin service for cluster")
					assert.Contains(t, msg, "source-cluster")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			params := &testParams{}
			tc.configure(t, params)

			var components []workercommon.WorkerComponent

			fxtest.New(
				t,
				dlq.Module,
				fx.Provide(
					func() dlq.HistoryClient {
						return params.client
					},
					func() dlq.TaskClientDialer {
						return params.taskClientDialer
					},
					func() dlq.CurrentClusterName {
						return dlq.CurrentClusterName(params.currentClusterName)
					},
				),
				fx.Populate(fx.Annotate(&components, fx.ParamTags(workercommon.WorkerComponentTag))),
			)
			require.Len(t, components, 1)
			component := components[0]
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			component.RegisterWorkflow(env)
			component.RegisterActivities(env)
			require.Nil(t, component.DedicatedWorkflowWorkerOptions())
			assert.Equal(t, primitives.DLQActivityTQ, component.DedicatedActivityWorkerOptions().TaskQueue)

			env.ExecuteWorkflow(dlq.WorkflowName, params.workflowParams)
			err := env.GetWorkflowError()
			params.expectation(err)
			resp, err := env.QueryWorkflow(dlq.QueryTypeProgress)
			require.NoError(t, err)
			queryResp := dlq.ProgressQueryResponse{}
			err = resp.Get(&queryResp)
			require.NoError(t, err)
			params.queryExpectation(queryResp)
		})
	}
}

func getPaginatedResponse(req *historyservice.GetDLQTasksRequest) (*historyservice.GetDLQTasksResponse, error) {
	if len(req.NextPageToken) == 0 {
		return &historyservice.GetDLQTasksResponse{
			DlqTasks: []*commonspb.HistoryDLQTask{
				{
					Metadata: &commonspb.HistoryDLQTaskMetadata{
						MessageId: 0,
					},
					Payload: &commonspb.HistoryTask{
						ShardId: 1,
					},
				},
				{
					Metadata: &commonspb.HistoryDLQTaskMetadata{
						MessageId: 1,
					},
					Payload: &commonspb.HistoryTask{
						ShardId: 2,
					},
				},
				{
					Metadata: &commonspb.HistoryDLQTaskMetadata{
						MessageId: 2,
					},
					Payload: &commonspb.HistoryTask{
						ShardId: 2,
					},
				},
			},
			NextPageToken: []byte{42},
		}, nil
	}

	return &historyservice.GetDLQTasksResponse{
		DlqTasks: []*commonspb.HistoryDLQTask{
			{
				Metadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: 3,
				},
				Payload: &commonspb.HistoryTask{
					ShardId: 3,
				},
			},
			{
				Metadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: 4,
				},
				Payload: &commonspb.HistoryTask{
					ShardId: 4,
				},
			},
		},
		NextPageToken: []byte{42},
	}, nil
}

func (p *testParams) setDefaultDeleteParams(t *testing.T) {
	p.setDefaultParams(t)
	p.workflowParams = dlq.WorkflowParams{
		WorkflowType: dlq.WorkflowTypeDelete,
		DeleteParams: dlq.DeleteParams{
			Key: dlq.Key{
				TaskCategoryID: tasks.CategoryTransfer.ID(),
				SourceCluster:  "current-cluster",
				TargetCluster:  "current-cluster",
			},
		},
	}
	p.expectedQueryResp = dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:     p.workflowParams.DeleteParams.MaxMessageID,
		LastProcessedMessageID:    p.workflowParams.DeleteParams.MaxMessageID,
		NumberOfMessagesProcessed: 0,
		WorkflowType:              p.workflowParams.WorkflowType,
		DlqKey:                    p.workflowParams.DeleteParams.Key,
	}
}

func (p *testParams) setDefaultMergeParams(t *testing.T) {
	p.setDefaultParams(t)
	p.workflowParams = dlq.WorkflowParams{
		WorkflowType: dlq.WorkflowTypeMerge,
		MergeParams: dlq.MergeParams{
			Key: dlq.Key{
				TaskCategoryID: tasks.CategoryTransfer.ID(),
				SourceCluster:  "current-cluster",
				TargetCluster:  "current-cluster",
			},
		},
	}
	p.expectedQueryResp = dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:  p.workflowParams.MergeParams.MaxMessageID,
		LastProcessedMessageID: p.workflowParams.MergeParams.MaxMessageID,
		WorkflowType:           p.workflowParams.WorkflowType,
		DlqKey:                 p.workflowParams.MergeParams.Key,
	}
	p.currentClusterName = "current-cluster"
}

func (p *testParams) setDefaultParams(t *testing.T) {
	p.client = &testHistoryClient{}
	p.client.getTasksFn = func(
		*historyservice.GetDLQTasksRequest,
	) (*historyservice.GetDLQTasksResponse, error) {
		return nil, nil
	}
	p.client.deleteTasksFn = func(
		request *historyservice.DeleteDLQTasksRequest,
	) (*historyservice.DeleteDLQTasksResponse, error) {
		return nil, nil
	}
	p.expectation = func(err error) {
		require.NoError(t, err)
	}
	p.queryExpectation = func(response dlq.ProgressQueryResponse) {
		require.NotNil(t, response)
		require.Equal(t, p.expectedQueryResp.MaxMessageIDToProcess, response.MaxMessageIDToProcess)
		require.Equal(t, p.expectedQueryResp.LastProcessedMessageID, response.LastProcessedMessageID)
		require.Equal(t, p.expectedQueryResp.WorkflowType, response.WorkflowType)
		require.Equal(t, p.expectedQueryResp.NumberOfMessagesProcessed, response.NumberOfMessagesProcessed)
		require.EqualValues(t, p.expectedQueryResp.DlqKey, response.DlqKey)
	}
	p.taskClientDialer = dlq.TaskClientDialerFn(func(ctx context.Context, address string) (dlq.TaskClient, error) {
		return dlq.AddTasksFn(func(ctx context.Context, req *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
			return nil, nil
		}), nil
	})
}

func (c *testHistoryClient) GetDLQTasks(
	_ context.Context, req *historyservice.GetDLQTasksRequest, _ ...grpc.CallOption,
) (*historyservice.GetDLQTasksResponse, error) {
	return c.getTasksFn(req)
}

func (c *testHistoryClient) DeleteDLQTasks(
	_ context.Context, req *historyservice.DeleteDLQTasksRequest, _ ...grpc.CallOption,
) (*historyservice.DeleteDLQTasksResponse, error) {
	return c.deleteTasksFn(req)
}
