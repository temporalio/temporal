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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/historyservice/v1"
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
		workflowParams dlq.WorkflowParams
		client         *faultyHistoryClient
		// expectation is run with the result of the workflow execution
		expectation func(err error)
	}
	faultyHistoryClient struct {
		err error
	}
)

// TestModule tests the [dlq.Module] instead of a constructor because we only export the module, and that implicitly
// tests the constructor.
func TestModule(t *testing.T) {
	for _, tc := range []testCase{
		{
			name: "happy path",
			configure: func(t *testing.T, params *testParams) {
			},
		},
		{
			name: "invalid workflow type",
			configure: func(t *testing.T, params *testParams) {
				params.workflowParams.WorkflowType = "my-invalid-workflow-type"
				params.expectation = func(err error) {
					var applicationErr *temporal.ApplicationError
					require.ErrorAs(t, err, &applicationErr)
					assert.True(t, applicationErr.NonRetryable(),
						"Invalid workflow type should be non-retryable")
					assert.ErrorContains(t, err, "my-invalid-workflow-type")
				}
			},
		},
		{
			name: "invalid argument error",
			configure: func(t *testing.T, params *testParams) {
				params.client.err = new(serviceerror.InvalidArgument)
				verifyRetry(t, params, false)
			},
		},
		{
			name: "not found error",
			configure: func(t *testing.T, params *testParams) {
				params.client.err = new(serviceerror.NotFound)
				verifyRetry(t, params, false)
			},
		},
		{
			name: "some other error",
			configure: func(t *testing.T, params *testParams) {
				params.client.err = assert.AnError
				verifyRetry(t, params, true)
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			params := getDefaultTestParams(t)
			tc.configure(t, params)

			var components []workercommon.WorkerComponent

			fxtest.New(
				t,
				dlq.Module,
				fx.Provide(func() dlq.HistoryServiceClient {
					return params.client
				}),
				fx.Populate(fx.Annotate(&components, fx.ParamTags(workercommon.WorkerComponentTag))),
			)
			require.Len(t, components, 1)
			component := components[0]
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestWorkflowEnvironment()
			component.RegisterWorkflow(env)
			component.RegisterActivities(env)

			env.ExecuteWorkflow(dlq.WorkflowName, params.workflowParams)
			err := env.GetWorkflowError()
			params.expectation(err)
		})
	}
}

func verifyRetry(t *testing.T, params *testParams, shouldRetry bool) {
	params.expectation = func(err error) {
		var applicationErr *temporal.ApplicationError

		require.ErrorAs(t, err, &applicationErr)
		assert.Equal(t, applicationErr.NonRetryable(), !shouldRetry,
			fmt.Sprintf(
				"%v error should be considered retryable: %v",
				params.client.err, shouldRetry,
			),
		)
	}
}

func getDefaultTestParams(t *testing.T) *testParams {
	return &testParams{
		workflowParams: dlq.WorkflowParams{
			WorkflowType: dlq.WorkflowTypeDelete,
			DeleteParams: dlq.DeleteParams{
				TaskCategory:  tasks.CategoryTransfer.ID(),
				SourceCluster: "source-cluster",
				TargetCluster: "target-cluster",
			},
		},
		client: &faultyHistoryClient{},
		expectation: func(err error) {
			require.NoError(t, err)
		},
	}
}

func (t *faultyHistoryClient) DeleteDLQTasks(
	context.Context,
	*historyservice.DeleteDLQTasksRequest,
	...grpc.CallOption,
) (*historyservice.DeleteDLQTasksResponse, error) {
	return nil, t.err
}
