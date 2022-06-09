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

package deleteexecutions

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func Test_DeleteExecutionsWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.OnActivity(a.GetNextPageTokenActivity, mock.Anything, GetNextPageTokenParams{
		Namespace:     "namespace",
		NamespaceID:   "namespace-id",
		PageSize:      1000,
		NextPageToken: nil,
	}).Return(nil, nil).Once()

	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, DeleteExecutionsActivityParams{
		Namespace:     "namespace",
		NamespaceID:   "namespace-id",
		RPS:           100,
		ListPageSize:  1000,
		NextPageToken: nil,
	}).Return(DeleteExecutionsActivityResult{
		ErrorCount:   1,
		SuccessCount: 2,
	}, nil).Once()

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 1, result.ErrorCount)
	require.Equal(t, 2, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_NoActivityMocks_NoExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      1000,
		NextPageToken: nil,
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions:    nil,
		NextPageToken: nil,
	}, nil).Times(2)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     nil,
		metricsClient:     nil,
		logger:            nil,
	}

	env.RegisterActivity(a.GetNextPageTokenActivity)
	env.RegisterActivity(a.DeleteExecutionsActivity)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	})

	require.True(t, env.IsWorkflowCompleted())
	ctrl.Finish()
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.ErrorCount)
	require.Equal(t, 0, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_ManyExecutions_NoContinueAsNew(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	pageNumber := 0
	env.OnActivity(a.GetNextPageTokenActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params GetNextPageTokenParams) ([]byte, error) {
		require.Equal(t, namespace.Name("namespace"), params.Namespace)
		require.Equal(t, namespace.ID("namespace-id"), params.NamespaceID)
		require.Equal(t, 3, params.PageSize)
		if pageNumber == 0 {
			require.Nil(t, params.NextPageToken)
		} else {
			require.Equal(t, []byte{3, 22, 83}, params.NextPageToken)
		}
		pageNumber++
		if pageNumber == 100 { // Emulate 100 pages of executions.
			return nil, nil
		}
		return []byte{3, 22, 83}, nil
	}).Times(100)

	nilTokenOnce := false
	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
		require.Equal(t, namespace.Name("namespace"), params.Namespace)
		require.Equal(t, namespace.ID("namespace-id"), params.NamespaceID)
		require.Equal(t, 100, params.RPS)
		require.Equal(t, 3, params.ListPageSize)
		if params.NextPageToken == nil {
			nilTokenOnce = true
		} else if nilTokenOnce {
			require.Equal(t, []byte{3, 22, 83}, params.NextPageToken)
		}

		return DeleteExecutionsActivityResult{
			ErrorCount:   1,
			SuccessCount: 2,
		}, nil
	}).Times(100)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize: 3,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 100, result.ErrorCount)
	require.Equal(t, 200, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_ManyExecutions_ContinueAsNew(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.OnActivity(a.GetNextPageTokenActivity, mock.Anything, mock.Anything).Return([]byte{3, 22, 83}, nil).Times(78)
	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).Return(DeleteExecutionsActivityResult{}, nil).Times(78)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize:               3,
			PagesPerExecutionCount: 78,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	wfErr := env.GetWorkflowError()
	require.Error(t, wfErr)
	var errContinueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, wfErr, &errContinueAsNew)
}

func Test_DeleteExecutionsWorkflow_ManyExecutions_ActivityError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.OnActivity(a.GetNextPageTokenActivity, mock.Anything, mock.Anything).
		Return([]byte{3, 22, 83}, nil).
		Times(40) // GoSDK defaultMaximumAttemptsForUnitTest value * defaultConcurrentDeleteExecutionsActivities.
	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).
		Return(DeleteExecutionsActivityResult{}, serviceerror.NewUnavailable("specific_error_from_activity")).
		Times(40) // GoSDK defaultMaximumAttemptsForUnitTest value * defaultConcurrentDeleteExecutionsActivities.

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize: 3,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, stderrors.As(err, &appErr))
	require.Contains(t, appErr.Error(), "unable to execute activity: DeleteExecutionsActivity")
	require.Contains(t, appErr.Error(), "specific_error_from_activity")
}

func Test_DeleteExecutionsWorkflow_NoActivityMocks_ManyExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: nil,
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-2",
					RunId:      "run-id-2",
				},
			},
		},
		NextPageToken: []byte{22, 8, 78},
	}, nil).Times(2)

	// Second page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: []byte{22, 8, 78},
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-2",
					RunId:      "run-id-2",
				},
			},
		},
		NextPageToken: nil,
	}, nil).Times(2)

	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil).Times(2)
	// NotFound errors should not affect the error count.
	historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found")).Times(2)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.GetNextPageTokenActivity)
	env.RegisterActivity(a.DeleteExecutionsActivity)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize: 2,
		},
	})

	ctrl.Finish()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.ErrorCount)
	require.Equal(t, 2, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_NoActivityMocks_HistoryClientError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: nil,
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-2",
					RunId:      "run-id-2",
				},
			},
		},
		NextPageToken: []byte{22, 8, 78},
	}, nil).Times(2)

	// Second page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: []byte{22, 8, 78},
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-2",
					RunId:      "run-id-2",
				},
			},
		},
		NextPageToken: nil,
	}, nil).Times(2)

	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("random")).Times(4)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.GetNextPageTokenActivity)
	env.RegisterActivity(a.DeleteExecutionsActivity)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize: 2,
		},
	})

	ctrl.Finish()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 4, result.ErrorCount)
	require.Equal(t, 0, result.SuccessCount)
}
