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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"

	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func Test_DeleteExecutionsWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.OnActivity(a.GetNextPageTokenActivity, mock.Anything, GetNextPageTokenParams{
		Namespace:     "namespace",
		NamespaceID:   "namespace-id",
		PageSize:      1000,
		NextPageToken: nil,
	}).Return(nil, nil)

	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, DeleteExecutionsActivityParams{
		Namespace:     "namespace",
		NamespaceID:   "namespace-id",
		DeleteRPS:     100,
		ListPageSize:  1000,
		NextPageToken: nil,
	}).Return(DeleteExecutionsActivityResult{
		ErrorCount:   1,
		SuccessCount: 2,
	}, nil)

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

func Test_DeleteExecutionsWorkflow_NoExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().ListWorkflowExecutions(&manager.ListWorkflowExecutionsRequestV2{
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

func Test_DeleteExecutionsWorkflow_ManyExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(&manager.ListWorkflowExecutionsRequestV2{
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
	visibilityManager.EXPECT().ListWorkflowExecutions(&manager.ListWorkflowExecutionsRequestV2{
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
	historyClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, nil)
	historyClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.GetNextPageTokenActivity)
	env.RegisterActivity(a.DeleteExecutionsActivity)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID:  "namespace-id",
		Namespace:    "namespace",
		ListPageSize: 2,
	})

	ctrl.Finish()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.ErrorCount)
	require.Equal(t, 4, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_ActivityError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(&manager.ListWorkflowExecutionsRequestV2{
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
	visibilityManager.EXPECT().ListWorkflowExecutions(&manager.ListWorkflowExecutionsRequestV2{
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
	historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("random")).Times(2)
	historyClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("random")).Times(2)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.GetNextPageTokenActivity)
	env.RegisterActivity(a.DeleteExecutionsActivity)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID:  "namespace-id",
		Namespace:    "namespace",
		ListPageSize: 2,
	})

	ctrl.Finish()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 4, result.ErrorCount)
	require.Equal(t, 0, result.SuccessCount)
}
