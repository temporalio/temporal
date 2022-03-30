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

package reclaimresources

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

func Test_ReclaimResourcesWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil)

	env.OnActivity(a.EnsureNoExecutionsActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(nil)

	env.OnActivity(a.DeleteNamespaceActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(nil)

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result ReclaimResourcesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.ErrorCount)
	require.Equal(t, 10, result.SuccessCount)
}

func Test_ReclaimResourcesWorkflow_EnsureNoExecutionsActivity_Error(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil)

	env.OnActivity(a.EnsureNoExecutionsActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(stderrors.New("random error"))

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func Test_ReclaimResourcesWorkflow_EnsureNoExecutionsActivity_ExecutionsStillExist(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil)

	errorCount := 0
	env.OnActivity(a.EnsureNoExecutionsActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).
		Return(func(_ context.Context, namespaceID namespace.ID, namespaceName namespace.Name) error {
			errorCount++
			if errorCount < 20 {
				return errors.ErrExecutionsStillExist
			}
			return stderrors.New("random error")
		})

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

func Test_ReclaimResourcesWorkflow_NoActivityMocks_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().GetName().Return("elasticsearch").Times(3)

	countWorkflowExecutionsCallTimes := 1
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).DoAndReturn(func(_ context.Context, request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
		if countWorkflowExecutionsCallTimes == 3 {
			return &manager.CountWorkflowExecutionsResponse{
				Count: 0,
			}, nil
		}
		countWorkflowExecutionsCallTimes++
		return &manager.CountWorkflowExecutionsResponse{
			Count: 1,
		}, nil
	}).Times(3)

	metadataManager := persistence.NewMockMetadataManager(ctrl)
	metadataManager.EXPECT().DeleteNamespaceByName(gomock.Any(), &persistence.DeleteNamespaceByNameRequest{
		Name: "namespace",
	}).Return(nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		metadataManager:   metadataManager,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.EnsureNoExecutionsActivity)
	env.RegisterActivity(a.DeleteNamespaceActivity)

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecutionCount:               256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil)

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
	})

	ctrl.Finish()
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result ReclaimResourcesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.ErrorCount)
	require.Equal(t, 10, result.SuccessCount)
}
