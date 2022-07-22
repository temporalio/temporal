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
	"go.temporal.io/sdk/temporal"
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
	}, nil).Once()

	env.OnActivity(a.IsAdvancedVisibilityActivity, mock.Anything).Return(true, nil).Once()
	env.OnActivity(a.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).Return(nil).Once()

	env.OnActivity(a.DeleteNamespaceActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(nil).Once()

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
	require.Equal(t, 0, result.DeleteErrorCount)
	require.Equal(t, 10, result.DeleteSuccessCount)
	require.Equal(t, true, result.NamespaceDeleted)
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
	}, nil).Once()

	env.OnActivity(a.IsAdvancedVisibilityActivity, mock.Anything).Return(true, nil).Once()
	env.OnActivity(a.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).
		Return(stderrors.New("specific_error_from_activity")).
		Times(10) // GoSDK defaultMaximumAttemptsForUnitTest value.

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
	err := env.GetWorkflowError()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to execute activity: EnsureNoExecutionsActivity")
	require.Contains(t, err.Error(), "specific_error_from_activity")
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
	}, nil).Once()

	env.OnActivity(a.IsAdvancedVisibilityActivity, mock.Anything).Return(true, nil).Once()
	env.OnActivity(a.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).
		Return(errors.NewExecutionsStillExistError(1)).
		Times(10) // GoSDK defaultMaximumAttemptsForUnitTest value.

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
	err := env.GetWorkflowError()
	var appErr *temporal.ApplicationError
	require.True(t, stderrors.As(err, &appErr))
	require.Equal(t, errors.ExecutionsStillExistErrType, appErr.Type())
}

func Test_ReclaimResourcesWorkflow_NoActivityMocks_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().GetName().Return("elasticsearch")

	// For CountExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	// For EnsureNoExecutionsAdvVisibilityActivity.
	countWorkflowExecutionsCallTimes := 1
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).DoAndReturn(func(_ context.Context, request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
		if countWorkflowExecutionsCallTimes == 8 {
			return &manager.CountWorkflowExecutionsResponse{
				Count: 0,
			}, nil
		}
		countWorkflowExecutionsCallTimes++
		// Return same "1" 8 times to emulate ErrNoProgress.
		return &manager.CountWorkflowExecutionsResponse{
			Count: 1,
		}, nil
	}).Times(8)

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

	env.RegisterActivity(a.IsAdvancedVisibilityActivity)
	env.RegisterActivity(a.CountExecutionsAdvVisibilityActivity)
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)
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

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result ReclaimResourcesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.DeleteErrorCount)
	require.Equal(t, 10, result.DeleteSuccessCount)
	require.Equal(t, true, result.NamespaceDeleted)
}

func Test_ReclaimResourcesWorkflow_NoActivityMocks_NoProgressMade(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().GetName().Return("elasticsearch")

	// For CountExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	// For EnsureNoExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil).
		Times(8)

	a := &Activities{
		visibilityManager: visibilityManager,
		metricsClient:     metrics.NoopClient,
		logger:            log.NewNoopLogger(),
	}

	env.RegisterActivity(a.IsAdvancedVisibilityActivity)
	env.RegisterActivity(a.CountExecutionsAdvVisibilityActivity)
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)

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
	}, nil).Twice()

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
	err := env.GetWorkflowError()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, stderrors.As(err, &appErr))
	require.Equal(t, errors.NoProgressErrType, appErr.Type())
}
