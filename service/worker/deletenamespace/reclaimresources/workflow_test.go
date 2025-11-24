package reclaimresources

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.uber.org/mock/gomock"
)

func Test_ReclaimResourcesWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		TotalExecutionsCount: 10,
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil).Once()

	env.OnActivity(la.GetNamespaceCacheRefreshInterval, mock.Anything).Return(10*time.Second, nil).Once()
	env.OnActivity(la.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).Return(nil).Once()

	env.OnActivity(la.DeleteNamespaceActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(nil).Once()

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
		NamespaceDeleteDelay: 10 * time.Hour,
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		TotalExecutionsCount: 10,
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil).Once()

	env.OnActivity(la.GetNamespaceCacheRefreshInterval, mock.Anything).Return(10*time.Second, nil).Once()
	env.OnActivity(la.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
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
	require.Equal(t,
		err.Error(),
		"workflow execution error (type: ReclaimResourcesWorkflow, workflowID: default-test-workflow-id, runID: default-test-run-id): activity error (type: EnsureNoExecutionsAdvVisibilityActivity, scheduledEventID: 0, startedEventID: 0, identity: ): specific_error_from_activity")
}

func Test_ReclaimResourcesWorkflow_EnsureNoExecutionsActivity_ExecutionsStillExist(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		TotalExecutionsCount: 10,
		PreviousSuccessCount: 0,
		PreviousErrorCount:   0,
	}).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil).Once()

	env.OnActivity(la.GetNamespaceCacheRefreshInterval, mock.Anything).Return(10*time.Second, nil).Once()
	env.OnActivity(la.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).
		Return(errors.NewExecutionsStillExist(1)).
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)

	// For CountExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	// For EnsureNoExecutionsAdvVisibilityActivity.
	countWorkflowExecutionsCallTimes := 1
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
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
		logger:            log.NewTestLogger(),
	}
	la := &LocalActivities{
		visibilityManager:             visibilityManager,
		metadataManager:               metadataManager,
		namespaceCacheRefreshInterval: dynamicconfig.GetDurationPropertyFn(10 * time.Second),

		logger: log.NewTestLogger(),
	}

	env.RegisterActivity(la.GetNamespaceCacheRefreshInterval)
	env.RegisterActivity(la.CountExecutionsAdvVisibilityActivity)
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)
	env.RegisterActivity(la.DeleteNamespaceActivity)

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		TotalExecutionsCount: 1,
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)

	// For CountExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil)

	// For EnsureNoExecutionsAdvVisibilityActivity.
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Query:       sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.CountWorkflowExecutionsResponse{
		Count: 1,
	}, nil).
		Times(8)

	a := &Activities{
		visibilityManager: visibilityManager,
		logger:            log.NewTestLogger(),
	}
	la := &LocalActivities{
		visibilityManager:             visibilityManager,
		namespaceCacheRefreshInterval: dynamicconfig.GetDurationPropertyFn(10 * time.Second),
		logger:                        log.NewTestLogger(),
	}

	env.RegisterActivity(la.GetNamespaceCacheRefreshInterval)
	env.RegisterActivity(la.CountExecutionsAdvVisibilityActivity)
	env.RegisterActivity(a.EnsureNoExecutionsAdvVisibilityActivity)

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, deleteexecutions.DeleteExecutionsParams{
		Namespace:   "namespace",
		NamespaceID: "namespace-id",
		Config: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    100,
			PageSize:                             1000,
			PagesPerExecution:                    256,
			ConcurrentDeleteExecutionsActivities: 4,
		},
		TotalExecutionsCount: 1,
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

func Test_ReclaimResourcesWorkflow_UpdateDeleteDelay(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	env.OnWorkflow(deleteexecutions.DeleteExecutionsWorkflow, mock.Anything, mock.Anything).Return(deleteexecutions.DeleteExecutionsResult{
		SuccessCount: 10,
		ErrorCount:   0,
	}, nil).Once()

	env.OnActivity(la.GetNamespaceCacheRefreshInterval, mock.Anything).Return(10*time.Second, nil).Once()
	env.OnActivity(la.CountExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(int64(10), nil).Once()
	env.OnActivity(a.EnsureNoExecutionsAdvVisibilityActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace"), 0).Return(nil).Once()

	env.OnActivity(la.DeleteNamespaceActivity, mock.Anything, namespace.ID("namespace-id"), namespace.Name("namespace")).Return(nil).Once()

	timerNo := 0
	env.SetOnTimerScheduledListener(func(_ string, delayDuration time.Duration) {
		timerNo++
		// There are 2 timers in WF. Test needs to skip the first one.
		if timerNo == 2 {
			require.Equal(t, 10*time.Hour, delayDuration)
			uc := &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					require.Fail(t, "update should not be rejected")
				},
				OnAccept: func() {},
				OnComplete: func(r any, err error) {
					require.EqualValues(t, "Existing namespace delete delay timer is cancelled. Namespace delete delay is updated to 1h0m0s.", r)
				},
			}
			env.UpdateWorkflow("update_namespace_delete_delay", "", uc, "1h")
		}
		if timerNo == 3 {
			require.Equal(t, 1*time.Hour, delayDuration)
			uc := &testsuite.TestUpdateCallback{
				OnReject: func(err error) {
					require.Fail(t, "update should not be rejected")
				},
				OnAccept: func() {},
				OnComplete: func(r any, err error) {
					require.EqualValues(t, "Existing namespace delete delay timer is cancelled. Namespace delete delay is removed.", r)
				},
			}
			env.UpdateWorkflow("update_namespace_delete_delay", "", uc, "0")
		}
	})

	// If the timer is not updated, WF will fail.
	env.SetWorkflowRunTimeout(1 * time.Minute)

	env.ExecuteWorkflow(ReclaimResourcesWorkflow, ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:            "namespace",
			NamespaceID:          "namespace-id",
			Config:               deleteexecutions.DeleteExecutionsConfig{},
			PreviousSuccessCount: 0,
			PreviousErrorCount:   0,
		},
		NamespaceDeleteDelay: 10 * time.Hour,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result ReclaimResourcesResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 0, result.DeleteErrorCount)
	require.Equal(t, 10, result.DeleteSuccessCount)
	require.Equal(t, true, result.NamespaceDeleted)
}
