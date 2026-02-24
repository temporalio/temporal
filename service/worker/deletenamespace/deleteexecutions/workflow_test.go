package deleteexecutions

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.uber.org/mock/gomock"
)

func Test_DeleteExecutionsWorkflow_Success(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.OnActivity(la.GetNextPageTokenActivity, mock.Anything, GetNextPageTokenParams{
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      1000,
		NextPageToken: nil,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions:    nil,
		NextPageToken: nil,
	}, nil).Times(2)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     nil,
		deleteActivityRPS: func(callback func(int)) (v int, cancel func()) {
			return 100, func() {}
		},
		metricsHandler: nil,
		logger:         nil,
	}
	la := &LocalActivities{
		visibilityManager: visibilityManager,
	}

	env.RegisterActivity(la.GetNextPageTokenActivity)
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	pageNumber := atomic.Int32{}
	env.OnActivity(la.GetNextPageTokenActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params GetNextPageTokenParams) ([]byte, error) {
		require.Equal(t, namespace.Name("namespace"), params.Namespace)
		require.Equal(t, namespace.ID("namespace-id"), params.NamespaceID)
		require.Equal(t, 3, params.PageSize)
		if pageNumber.Load() == 0 {
			require.Nil(t, params.NextPageToken)
		} else {
			require.Equal(t, []byte{3, 22, 83}, params.NextPageToken)
		}
		pageNumber.Add(1)
		if pageNumber.Load() == 100 { // Emulate 100 pages of executions.
			return nil, nil
		}
		return []byte{3, 22, 83}, nil
	}).Times(100)

	nilTokenOnce := atomic.Bool{}
	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
		require.Equal(t, namespace.Name("namespace"), params.Namespace)
		require.Equal(t, namespace.ID("namespace-id"), params.NamespaceID)
		require.Equal(t, 3, params.ListPageSize)
		if params.NextPageToken == nil {
			nilTokenOnce.Store(true)
		} else if nilTokenOnce.Load() {
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.OnActivity(la.GetNextPageTokenActivity, mock.Anything, mock.Anything).Return([]byte{3, 22, 83}, nil).Times(78)
	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).Return(DeleteExecutionsActivityResult{SuccessCount: 1, ErrorCount: 0}, nil).Times(78)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			PageSize:          3,
			PagesPerExecution: 78,
		},
	})

	require.True(t, env.IsWorkflowCompleted())
	wfErr := env.GetWorkflowError()
	require.Error(t, wfErr)
	var errContinueAsNew *workflow.ContinueAsNewError
	require.ErrorAs(t, wfErr, &errContinueAsNew)

	require.NotNil(t, errContinueAsNew.Input)
	var newWfParams DeleteExecutionsParams
	err := payloads.Decode(errContinueAsNew.Input, &newWfParams)
	require.NoError(t, err)
	require.Equal(t, 78, newWfParams.PreviousSuccessCount)
	require.Equal(t, 0, newWfParams.PreviousErrorCount)
	require.Equal(t, []byte{3, 22, 83}, newWfParams.NextPageToken)
}

func Test_DeleteExecutionsWorkflow_ManyExecutions_ActivityError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	var a *Activities
	var la *LocalActivities

	env.OnActivity(la.GetNextPageTokenActivity, mock.Anything, mock.Anything).
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
	require.Equal(t, appErr.Error(), "specific_error_from_activity (type: Unavailable, retryable: true)")
}

func Test_DeleteExecutionsWorkflow_NoActivityMocks_ManyExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: nil,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
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
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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

	historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found")).Times(2)
	// NotFound errors should not affect neither error nor success count.
	historyClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))
	// NotFound in main store but no error from visibility store adds 1 to success count.
	historyClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), gomock.Any()).Return(nil, nil)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		deleteActivityRPS: func(callback func(int)) (v int, cancel func()) {
			return 100, func() {}
		},
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}
	la := &LocalActivities{
		visibilityManager: visibilityManager,
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewTestLogger(),
	}

	env.RegisterActivity(la.GetNextPageTokenActivity)
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
	require.Equal(t, 3, result.SuccessCount)
}

func Test_DeleteExecutionsWorkflow_NoActivityMocks_ChasmExecutions(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	execution1 := &commonpb.WorkflowExecution{
		WorkflowId: "workflow-id-1",
		RunId:      "run-id-1",
	}
	archetypeID1 := 12345
	execution2 := &commonpb.WorkflowExecution{
		WorkflowId: "workflow-id-2",
		RunId:      "run-id-2",
	}
	archetypeID2 := 54321

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: nil,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: execution1,
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						sadefs.TemporalNamespaceDivision: payload.EncodeString(strconv.FormatUint(uint64(archetypeID1), 10)),
					},
				},
			},
			{
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: execution2,
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						sadefs.TemporalNamespaceDivision: payload.EncodeString(strconv.FormatUint(uint64(archetypeID2), 10)),
					},
				},
			},
		},
	}, nil).Times(2)

	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	historyClient.EXPECT().ForceDeleteWorkflowExecution(gomock.Any(), &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: "namespace-id",
		ArchetypeId: uint32(archetypeID1),
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: execution1,
		},
	}).Return(nil, nil).Times(1)
	historyClient.EXPECT().ForceDeleteWorkflowExecution(gomock.Any(), &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: "namespace-id",
		ArchetypeId: uint32(archetypeID2),
		Request: &adminservice.DeleteWorkflowExecutionRequest{
			Execution: execution2,
		},
	}).Return(nil, nil).Times(1)

	a := &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		deleteActivityRPS: func(callback func(int)) (v int, cancel func()) {
			return 100, func() {}
		},
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}
	la := &LocalActivities{
		visibilityManager: visibilityManager,
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewTestLogger(),
	}

	env.RegisterActivity(la.GetNextPageTokenActivity)
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
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()

	ctrl := gomock.NewController(t)
	visibilityManager := manager.NewMockVisibilityManager(ctrl)
	// First page.
	visibilityManager.EXPECT().ListWorkflowExecutions(gomock.Any(), &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "namespace",
		PageSize:      2,
		NextPageToken: nil,
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
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
		Query:         sadefs.QueryWithAnyNamespaceDivision(""),
	}).Return(&manager.ListWorkflowExecutionsResponse{
		Executions: []*workflowpb.WorkflowExecutionInfo{
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "workflow-id-1",
					RunId:      "run-id-1",
				},
			},
			{
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
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
		deleteActivityRPS: func(callback func(int)) (v int, cancel func()) {
			return 100, func() {}
		},
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}
	la := &LocalActivities{
		visibilityManager: visibilityManager,
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewTestLogger(),
	}

	env.RegisterActivity(la.GetNextPageTokenActivity)
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

func Test_DeleteExecutionsWorkflow_QueryStats(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	testSuite.SetLogger(log.NewSdkLogger(log.NewTestLogger()))
	env := testSuite.NewTestWorkflowEnvironment()
	startTime := env.Now().UTC()

	var a *Activities
	var la *LocalActivities

	pageNumber := 0
	env.OnActivity(la.GetNextPageTokenActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params GetNextPageTokenParams) ([]byte, error) {
		pageNumber++

		if pageNumber == 4 { // Emulate 100 pages of executions.
			return nil, nil
		}
		return []byte{3, 22, 83}, nil
	}).Times(4)

	env.OnActivity(a.DeleteExecutionsActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
		return DeleteExecutionsActivityResult{
			ErrorCount:   10,
			SuccessCount: 220,
		}, nil
	}).Times(4).After(5 * time.Second)

	queryStatsFn := func() {
		ev, err := env.QueryWorkflow("stats")
		require.NoError(t, err)
		var des DeleteExecutionsStats
		err = ev.Get(&des)
		require.NoError(t, err)

		desJson, err := json.Marshal(des)
		require.NoError(t, err)
		testSuite.GetLogger().Info("Current stats.", "pageNumber", pageNumber, "DeleteExecutionsStats", string(desJson))

		require.Equal(t, 10*(pageNumber-1), des.DeleteExecutionsResult.ErrorCount)
		require.Equal(t, 220*(pageNumber-1), des.DeleteExecutionsResult.SuccessCount)
		require.Equal(t, (10+220)*4, des.TotalExecutionsCount)
		require.Equal(t, (10+220)*(4-(pageNumber-1)), des.RemainingExecutionsCount)
		require.Equal(t, (10+220)/5, des.AverageRPS) // 5 seconds for every activity run.
		require.Equal(t, startTime, des.StartTime)
		require.Equal(t, (10+220)*(4-(pageNumber-1))/((10+220)/5), int(des.ApproximateTimeLeft.Seconds()))                                 // Remaining executions / average RPS.
		require.Equal(t, env.Now().UTC().Add(time.Duration((10+220)*(4-(pageNumber-1))/((10+220)/5))*time.Second), des.ApproximateEndTime) // now + time left.
	}

	env.RegisterDelayedCallback(queryStatsFn, 5100*time.Millisecond)
	env.RegisterDelayedCallback(queryStatsFn, 10100*time.Millisecond)
	env.RegisterDelayedCallback(queryStatsFn, 15100*time.Millisecond)

	env.ExecuteWorkflow(DeleteExecutionsWorkflow, DeleteExecutionsParams{
		NamespaceID: "namespace-id",
		Namespace:   "namespace",
		Config: DeleteExecutionsConfig{
			ConcurrentDeleteExecutionsActivities: 1, // To linearize the execution of activities.
		},
		TotalExecutionsCount: (10 + 220) * 4,
		FirstRunStartTime:    startTime,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result DeleteExecutionsResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 10*4, result.ErrorCount)
	require.Equal(t, 220*4, result.SuccessCount)
}
