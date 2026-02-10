package queryworkflow

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type (
	apiSuite struct {
		suite.Suite
		*require.Assertions

		controller                 *gomock.Controller
		workflowConsistencyChecker *api.MockWorkflowConsistencyChecker
		workflowLease              api.WorkflowLease
		workflowContext            *historyi.MockWorkflowContext
		mutableState               *historyi.MockMutableState
		shardContext               *historyi.MockShardContext
		namespaceRegistry          *namespace.MockRegistry
		clusterMetadata            *cluster.MockMetadata
	}
)

func TestAPISuite(t *testing.T) {
	s := new(apiSuite)
	suite.Run(t, s)
}

func (s *apiSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *apiSuite) SetupSubTest() {
	s.controller = gomock.NewController(s.T())
	s.workflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.mutableState = historyi.NewMockMutableState(s.controller)
	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.namespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.workflowConsistencyChecker = api.NewMockWorkflowConsistencyChecker(s.controller)

	s.workflowLease = api.NewWorkflowLease(
		s.workflowContext,
		func(err error) {},
		s.mutableState,
	)
}

func (s *apiSuite) TearDownSubTest() {
	s.controller.Finish()
}

func (s *apiSuite) TestContextMetadataPopulated() {
	// This test suite verifies that SetContextMetadata is ALWAYS called for QueryWorkflow operations,
	// regardless of which code path is taken (success, rejected, paused, error, etc.).
	// SetContextMetadata is called early in the API handler, before any branching logic.

	s.Run("SUCCESS CASE - Query executed successfully", func() {
		// This is the MAIN case - a query that actually runs and returns results
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-id-success"
		runID := "test-run-id-success"
		workflowType := &commonpb.WorkflowType{Name: "test-workflow-type-success"}
		taskQueue := "test-task-queue-success"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		// Mock matching client to return successful query response
		mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(
			&matchingservice.QueryWorkflowResponse{
				QueryResult: payloads.EncodeString("query-success"),
			}, nil)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)
		s.shardContext.EXPECT().GetClusterMetadata().Return(s.clusterMetadata).AnyTimes()
		s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		// THIS IS THE KEY: SetContextMetadata is called
		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})

		// Setup for successful query dispatch
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		).AnyTimes()
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 0,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branch-token"),
						Items:       []*historyspb.VersionHistoryItem{{EventId: 10, Version: 1}},
					},
				},
			},
		}).AnyTimes()
		s.mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId:  runID,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}).AnyTimes()
		s.mutableState.EXPECT().GetCurrentBranchToken().Return([]byte("branch-token"), nil).AnyTimes()
		s.mutableState.EXPECT().GetNextEventID().Return(int64(10)).AnyTimes()
		s.mutableState.EXPECT().GetLastFirstEventIDTxnID().Return(int64(1), int64(1)).AnyTimes()
		s.mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(int64(5)).AnyTimes()
		s.mutableState.EXPECT().IsStickyTaskQueueSet().Return(false).AnyTimes()
		s.mutableState.EXPECT().GetAssignedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetInheritedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetMostRecentWorkerVersionStamp().Return(nil).AnyTimes()
		s.mutableState.EXPECT().HadOrHasWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
				QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NONE,
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, mockMatchingClient, mockMatchingClient)
		s.NoError(err, "MAIN success case should not error")
		s.NotNil(resp)
		s.NotNil(resp.Response)
		s.NotNil(resp.Response.QueryResult)

		// VERIFY: Context metadata was populated in the SUCCESS case
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set in success case")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set in success case")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("Completed workflow (rejected)", func() {
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-id"
		runID := "test-run-id"
		workflowType := &commonpb.WorkflowType{Name: "test-workflow-type"}
		taskQueue := "test-task-queue"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue: taskQueue,
		}).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
				QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Response.QueryRejected)

		// Verify context metadata was populated
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("Paused workflow", func() {
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-id-2"
		runID := "test-run-id-2"
		workflowType := &commonpb.WorkflowType{Name: "test-workflow-type-2"}
		taskQueue := "test-task-queue-2"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		// Return PAUSED status to trigger the paused workflow path
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue: taskQueue,
		}).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Response.QueryRejected)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, resp.Response.QueryRejected.Status)

		// Verify context metadata was populated even for paused workflow
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set for paused workflow")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set for paused workflow")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("Namespace validation error - metadata NOT set", func() {
		// Test early exit BEFORE SetContextMetadata
		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler)

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: "invalid-uuid",
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "test",
					RunId:      "test",
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err) // Should error on namespace validation

		// Verify context metadata was NOT set (error before SetContextMetadata)
		_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.False(ok, "workflow type should NOT be set - exited before SetContextMetadata")
	})

	s.Run("Namespace registry error - metadata NOT set", func() {
		// Test error when GetNamespaceByID fails
		namespaceID := namespace.ID(uuid.NewString())

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler)
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nil, serviceerror.NewNotFound("namespace not found"))

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "test-wf",
					RunId:      "test-run",
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err, "should error when GetNamespaceByID fails")

		// Verify metadata was NOT set (error before SetContextMetadata)
		_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.False(ok, "metadata should NOT be set - error before SetContextMetadata")
	})

	s.Run("GetCurrentWorkflowRunID path", func() {
		// Test when RunID is empty and needs to be fetched (lines 52-62)
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-runid"
		runID := "fetched-run-id"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type"}
		taskQueue := "test-queue"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		// Mock GetCurrentWorkflowRunID
		s.workflowConsistencyChecker.EXPECT().GetCurrentWorkflowRunID(
			gomock.Any(),
			namespaceID.String(),
			workflowID,
			locks.PriorityHigh,
		).Return(runID, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue: taskQueue,
		}).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					// RunId is EMPTY - will be fetched
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
				QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN,
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.NoError(err)
		s.NotNil(resp)

		// VERIFY: Context metadata set even when RunID was fetched
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set after RunID fetch")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set after RunID fetch")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("Running workflow with no reject condition", func() {
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-id-3"
		runID := "test-run-id-3"
		workflowType := &commonpb.WorkflowType{Name: "test-workflow-type-3"}
		taskQueue := "test-task-queue-3"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 0, // Not in failed state
		}).AnyTimes()
		// Return false so it triggers "workflow closed before task started" error
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
		s.mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(false)

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err) // Should return ErrWorkflowClosedBeforeWorkflowTaskStarted

		// Verify context metadata was populated even when query fails with error
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set even when query fails")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set even when query fails")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("GetWorkflowLease error - metadata NOT set", func() {
		// Test error BEFORE SetContextMetadata is called
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-lease-error"
		runID := "test-run-lease-error"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		// Return error from GetWorkflowLease - this happens BEFORE SetContextMetadata
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(nil, serviceerror.NewNotFound("workflow not found"))

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err, "should error when GetWorkflowLease fails")

		// Verify context metadata was NOT set (error before SetContextMetadata)
		_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.False(ok, "metadata should NOT be set - error before SetContextMetadata call")
	})

	s.Run("NOT_COMPLETED_CLEANLY reject condition", func() {
		// Test QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY path
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-not-cleanly"
		runID := "test-run-not-cleanly"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type-cleanly"}
		taskQueue := "test-queue-cleanly"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		// Return FAILED status (not completed cleanly)
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue: taskQueue,
		}).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
				// This should trigger rejection because workflow is FAILED (not completed cleanly)
				QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY,
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Response.QueryRejected)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, resp.Response.QueryRejected.Status)

		// Verify metadata was set
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set for NOT_COMPLETED_CLEANLY path")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set for NOT_COMPLETED_CLEANLY path")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("WorkflowTask failing repeatedly (attempt >= 3)", func() {
		// Test fail-fast path when workflow task keeps failing
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-task-failing"
		runID := "test-run-task-failing"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type-failing"}
		taskQueue := "test-queue-failing"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)
		s.shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		)
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 5, // >= 3, should fail fast
		}).AnyTimes()
		// !IsWorkflowExecutionRunning() && !HasCompletedAnyWorkflowTask() - returns true so short circuits, HasCompletedAnyWorkflowTask never called
		// !HadOrHasWorkflowTask() - returns true so this is false, block is skipped
		// GetExecutionInfo().WorkflowTaskAttempt >= 3 - this triggers the fail-fast error
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
		s.mutableState.EXPECT().HadOrHasWorkflowTask().Return(true)

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err, "should fail fast when workflow task attempt >= 3")

		// VERIFY: Context metadata set before failing fast
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set even when failing fast")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set even when failing fast")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("Namespace not active in cluster - dispatch directly", func() {
		// Test safeToDispatchDirectly when namespace is not active
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-not-active"
		runID := "test-run-not-active"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type-not-active"}
		taskQueue := "test-queue-not-active"

		// Namespace is active in "other-cluster", NOT in "test-cluster"
		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"other-cluster", // Different from current cluster
		)

		mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(
			&matchingservice.QueryWorkflowResponse{
				QueryResult: payloads.EncodeString("query-result-not-active"),
			}, nil)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)
		s.shardContext.EXPECT().GetClusterMetadata().Return(s.clusterMetadata).AnyTimes()
		s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		).AnyTimes()
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 0,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branch-token"),
						Items:       []*historyspb.VersionHistoryItem{{EventId: 10, Version: 1}},
					},
				},
			},
		}).AnyTimes()
		s.mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId:  runID,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}).AnyTimes()
		s.mutableState.EXPECT().GetCurrentBranchToken().Return([]byte("branch-token"), nil).AnyTimes()
		s.mutableState.EXPECT().GetNextEventID().Return(int64(10)).AnyTimes()
		s.mutableState.EXPECT().GetLastFirstEventIDTxnID().Return(int64(1), int64(1)).AnyTimes()
		s.mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(int64(5)).AnyTimes()
		s.mutableState.EXPECT().IsStickyTaskQueueSet().Return(false).AnyTimes()
		s.mutableState.EXPECT().GetAssignedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetInheritedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetMostRecentWorkerVersionStamp().Return(nil).AnyTimes()
		s.mutableState.EXPECT().HadOrHasWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
		// These are checked when determining safeToDispatchDirectly
		s.mutableState.EXPECT().HasPendingWorkflowTask().Return(false).AnyTimes()
		s.mutableState.EXPECT().HasStartedWorkflowTask().Return(false).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, mockMatchingClient, mockMatchingClient)
		s.NoError(err, "should succeed when dispatching directly (namespace not active)")
		s.NotNil(resp)
		s.NotNil(resp.Response)

		// VERIFY: Context metadata set for namespace not active path
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set for direct dispatch path")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set for direct dispatch path")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("GetCurrentWorkflowRunID error path", func() {
		// Test error when fetching current run ID
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-runid-error"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)

		// Return error from GetCurrentWorkflowRunID
		s.workflowConsistencyChecker.EXPECT().GetCurrentWorkflowRunID(
			gomock.Any(),
			namespaceID.String(),
			workflowID,
			locks.PriorityHigh,
		).Return("", serviceerror.NewNotFound("workflow not found"))

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					// RunId is EMPTY - will try to fetch but fail
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		_, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, nil, nil)
		s.Error(err, "should error when GetCurrentWorkflowRunID fails")

		// Verify metadata was NOT set (error before SetContextMetadata)
		_, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.False(ok, "metadata should NOT be set - error before SetContextMetadata")
	})

	s.Run("Workflow not running - dispatch directly", func() {
		// Test safeToDispatchDirectly when workflow is not running
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-not-running"
		runID := "test-run-not-running"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type-not-running"}
		taskQueue := "test-queue-not-running"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(
			&matchingservice.QueryWorkflowResponse{
				QueryResult: payloads.EncodeString("query-result-not-running"),
			}, nil)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)
		s.shardContext.EXPECT().GetClusterMetadata().Return(s.clusterMetadata).AnyTimes()
		s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		).AnyTimes()
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 0,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branch-token"),
						Items:       []*historyspb.VersionHistoryItem{{EventId: 10, Version: 1}},
					},
				},
			},
		}).AnyTimes()
		s.mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId:  runID,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}).AnyTimes()
		s.mutableState.EXPECT().GetCurrentBranchToken().Return([]byte("branch-token"), nil).AnyTimes()
		s.mutableState.EXPECT().GetNextEventID().Return(int64(10)).AnyTimes()
		s.mutableState.EXPECT().GetLastFirstEventIDTxnID().Return(int64(1), int64(1)).AnyTimes()
		s.mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(int64(5)).AnyTimes()
		s.mutableState.EXPECT().IsStickyTaskQueueSet().Return(false).AnyTimes()
		s.mutableState.EXPECT().GetAssignedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetInheritedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetMostRecentWorkerVersionStamp().Return(nil).AnyTimes()
		s.mutableState.EXPECT().HadOrHasWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(true).AnyTimes()
		// Workflow is NOT running - this makes safeToDispatchDirectly = true
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, mockMatchingClient, mockMatchingClient)
		s.NoError(err, "should succeed when dispatching directly (workflow not running)")
		s.NotNil(resp)
		s.NotNil(resp.Response)

		// VERIFY: Context metadata set for workflow not running path
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set for direct dispatch path")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set for direct dispatch path")
		s.Equal(taskQueue, contextTaskQueue)
	})

	s.Run("No pending/started workflow tasks - dispatch directly", func() {
		// Test safeToDispatchDirectly when no pending/started tasks
		namespaceID := namespace.ID(uuid.NewString())
		workflowID := "test-workflow-no-pending"
		runID := "test-run-no-pending"
		workflowType := &commonpb.WorkflowType{Name: "test-wf-type-no-pending"}
		taskQueue := "test-queue-no-pending"

		nsEntry := namespace.NewLocalNamespaceForTest(
			&persistencespb.NamespaceInfo{Name: "test-namespace"},
			&persistencespb.NamespaceConfig{},
			"test-cluster",
		)

		mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(s.controller)
		mockMatchingClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(
			&matchingservice.QueryWorkflowResponse{
				QueryResult: payloads.EncodeString("query-result-no-pending"),
			}, nil)

		s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
		s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry)
		s.namespaceRegistry.EXPECT().GetNamespaceByID(namespaceID).Return(nsEntry, nil)
		s.shardContext.EXPECT().GetClusterMetadata().Return(s.clusterMetadata).AnyTimes()
		s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

		workflowKey := definition.NewWorkflowKey(namespaceID.String(), workflowID, runID)
		s.workflowConsistencyChecker.EXPECT().GetWorkflowLease(
			gomock.Any(),
			nil,
			workflowKey,
			locks.PriorityHigh,
		).Return(s.workflowLease, nil)

		s.mutableState.EXPECT().SetContextMetadata(gomock.Any()).Do(func(ctx context.Context) {
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowType, workflowType.GetName())
			contextutil.ContextMetadataSet(ctx, contextutil.MetadataKeyWorkflowTaskQueue, taskQueue)
		})
		s.mutableState.EXPECT().GetWorkflowStateStatus().Return(
			enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		).AnyTimes()
		s.mutableState.EXPECT().GetWorkflowType().Return(workflowType).AnyTimes()
		s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
			TaskQueue:           taskQueue,
			WorkflowTaskAttempt: 0,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branch-token"),
						Items:       []*historyspb.VersionHistoryItem{{EventId: 10, Version: 1}},
					},
				},
			},
		}).AnyTimes()
		s.mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
			RunId:  runID,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}).AnyTimes()
		s.mutableState.EXPECT().GetCurrentBranchToken().Return([]byte("branch-token"), nil).AnyTimes()
		s.mutableState.EXPECT().GetNextEventID().Return(int64(10)).AnyTimes()
		s.mutableState.EXPECT().GetLastFirstEventIDTxnID().Return(int64(1), int64(1)).AnyTimes()
		s.mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(int64(5)).AnyTimes()
		s.mutableState.EXPECT().IsStickyTaskQueueSet().Return(false).AnyTimes()
		s.mutableState.EXPECT().GetAssignedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetInheritedBuildId().Return("").AnyTimes()
		s.mutableState.EXPECT().GetMostRecentWorkerVersionStamp().Return(nil).AnyTimes()
		s.mutableState.EXPECT().HadOrHasWorkflowTask().Return(true).AnyTimes()
		s.mutableState.EXPECT().HasCompletedAnyWorkflowTask().Return(true).AnyTimes()
		// Workflow IS running, namespace IS active in cluster, but NO pending/started tasks
		// This makes safeToDispatchDirectly = true
		s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
		s.mutableState.EXPECT().HasPendingWorkflowTask().Return(false)
		s.mutableState.EXPECT().HasStartedWorkflowTask().Return(false)

		ctx := contextutil.WithMetadataContext(context.Background())

		request := &historyservice.QueryWorkflowRequest{
			NamespaceId: namespaceID.String(),
			Request: &workflowservice.QueryWorkflowRequest{
				Namespace: "test-namespace",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
				Query: &querypb.WorkflowQuery{
					QueryType: "test-query",
				},
			},
		}

		resp, err := Invoke(ctx, request, s.shardContext, s.workflowConsistencyChecker, mockMatchingClient, mockMatchingClient)
		s.NoError(err, "should succeed when no pending/started tasks")
		s.NotNil(resp)
		s.NotNil(resp.Response)

		// VERIFY: Context metadata set for no pending/started tasks path
		contextWorkflowType, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowType)
		s.True(ok, "context workflow type MUST be set for direct dispatch path")
		s.Equal(workflowType.GetName(), contextWorkflowType)

		contextTaskQueue, ok := contextutil.ContextMetadataGet(ctx, contextutil.MetadataKeyWorkflowTaskQueue)
		s.True(ok, "context task queue MUST be set for direct dispatch path")
		s.Equal(taskQueue, contextTaskQueue)
	})

}
