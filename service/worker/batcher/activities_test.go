package batcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller *gomock.Controller

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) TestTaskTimeoutContext() {
	s.Run("no parent deadline applies default timeout", func() {
		ctx, cancel := taskTimeoutContext(context.Background())
		defer cancel()

		deadline, ok := ctx.Deadline()
		s.True(ok)
		s.InDelta(defaultTaskTimeout, time.Until(deadline), float64(time.Second))
	})

	s.Run("longer parent deadline is shortened to default timeout", func() {
		parent, parentCancel := context.WithTimeout(context.Background(), defaultTaskTimeout+time.Hour)
		defer parentCancel()

		ctx, cancel := taskTimeoutContext(parent)
		defer cancel()

		deadline, ok := ctx.Deadline()
		s.True(ok)
		s.InDelta(defaultTaskTimeout, time.Until(deadline), float64(time.Second))
	})

	s.Run("shorter parent deadline is preserved", func() {
		shorter := defaultTaskTimeout - 5*time.Second
		parent, parentCancel := context.WithTimeout(context.Background(), shorter)
		defer parentCancel()

		ctx, cancel := taskTimeoutContext(parent)
		defer cancel()

		// The parent context is returned unchanged so we never extend an
		// existing, shorter deadline.
		s.Equal(parent, ctx)
		deadline, ok := ctx.Deadline()
		s.True(ok)
		s.InDelta(shorter, time.Until(deadline), float64(time.Second))
	})
}

const NumTotalEvents = 10

// Pattern contains either c or f representing completed or failed task.
// Schedule events for each task has id of NumTotalEvents*i + 1 where i is the index of the character
// EventId for each task has id of NumTotalEvents*i+NumTotalEvents where i is the index of the character
func generateEventHistory(pattern string) *historypb.History {
	events := make([]*historypb.HistoryEvent, 0)
	for i, char := range pattern {
		// add a Schedule event independent of type of event
		scheduledEventId := int64(NumTotalEvents*i + 1)
		scheduledEvent := historypb.HistoryEvent{EventId: scheduledEventId, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}
		events = append(events, &scheduledEvent)

		event := historypb.HistoryEvent{EventId: int64(NumTotalEvents*i + NumTotalEvents)}
		switch unicode.ToLower(char) {
		case 'c':
			event.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
			event.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: scheduledEventId},
			}
		case 'f':
			event.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
		}
		events = append(events, &event)
	}

	return &historypb.History{Events: events}
}

func (s *activitiesSuite) TestGetLastWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	tests := []struct {
		name                    string
		history                 *historypb.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc"),
			wantWorkflowTaskEventID: NumTotalEvents*4 + NumTotalEvents,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf"),
			wantWorkflowTaskEventID: NumTotalEvents*3 + NumTotalEvents,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff"),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc"),
			wantWorkflowTaskEventID: NumTotalEvents*4 + NumTotalEvents,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory(""),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			ctx := context.Background()
			slices.Reverse(tt.history.Events)
			workflowExecution := &commonpb.WorkflowExecution{}
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistoryReverse(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getLastWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, gotWorkflowTaskEventID)
			if tt.wantErr {
				var appErr *temporal.ApplicationError
				s.Require().ErrorAs(err, &appErr, "error should be an ApplicationError")
				s.True(appErr.NonRetryable(), "error should be non-retryable")
				s.Equal("NoWorkflowTaskFound", appErr.Type(), "error type should be NoWorkflowTaskFound")
			}
		})
	}
}

func (s *activitiesSuite) TestGetFirstWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	workflowExecution := commonpb.WorkflowExecution{}
	tests := []struct {
		name                    string
		history                 *historypb.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:                    "Test history with first task failing",
			history:                 generateEventHistory("fcccc"),
			wantWorkflowTaskEventID: NumTotalEvents*1 + NumTotalEvents,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff"),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory(""),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			ctx := context.Background()
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistory(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryResponse{History: tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getFirstWorkflowTaskEventID(ctx, namespaceStr, &workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, gotWorkflowTaskEventID)
			if tt.wantErr {
				var appErr *temporal.ApplicationError
				s.Require().ErrorAs(err, &appErr, "error should be an ApplicationError")
				s.True(appErr.NonRetryable(), "error should be non-retryable")
				s.Equal("NoWorkflowTaskFound", appErr.Type(), "error type should be NoWorkflowTaskFound")
			}
		})
	}
}

func (s *activitiesSuite) TestGetResetPoint() {
	ctx := context.Background()
	ns := "namespacename"
	tests := []struct {
		name                    string
		points                  []*workflowpb.ResetPointInfo
		buildId                 string
		currentRunOnly          bool
		wantWorkflowTaskEventID int64
		wantErr                 bool
		wantSetRunId            string
	}{
		{
			name: "not found",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
				},
			},
			buildId: "otherbuild",
			wantErr: true,
		},
		{
			name: "found",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
				},
			},
			buildId:                 "build1",
			wantWorkflowTaskEventID: 123,
		},
		{
			name: "not resettable",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   false,
				},
			},
			buildId: "build1",
			wantErr: true,
		},
		{
			name: "from another run",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run0",
					FirstWorkflowTaskCompletedId: 34,
					Resettable:                   true,
				},
			},
			buildId:                 "build1",
			wantWorkflowTaskEventID: 34,
			wantSetRunId:            "run0",
		},
		{
			name: "from another run but not allowed",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run0",
					FirstWorkflowTaskCompletedId: 34,
					Resettable:                   true,
				},
			},
			buildId:        "build1",
			currentRunOnly: true,
			wantErr:        true,
		},
		{
			name: "expired",
			points: []*workflowpb.ResetPointInfo{
				{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
					ExpireTime:                   timestamp.TimePtr(time.Now().Add(-1 * time.Hour)),
				},
			},
			buildId: "build1",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.mockFrontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&workflowservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						AutoResetPoints: &workflowpb.ResetPoints{
							Points: tt.points,
						},
					},
				},
				nil,
			)
			execution := &commonpb.WorkflowExecution{
				WorkflowId: "wfid",
				RunId:      "run1",
			}
			id, err := getResetPoint(ctx, ns, execution, s.mockFrontendClient, tt.buildId, tt.currentRunOnly)
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, id)
			if tt.wantSetRunId != "" {
				s.Equal(tt.wantSetRunId, execution.RunId)
			}
		})
	}
}

func (s *activitiesSuite) TestAdjustQueryBatchTypeEnum() {
	tests := []struct {
		name           string
		query          string
		expectedResult string
		batchType      enumspb.BatchOperationType
	}{
		{
			name:           "Empty query",
			query:          "",
			expectedResult: "",
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Acceptance",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Acceptance with parenthesis",
			query:          "(A=B)",
			expectedResult: fmt.Sprintf("((A=B)) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Acceptance with multiple conditions",
			query:          "(A=B) OR C=D",
			expectedResult: fmt.Sprintf("((A=B) OR C=D) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Contains status - 1",
			query:          "ExecutionStatus=Completed",
			expectedResult: fmt.Sprintf("(ExecutionStatus=Completed) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Contains status - 2",
			query:          "A=B OR ExecutionStatus='Completed'",
			expectedResult: fmt.Sprintf("(A=B OR ExecutionStatus='Completed') AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		{
			name:           "Not supported batch type",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED,
		},
		{
			name:           "Terminate workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Terminate legacy workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Cancel workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_CANCEL_WORKFLOW,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Cancel legacy workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_CANCEL,
		},
		{
			name:           "Signal workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Signal legacy workflow variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_SIGNAL,
		},
		{
			name:           "Update workflow execution options variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Update legacy workflow execution options variant",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningOrPausedQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
		},
		{
			// Reset applies regardless of execution status (matches the legacy
			// BATCH_OPERATION_TYPE_RESET, which is also excluded), so no filter is added.
			name:           "Reset workflow variant is not filtered",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Reset legacy workflow variant is not filtered",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_RESET,
		},
		{
			// Delete applies regardless of execution status (matches the legacy
			// BATCH_OPERATION_TYPE_DELETE, which is also excluded), so no filter is added.
			name:           "Delete workflow variant is not filtered",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_DELETE_WORKFLOW,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:           "Delete legacy workflow variant is not filtered",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_DELETE,
		},
		{
			// A caller must be able to terminate/cancel non-terminal activities via
			// query; the server adds the filter so the caller doesn't have to.
			name:           "Terminate activity is filtered to running and paused",
			query:          "ActivityType='foo'",
			expectedResult: "(ActivityType='foo') AND (ExecutionStatus='Running' OR ExecutionStatus='Paused')",
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
		},
		{
			name:           "Cancel activity is filtered to running and paused",
			query:          "ActivityType='foo'",
			expectedResult: "(ActivityType='foo') AND (ExecutionStatus='Running' OR ExecutionStatus='Paused')",
			batchType:      enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
		},
		{
			name:           "Unpause workflow activity is filtered to running and paused workflows",
			query:          "WorkflowType='foo'",
			expectedResult: "(WorkflowType='foo') AND (ExecutionStatus='Running' OR ExecutionStatus='Paused')",
			batchType:      enumspb.BATCH_OPERATION_TYPE_UNPAUSE_ACTIVITY,
		},
		{
			name:           "Update workflow activity options is filtered to running and paused workflows",
			query:          "WorkflowType='foo'",
			expectedResult: "(WorkflowType='foo') AND (ExecutionStatus='Running' OR ExecutionStatus='Paused')",
			batchType:      enumspb.BATCH_OPERATION_TYPE_UPDATE_ACTIVITY_OPTIONS,
		},
		{
			name:           "Reset workflow activity is filtered to running and paused workflows",
			query:          "WorkflowType='foo'",
			expectedResult: "(WorkflowType='foo') AND (ExecutionStatus='Running' OR ExecutionStatus='Paused')",
			batchType:      enumspb.BATCH_OPERATION_TYPE_RESET_ACTIVITY,
		},
		{
			// Delete applies regardless of execution status, so no filter is added.
			name:           "Delete activity is not filtered",
			query:          "ActivityType='foo'",
			expectedResult: "ActivityType='foo'",
			batchType:      enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY,
		},
	}
	for _, testRun := range tests {
		s.Run(testRun.name, func() {
			a := activities{}
			adjustedQuery := a.adjustQueryBatchTypeEnum(testRun.query, testRun.batchType)
			s.Equal(testRun.expectedResult, adjustedQuery)
		})
	}
}

func (s *activitiesSuite) TestAdjustQueryAdminBatchType() {
	a := activities{}

	s.Run("Empty query", func() {
		adminReq := &adminservice.StartAdminBatchOperationRequest{
			VisibilityQuery: "",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		}
		adjustedQuery := a.adjustQueryAdminBatchType(adminReq)
		s.Empty(adjustedQuery)
	})

	s.Run("RefreshWorkflowTasks returns query unchanged", func() {
		adminReq := &adminservice.StartAdminBatchOperationRequest{
			VisibilityQuery: "WorkflowType='MyWorkflow'",
			Identity:        "test",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		}
		adjustedQuery := a.adjustQueryAdminBatchType(adminReq)
		// RefreshWorkflowTasks applies to both open and closed workflows, no filter added
		s.Equal("WorkflowType='MyWorkflow'", adjustedQuery)
	})

	s.Run("RefreshWorkflowTasks with complex query unchanged", func() {
		adminReq := &adminservice.StartAdminBatchOperationRequest{
			VisibilityQuery: "(WorkflowType='MyWorkflow') OR (WorkflowType='OtherWorkflow')",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		}
		adjustedQuery := a.adjustQueryAdminBatchType(adminReq)
		// RefreshWorkflowTasks applies to both open and closed workflows, no filter added
		s.Equal("(WorkflowType='MyWorkflow') OR (WorkflowType='OtherWorkflow')", adjustedQuery)
	})

	s.Run("Nil operation returns query unchanged", func() {
		adminReq := &adminservice.StartAdminBatchOperationRequest{
			VisibilityQuery: "WorkflowType='MyWorkflow'",
		}
		adjustedQuery := a.adjustQueryAdminBatchType(adminReq)
		s.Equal("WorkflowType='MyWorkflow'", adjustedQuery)
	})
}

func (s *activitiesSuite) TestProcessAdminTask_RefreshWorkflowTasks() {
	ctx := context.Background()
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(s.controller)

	a := &activities{
		activityDeps: activityDeps{
			HistoryClient: mockHistoryClient,
		},
	}

	namespaceID := "test-namespace-id"
	workflowID := "test-workflow-id"
	runID := "test-run-id"

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: namespaceID,
		AdminRequest: &adminservice.StartAdminBatchOperationRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		},
	}

	testTask := task{
		executionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	}

	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100 }))

	// Expect RefreshWorkflowTasks to be called with correct parameters
	mockHistoryClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyservice.RefreshWorkflowTasksRequest, _ ...any) (*historyservice.RefreshWorkflowTasksResponse, error) {
			s.Equal(namespaceID, req.NamespaceId)
			s.NotZero(req.ArchetypeId) // WorkflowArchetypeID is computed dynamically
			s.Equal(workflowID, req.Request.Execution.WorkflowId)
			s.Equal(runID, req.Request.Execution.RunId)
			return &historyservice.RefreshWorkflowTasksResponse{}, nil
		})

	err := a.processAdminTask(ctx, batchOperation, testTask, limiter)
	s.NoError(err)
}

func (s *activitiesSuite) TestProcessAdminTask_RefreshWorkflowTasks_Error() {
	ctx := context.Background()
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(s.controller)

	a := &activities{
		activityDeps: activityDeps{
			HistoryClient: mockHistoryClient,
		},
	}

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "test-namespace-id",
		AdminRequest: &adminservice.StartAdminBatchOperationRequest{
			Namespace: "test-namespace",
			Identity:  "test-identity",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		},
	}

	testTask := task{
		executionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow-id",
				RunId:      "test-run-id",
			},
		},
	}

	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100 }))

	expectedErr := errors.New("refresh failed")
	// Use gomock.Any() for context since it's modified with CallerTypePreemptable header
	mockHistoryClient.EXPECT().RefreshWorkflowTasks(gomock.Any(), gomock.Any()).Return(nil, expectedErr)

	err := a.processAdminTask(ctx, batchOperation, testTask, limiter)
	s.Require().Error(err)
	s.Equal(expectedErr, err)
}

func (s *activitiesSuite) TestIsNonRetryableError() {
	tests := []struct {
		name      string
		err       error
		batchType enumspb.BatchOperationType
		want      bool
	}{
		{
			name:      "nil error returns false",
			err:       nil,
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
			want:      false,
		},
		{
			name:      "pinned version error for UPDATE_EXECUTION_OPTIONS returns true",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
			want:      true,
		},
		//nolint:staticcheck // SA1019: verifies batches started before the enum split
		{
			name:      "pinned version error for legacy UPDATE_EXECUTION_OPTIONS returns true",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "pinned version error with different format for UPDATE_EXECUTION_OPTIONS returns true",
			err:       errors.New("Pinned version 'prod:v2.0.1' is not present in task queue 'activity-queue' of type 'Activity'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "error containing substring for UPDATE_EXECUTION_OPTIONS returns true",
			err:       fmt.Errorf("Some prefix: %s suffix", "is not present in task queue"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "unrelated error for UPDATE_EXECUTION_OPTIONS returns false",
			err:       errors.New("some other error that doesn't match"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS,
			want:      false,
		},
		{
			name:      "pinned version error for different operation type returns false",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
			want:      false,
		},
		{
			name:      "pinned version error for SIGNAL operation returns false",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
			want:      false,
		},
		{
			// A completed activity never leaves its terminal state, so retrying
			// the cancel can never succeed.
			name:      "terminal state error for CANCEL_ACTIVITY returns true",
			err:       serviceerror.NewFailedPreconditionf("activity is in terminal state %v", "Completed"),
			batchType: enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
			want:      true,
		},
		{
			name:      "terminal state error for TERMINATE_ACTIVITY returns true",
			err:       serviceerror.NewFailedPrecondition("invalid transition"),
			batchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
			want:      true,
		},
		{
			// Delete gets no ExecutionStatus='Running' filter from
			// adjustQueryBatchTypeEnum, so it never hits the stale-visibility
			// case and keeps the default retry behavior.
			name:      "terminal state error for DELETE_ACTIVITY returns false",
			err:       serviceerror.NewFailedPreconditionf("activity is in terminal state %v", "Completed"),
			batchType: enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY,
			want:      false,
		},
		{
			// Wrapped errors must still be classified, since the task layer may
			// annotate before processTaskWithRetries sees the error.
			name:      "wrapped FailedPrecondition for CANCEL_ACTIVITY returns true",
			err:       fmt.Errorf("cancel activity: %w", serviceerror.NewFailedPrecondition("activity is in terminal state Canceled")),
			batchType: enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
			want:      true,
		},
		{
			// Transient failures on activity batches must stay retryable.
			name:      "unavailable error for CANCEL_ACTIVITY returns false",
			err:       serviceerror.NewUnavailable("history service is unavailable"),
			batchType: enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
			want:      false,
		},
		{
			name:      "generic error for CANCEL_ACTIVITY returns false",
			err:       errors.New("some transient error"),
			batchType: enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
			want:      false,
		},
		{
			// Workflow batch types must keep their existing behavior.
			name:      "FailedPrecondition for TERMINATE_WORKFLOW returns false",
			err:       serviceerror.NewFailedPrecondition("workflow is in terminal state"),
			batchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
			want:      false,
		},
		{
			// An InvalidArgument is permanent whatever the batch type: the same
			// arguments fail the same way on every attempt.
			name:      "InvalidArgument for RESET_WORKFLOW returns true",
			err:       serviceerror.NewInvalidArgument("Workflow task finish ID must be > 1 && <= workflow last event ID."),
			batchType: enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW,
			want:      true,
		},
		{
			name:      "InvalidArgument for SIGNAL_WORKFLOW returns true",
			err:       serviceerror.NewInvalidArgument("signal name is not set"),
			batchType: enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
			want:      true,
		},
		{
			name:      "InvalidArgument for UNPAUSE_ACTIVITY returns true",
			err:       serviceerror.NewInvalidArgument("activity type is not set"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UNPAUSE_ACTIVITY,
			want:      true,
		},
		{
			// The batch type carries no operation-specific rule at all, so this
			// covers the path that used to fall through to "retryable".
			name:      "InvalidArgument for DELETE_ACTIVITY returns true",
			err:       serviceerror.NewInvalidArgument("run id is not valid"),
			batchType: enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY,
			want:      true,
		},
		{
			// The task layer may annotate before processTaskWithRetries sees it.
			name:      "wrapped InvalidArgument returns true",
			err:       fmt.Errorf("reset workflow: %w", serviceerror.NewInvalidArgument("invalid reset point")),
			batchType: enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW,
			want:      true,
		},
		{
			// Only the typed error is permanent; the same wording from an
			// untyped error carries no such guarantee.
			name:      "untyped error mentioning invalid argument returns false",
			err:       errors.New("invalid argument"),
			batchType: enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW,
			want:      false,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			got := isNonRetryableError(tt.err, tt.batchType)
			s.Equal(tt.want, got)
		})
	}
}

// TestStartTaskProcessor_SignalUsesWorkerNamespace verifies that startTaskProcessor uses
// the worker's authoritative namespace (passed as the namespace argument) for operations,
// not the user-controlled namespace from batchOperation.Request.Namespace.
// This guards against a regression introduced in PR #8144 where batchParams.Request.Namespace
// (user-controlled) was used instead of a.namespace.String() (server-trusted).
func (s *activitiesSuite) TestStartTaskProcessor_SignalUsesWorkerNamespace() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	workerNamespace := "trusted-namespace"
	requestNamespace := "untrusted-namespace" // intentionally different

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: requestNamespace,
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{
					Signal: "test-signal",
				},
			},
		},
	}

	testPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
				},
			},
		},
	}
	testTask := task{
		executionInfo: testPage.executionInfos[0],
		attempts:      1,
		page:          testPage,
	}

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100 }))

	// The signal must be executed with the worker's trusted namespace, not the user-supplied one.
	s.mockFrontendClient.EXPECT().
		SignalWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.SignalWorkflowExecutionRequest, _ ...any) (*workflowservice.SignalWorkflowExecutionResponse, error) {
			s.Equal(workerNamespace, req.Namespace, "must use worker namespace, not request namespace")
			return &workflowservice.SignalWorkflowExecutionResponse{}, nil
		})

	taskCh <- testTask

	go a.startTaskProcessor(ctx, batchOperation, workerNamespace, taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	resp := <-respCh
	s.NoError(resp.err)
}

// TestStartTaskProcessor_SignalForwardsRequestFields verifies the signal path
// forwards every field the requester supplied, including the header that carries
// tracing and auth tokens through to the receiving workflow.
func (s *activitiesSuite) TestStartTaskProcessor_SignalForwardsRequestFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	input := payloads.EncodeString("signal-input")
	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"tracing-token": payload.EncodeString("trace-me"),
		},
	}
	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		BatchType:   enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: "untrusted-namespace",
			JobId:     "job-id",
			Reason:    "batch reason",
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{
					Signal:   "test-signal",
					Input:    input,
					Header:   header,
					Identity: "batch-signaler",
				},
			},
		},
	}

	testPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "test-workflow-id", RunId: "test-run-id"}},
		},
	}

	var captured *workflowservice.SignalWorkflowExecutionRequest
	s.mockFrontendClient.EXPECT().
		SignalWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.SignalWorkflowExecutionRequest, _ ...any) (*workflowservice.SignalWorkflowExecutionResponse, error) {
			captured = req
			return &workflowservice.SignalWorkflowExecutionResponse{}, nil
		})

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "trusted-namespace", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	taskCh <- task{executionInfo: testPage.executionInfos[0], attempts: 1, page: testPage}

	resp := <-respCh
	s.NoError(resp.err)

	s.Require().NotNil(captured)
	s.Equal("trusted-namespace", captured.Namespace, "must use worker namespace, not request namespace")
	protorequire.ProtoEqual(s.T(), testPage.executionInfos[0].Execution, captured.WorkflowExecution)
	s.Equal("test-signal", captured.SignalName)
	protorequire.ProtoEqual(s.T(), input, captured.Input)
	protorequire.ProtoEqual(s.T(), header, captured.Header)
	s.Equal("batch-signaler", captured.Identity)
	s.Equal(
		deterministicRequestID("job-id", "signal", "test-workflow-id", "test-run-id", "test-signal"),
		captured.RequestId,
	)
}

// TestStartTaskProcessor_RetryableErrorsDoNotDeadlock verifies that repeated retryable
// failures are retried in place and each task still yields exactly one response, so the
// worker pool keeps making progress.
func (s *activitiesSuite) TestStartTaskProcessor_RetryableErrorsDoNotDeadlock() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	const numTasks = 5
	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		// Bounded retries keep the test fast; each task is attempted a few times.
		AttemptsOnRetryableError: 2,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: "ns",
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{Signal: "test-signal"},
			},
		},
	}

	// Every signal fails with a retryable error, forcing the worker down the retry path.
	s.mockFrontendClient.EXPECT().
		SignalWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("transient error")).
		AnyTimes()

	// A single worker with a small buffer is the configuration that wedged when retries
	// were re-queued onto taskCh.
	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "ns", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	// Feed tasks from a separate goroutine so the test can drain responses concurrently.
	go func() {
		for i := range numTasks {
			p := &page{
				executionInfos: []*workflowpb.WorkflowExecutionInfo{
					{Execution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("wf-%d", i), RunId: "run"}},
				},
			}
			taskCh <- task{executionInfo: p.executionInfos[0], attempts: 1, page: p}
		}
	}()

	// Every task must produce exactly one error response; the activity must not deadlock.
	for range numTasks {
		select {
		case resp := <-respCh:
			s.Require().Error(resp.err)
		case <-time.After(10 * time.Second):
			s.FailNow("timed out waiting for task response: worker is deadlocked")
		}
	}
}

// TestStartTaskProcessor_ActivityTerminalStateIsNotRetried verifies that a
// FailedPrecondition from an activity cancel is attempted exactly once. A
// terminal activity never leaves its terminal state, so retrying can never
// succeed; because processTaskWithRetries retries in place with no backoff, a
// misclassification here spends all AttemptsOnRetryableError attempts hammering
// history within milliseconds.
func (s *activitiesSuite) TestStartTaskProcessor_ActivityTerminalStateIsNotRetried() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		BatchType:   enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY,
		// Generous retry budget: the point is that none of it gets used.
		AttemptsOnRetryableError: 50,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: "ns",
			Operation: &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
				CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{
					Identity: "batch-canceler",
					Reason:   "test",
				},
			},
		},
	}

	// The activity completed before the batch reached it -- exactly what a stale
	// ExecutionStatus='Running' visibility record produces.
	var calls atomic.Int32
	s.mockFrontendClient.EXPECT().
		RequestCancelActivityExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *workflowservice.RequestCancelActivityExecutionRequest, ...any) (*workflowservice.RequestCancelActivityExecutionResponse, error) {
			calls.Add(1)
			return nil, serviceerror.NewFailedPreconditionf("activity is in terminal state %v", "Completed")
		}).
		AnyTimes()

	testPage := &page{
		targetExecutionInfo: []*commonpb.Execution{
			{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: "activity-id", RunId: "run-id"},
		},
	}

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "ns", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	taskCh <- task{targetExecution: testPage.targetExecutionInfo[0], attempts: 1, page: testPage}

	select {
	case resp := <-respCh:
		s.Require().Error(resp.err)
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for task response")
	}

	s.Equal(int32(1), calls.Load(), "terminal-state failure must not be retried")
}

// TestStartTaskProcessor_InvalidArgumentIsNotRetried verifies that an
// InvalidArgument from a target's own operation is attempted exactly once. The
// same arguments fail the same way every time, and processTaskWithRetries retries
// in place paced only by the batch's rate limiter, so retrying spends the whole
// AttemptsOnRetryableError budget -- and the batch's request budget -- to arrive
// at the same failure. Unlike the activity FailedPrecondition case, this holds
// for every batch type.
func (s *activitiesSuite) TestStartTaskProcessor_InvalidArgumentIsNotRetried() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		BatchType:   enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW,
		// Generous retry budget: the point is that none of it gets used.
		AttemptsOnRetryableError: 50,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: "ns",
			JobId:     "job-id",
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{Signal: "test-signal"},
			},
		},
	}

	var calls atomic.Int32
	s.mockFrontendClient.EXPECT().
		SignalWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *workflowservice.SignalWorkflowExecutionRequest, ...any) (*workflowservice.SignalWorkflowExecutionResponse, error) {
			calls.Add(1)
			return nil, serviceerror.NewInvalidArgument("signal name is not set")
		}).
		AnyTimes()

	testPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"}},
		},
	}

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "ns", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	taskCh <- task{executionInfo: testPage.executionInfos[0], attempts: 1, page: testPage}

	select {
	case resp := <-respCh:
		s.Require().Error(resp.err)
	case <-time.After(10 * time.Second):
		s.FailNow("timed out waiting for task response")
	}

	s.Equal(int32(1), calls.Load(), "an InvalidArgument must not be retried")
}

// TestStartTaskProcessor_TerminationForwardsRequestFields verifies the terminate
// path calls the frontend with the batch job's reason plus the requester's
// identity and details, all against the worker's bound namespace. The SDK client
// this path used to go through substituted its own identity (pid@host) and had
// no way to send details at all.
func (s *activitiesSuite) TestStartTaskProcessor_TerminationForwardsRequestFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	details := payloads.EncodeString("why-it-was-terminated")
	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "some-namespace-id",
		BatchType:   enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		Request: &workflowservice.StartBatchOperationRequest{
			// Intentionally different from the worker's bound namespace below.
			Namespace: "untrusted-namespace",
			JobId:     "job-id",
			Reason:    "batch reason",
			Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
				TerminationOperation: &batchpb.BatchOperationTermination{
					Identity: "batch-terminator",
					Details:  details,
				},
			},
		},
	}

	testPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "test-workflow-id", RunId: "test-run-id"}},
		},
	}

	var captured *workflowservice.TerminateWorkflowExecutionRequest
	s.mockFrontendClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.TerminateWorkflowExecutionRequest, _ ...any) (*workflowservice.TerminateWorkflowExecutionResponse, error) {
			captured = req
			return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
		})

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "trusted-namespace", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	taskCh <- task{executionInfo: testPage.executionInfos[0], attempts: 1, page: testPage}

	resp := <-respCh
	s.NoError(resp.err)

	s.Require().NotNil(captured)
	s.Equal("trusted-namespace", captured.Namespace, "must use worker namespace, not request namespace")
	protorequire.ProtoEqual(s.T(), testPage.executionInfos[0].Execution, captured.WorkflowExecution)
	s.Equal("batch reason", captured.Reason)
	s.Equal("batch-terminator", captured.Identity)
	protorequire.ProtoEqual(s.T(), details, captured.Details)
}

// TestStartTaskProcessor_CancellationForwardsRequestFields verifies the cancel
// path forwards the requester's identity, the batch's reason, and a request ID
// derived from the job and target, so the server can de-dupe retries of the same
// cancel instead of recording a second cancel-requested event. The SDK client
// this path used to go through dropped identity and reason, and generated a
// fresh UUID on every call.
func (s *activitiesSuite) TestStartTaskProcessor_CancellationForwardsRequestFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := &activities{
		activityDeps: activityDeps{
			FrontendClient: s.mockFrontendClient,
			Logger:         log.NewTestLogger(),
			MetricsHandler: metrics.NoopMetricsHandler,
		},
	}

	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId:              "some-namespace-id",
		BatchType:                enumspb.BATCH_OPERATION_TYPE_CANCEL_WORKFLOW,
		AttemptsOnRetryableError: 2,
		Request: &workflowservice.StartBatchOperationRequest{
			// Intentionally different from the worker's bound namespace below.
			Namespace: "untrusted-namespace",
			JobId:     "job-id",
			Reason:    "batch reason",
			Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
				CancellationOperation: &batchpb.BatchOperationCancellation{
					Identity: "batch-canceler",
				},
			},
		},
	}

	// The first attempt fails transiently, so the task is retried in place: both
	// attempts must carry the same request ID.
	var mu sync.Mutex
	var requests []*workflowservice.RequestCancelWorkflowExecutionRequest
	s.mockFrontendClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.RequestCancelWorkflowExecutionRequest, _ ...any) (*workflowservice.RequestCancelWorkflowExecutionResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			requests = append(requests, req)
			if len(requests) == 1 {
				return nil, errors.New("transient error")
			}
			return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
		}).
		Times(3)

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 1000 }))

	go a.startTaskProcessor(ctx, batchOperation, "trusted-namespace", taskCh, respCh, limiter, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	firstPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"}},
		},
	}
	taskCh <- task{executionInfo: firstPage.executionInfos[0], attempts: 1, page: firstPage}
	s.NoError((<-respCh).err)

	// A different target in the same job must get its own request ID.
	secondPage := &page{
		executionInfos: []*workflowpb.WorkflowExecutionInfo{
			{Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-2", RunId: "run-2"}},
		},
	}
	taskCh <- task{executionInfo: secondPage.executionInfos[0], attempts: 1, page: secondPage}
	s.NoError((<-respCh).err)

	mu.Lock()
	defer mu.Unlock()
	s.Require().Len(requests, 3)
	for _, req := range requests {
		s.Equal("trusted-namespace", req.Namespace, "must use worker namespace, not request namespace")
		s.Equal("batch-canceler", req.Identity)
		s.Equal("batch reason", req.Reason)
		s.NotEmpty(req.RequestId)
	}
	protorequire.ProtoEqual(s.T(), firstPage.executionInfos[0].Execution, requests[0].WorkflowExecution)
	s.Equal(requests[0].RequestId, requests[1].RequestId, "a retry of the same cancel must reuse its request ID")
	s.NotEqual(requests[0].RequestId, requests[2].RequestId, "a different target must get a different request ID")
}

// TestRecordCompletedPages_ResumeTracksOldestIncompletePage verifies the resume point only
// advances across the contiguous run of completed pages, never past a page still in flight.
func (s *activitiesSuite) TestRecordCompletedPages_ResumeTracksOldestIncompletePage() {
	mkPage := func(num, size int, next string) *page {
		return &page{
			executionInfos: make([]*workflowpb.WorkflowExecutionInfo, size),
			nextPageToken:  []byte(next),
			pageNumber:     num,
		}
	}
	p1 := mkPage(0, 2, "tok-p2")
	p2 := mkPage(1, 2, "tok-p3")
	p3 := mkPage(2, 1, "")
	p1.next, p2.prev = p2, p1
	p2.next, p3.prev = p3, p2

	hbd := &HeartBeatDetails{PageToken: []byte("tok-p1")}

	// Page 3 finishes first, while pages 1 and 2 are still in flight.
	p3.successCount = 1
	recordCompletedPages(hbd, p3)
	// Resume point must NOT move: page 1 (the oldest) is still in flight.
	s.Equal([]byte("tok-p1"), hbd.PageToken, "must not advance past in-flight earlier pages")
	s.Equal(0, hbd.CurrentPage)
	s.Equal(0, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)

	// Page 1 finishes; page 2 still in flight.
	p1.successCount, p1.errorCount = 1, 1
	recordCompletedPages(hbd, p1)
	// Resume point advances to page 2 (now the oldest incomplete); only page 1 is counted.
	s.Equal([]byte("tok-p2"), hbd.PageToken)
	s.Equal(1, hbd.CurrentPage)
	s.Equal(1, hbd.SuccessCount)
	s.Equal(1, hbd.ErrorCount)

	// Page 2 finishes; the done prefix now extends through the already-complete page 3.
	p2.successCount = 2
	recordCompletedPages(hbd, p2)
	s.Empty(hbd.PageToken, "all pages done -> resume token is the last page's (empty) next token")
	s.Equal(3, hbd.CurrentPage)
	s.Equal(4, hbd.SuccessCount) // p1:1 + p2:2 + p3:1
	s.Equal(1, hbd.ErrorCount)   // p1:1
}

// TestProcessWorkflowsWithProactiveFetching_ProcessesAllPages drives the coordinator over
// several pages and checks every workflow is processed exactly once and the activity completes.
func (s *activitiesSuite) TestProcessWorkflowsWithProactiveFetching_ProcessesAllPages() {
	type pageSpec struct {
		size       int
		fetchToken string // NextPageToken used to fetch this page
		nextToken  string // NextPageToken this page returns
	}
	pages := []pageSpec{
		{size: 5, fetchToken: "", nextToken: "p2"},
		{size: 5, fetchToken: "p2", nextToken: "p3"},
		{size: 3, fetchToken: "p3", nextToken: ""},
	}
	const total = 13

	mockSdk := &mocks.Client{}
	for i, pg := range pages {
		execs := make([]*workflowpb.WorkflowExecutionInfo, pg.size)
		for j := range execs {
			execs[j] = &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("p%d-wf%d", i, j)},
			}
		}
		fetchToken := pg.fetchToken
		mockSdk.On("ListWorkflow", mock.Anything, mock.MatchedBy(func(r *workflowservice.ListWorkflowExecutionsRequest) bool {
			return string(r.NextPageToken) == fetchToken
		})).Return(&workflowservice.ListWorkflowExecutionsResponse{
			Executions:    execs,
			NextPageToken: []byte(pg.nextToken),
		}, nil).Once()
	}

	// Fake worker pool: drain taskCh and report success for every real task.
	var processed int64
	fakeWorker := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		_ quotas.RequestRateLimiter,
		_ workflowservice.WorkflowServiceClient,
		_ metrics.Handler,
		_ log.Logger,
	) {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-taskCh:
				if t.executionInfo == nil {
					continue
				}
				atomic.AddInt64(&processed, 1)
				select {
				case respCh <- taskResponse{err: nil, page: t.page}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	a := &activities{}
	config := batchProcessorConfig{
		adjustedQuery: "ExecutionStatus = 'Completed'",
		concurrency:   3,
	}
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 10000 }))

	// Run inside an activity environment so the coordinator's RecordHeartbeat call is valid.
	env := s.NewTestActivityEnvironment()
	runner := func(ctx context.Context) (HeartBeatDetails, error) {
		return a.processWorkflowsWithProactiveFetching(
			ctx, config, fakeWorker, limiter, mockSdk, metrics.NoopMetricsHandler, log.NewTestLogger(), HeartBeatDetails{},
		)
	}
	env.RegisterActivity(runner)

	encoded, err := env.ExecuteActivity(runner)
	s.NoError(err)

	var hbd HeartBeatDetails
	s.NoError(encoded.Get(&hbd))
	s.Equal(total, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(int64(total), atomic.LoadInt64(&processed))
	mockSdk.AssertExpectations(s.T())
}

func (s *activitiesSuite) TestProcessWorkflowsWithProactiveFetching_ProcessesAllActivityPages() {
	type pageSpec struct {
		size       int
		fetchToken string
		nextToken  string
	}
	pages := []pageSpec{
		{size: 5, nextToken: "p2"},
		{size: 5, fetchToken: "p2", nextToken: "p3"},
		{size: 3, fetchToken: "p3"},
	}
	const total = 13

	for i, pg := range pages {
		executions := make([]*activitypb.ActivityExecutionListInfo, pg.size)
		for j := range executions {
			executions[j] = &activitypb.ActivityExecutionListInfo{
				ActivityId: fmt.Sprintf("p%d-activity%d", i, j),
				RunId:      fmt.Sprintf("p%d-run%d", i, j),
			}
		}
		fetchToken := pg.fetchToken
		s.mockFrontendClient.EXPECT().ListActivityExecutions(gomock.Any(), gomock.Cond(func(r *workflowservice.ListActivityExecutionsRequest) bool {
			return r.GetNamespace() == "test-namespace" &&
				r.GetQuery() == "ActivityType = 'test-activity'" &&
				string(r.GetNextPageToken()) == fetchToken
		})).Return(&workflowservice.ListActivityExecutionsResponse{
			Executions:    executions,
			NextPageToken: []byte(pg.nextToken),
		}, nil)
	}

	mockSdk := &mocks.Client{}
	mockSdk.On("WorkflowService").Return(s.mockFrontendClient)

	var processed int64
	var invalidTargets int64
	fakeWorker := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		_ quotas.RequestRateLimiter,
		_ workflowservice.WorkflowServiceClient,
		_ metrics.Handler,
		_ log.Logger,
	) {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-taskCh:
				if task.targetExecution == nil ||
					task.targetExecution.GetType() != enumspb.EXECUTION_TYPE_ACTIVITY ||
					task.targetExecution.GetBusinessId() == "" || task.targetExecution.GetRunId() == "" {
					atomic.AddInt64(&invalidTargets, 1)
				}
				atomic.AddInt64(&processed, 1)
				select {
				case respCh <- taskResponse{page: task.page}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	a := &activities{}
	config := batchProcessorConfig{
		namespace:     "test-namespace",
		adjustedQuery: "ActivityType = 'test-activity'",
		batchType:     enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
		concurrency:   3,
	}
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 10000 }))

	env := s.NewTestActivityEnvironment()
	runner := func(ctx context.Context) (HeartBeatDetails, error) {
		return a.processWorkflowsWithProactiveFetching(
			ctx, config, fakeWorker, limiter, mockSdk, metrics.NoopMetricsHandler, log.NewTestLogger(), HeartBeatDetails{},
		)
	}
	env.RegisterActivity(runner)

	encoded, err := env.ExecuteActivity(runner)
	s.Require().NoError(err)

	var hbd HeartBeatDetails
	s.Require().NoError(encoded.Get(&hbd))
	s.Equal(total, hbd.SuccessCount)
	s.Equal(0, hbd.ErrorCount)
	s.Equal(int64(total), atomic.LoadInt64(&processed))
	s.Zero(atomic.LoadInt64(&invalidTargets))
	mockSdk.AssertExpectations(s.T())
}

// TestProcessWorkflowsWithProactiveFetching_InitialTargetExecutions verifies
// that config.initialTargetExecutions builds tasks with the field the batch
// type's processor actually reads: executionInfo for workflow batch types,
// targetExecution for activity batch types. Regression test for a panic
// where a workflow-targeted TargetExecutions batch always populated
// targetExecution (leaving executionInfo nil), causing a nil pointer
// dereference in the workflow-op processors (e.g. TerminationOperation).
func (s *activitiesSuite) TestProcessWorkflowsWithProactiveFetching_InitialTargetExecutions() {
	tests := []struct {
		name              string
		batchType         enumspb.BatchOperationType
		wantExecutionInfo bool
	}{
		{
			name:              "workflow batch type uses executionInfo",
			batchType:         enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
			wantExecutionInfo: true,
		},
		{
			name:              "activity batch type uses targetExecution",
			batchType:         enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
			wantExecutionInfo: false,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			targetExecutions := []*commonpb.Execution{
				{Type: enumspb.EXECUTION_TYPE_WORKFLOW, BusinessId: "wf-1", RunId: "run-1"},
			}

			var gotExecutionInfo, gotTargetExecution bool
			fakeWorker := func(
				ctx context.Context,
				taskCh chan task,
				respCh chan taskResponse,
				_ quotas.RequestRateLimiter,
				_ workflowservice.WorkflowServiceClient,
				_ metrics.Handler,
				_ log.Logger,
			) {
				for {
					select {
					case <-ctx.Done():
						return
					case t := <-taskCh:
						if t.executionInfo == nil && t.targetExecution == nil {
							continue
						}
						gotExecutionInfo = t.executionInfo != nil
						gotTargetExecution = t.targetExecution != nil
						select {
						case respCh <- taskResponse{err: nil, page: t.page}:
						case <-ctx.Done():
							return
						}
					}
				}
			}

			a := &activities{}
			config := batchProcessorConfig{
				batchType:               tt.batchType,
				concurrency:             1,
				initialTargetExecutions: targetExecutions,
			}
			limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 10000 }))

			env := s.NewTestActivityEnvironment()
			runner := func(ctx context.Context) (HeartBeatDetails, error) {
				return a.processWorkflowsWithProactiveFetching(
					ctx, config, fakeWorker, limiter, nil, metrics.NoopMetricsHandler, log.NewTestLogger(), HeartBeatDetails{},
				)
			}
			env.RegisterActivity(runner)

			encoded, err := env.ExecuteActivity(runner)
			s.NoError(err)

			var hbd HeartBeatDetails
			s.NoError(encoded.Get(&hbd))
			s.Equal(1, hbd.SuccessCount)
			s.Equal(tt.wantExecutionInfo, gotExecutionInfo)
			s.Equal(!tt.wantExecutionInfo, gotTargetExecution)
		})
	}
}

func (s *activitiesSuite) TestProcessWorkflowsWithProactiveFetching_LegacyActivityExecutions() {
	legacyExecutions := []*commonpb.WorkflowExecution{
		{WorkflowId: "activity-id", RunId: "activity-run-id"},
	}

	var gotExecutionInfo bool
	var gotTargetExecution *commonpb.Execution
	fakeWorker := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		_ quotas.RequestRateLimiter,
		_ workflowservice.WorkflowServiceClient,
		_ metrics.Handler,
		_ log.Logger,
	) {
		for {
			select {
			case <-ctx.Done():
				return
			case task := <-taskCh:
				if task.executionInfo == nil && task.targetExecution == nil {
					continue
				}
				gotExecutionInfo = task.executionInfo != nil
				gotTargetExecution = task.targetExecution
				select {
				case respCh <- taskResponse{page: task.page}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	a := &activities{}
	config := batchProcessorConfig{
		batchType:         enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY,
		concurrency:       1,
		initialExecutions: legacyExecutions,
	}
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 10000 }))

	env := s.NewTestActivityEnvironment()
	runner := func(ctx context.Context) (HeartBeatDetails, error) {
		return a.processWorkflowsWithProactiveFetching(
			ctx, config, fakeWorker, limiter, nil, metrics.NoopMetricsHandler, log.NewTestLogger(), HeartBeatDetails{},
		)
	}
	env.RegisterActivity(runner)

	encoded, err := env.ExecuteActivity(runner)
	s.Require().NoError(err)

	var hbd HeartBeatDetails
	s.Require().NoError(encoded.Get(&hbd))
	s.Equal(1, hbd.SuccessCount)
	s.False(gotExecutionInfo)
	s.Equal(&commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
		BusinessId: "activity-id",
		RunId:      "activity-run-id",
	}, gotTargetExecution)
}

func (s *activitiesSuite) TestProcessAdminTask_UnknownOperation() {
	ctx := context.Background()

	a := &activities{}

	// AdminRequest with nil operation
	batchOperation := &batchspb.BatchOperationInput{
		NamespaceId: "test-namespace-id",
		AdminRequest: &adminservice.StartAdminBatchOperationRequest{
			Namespace: "test-namespace",
		},
	}

	testTask := task{
		executionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow-id",
				RunId:      "test-run-id",
			},
		},
	}

	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 100 }))

	err := a.processAdminTask(ctx, batchOperation, testTask, limiter)
	s.Require().Error(err)
	s.Contains(err.Error(), "unknown admin batch type")
}

// TestDeterministicRequestID_ScopedToJob ensures idempotency within a batch job.
func (s *activitiesSuite) TestDeterministicRequestID_ScopedToJob() {
	const (
		jobA = "job-a"
		jobB = "job-b"
	)
	parts := []string{"signal", "workflow-id", "run-id", "signal-name"}

	s.NotEqual(deterministicRequestID(jobA, parts...), deterministicRequestID(jobB, parts...))
	s.NotEqual(deterministicRequestID(jobA, "signal", "workflow-id", "run-id", "other-signal"), deterministicRequestID(jobA, parts...))
}

func (s *activitiesSuite) TestHeartbeatInterval() {
	for _, tc := range []struct {
		name             string
		heartbeatTimeout time.Duration
		expected         time.Duration
	}{
		{
			name:             "unset assumes the default timeout",
			heartbeatTimeout: 0,
			expected:         defaultActivityHeartBeatTimeout / 4,
		},
		{
			name:             "negative assumes the default timeout",
			heartbeatTimeout: -time.Second,
			expected:         defaultActivityHeartBeatTimeout / 4,
		},
		{
			name:             "a shorter timeout heartbeats more often",
			heartbeatTimeout: time.Second,
			expected:         250 * time.Millisecond,
		},
		{
			name:             "a longer timeout heartbeats less often",
			heartbeatTimeout: 2 * time.Minute,
			expected:         30 * time.Second,
		},
	} {
		s.Run(tc.name, func() {
			interval := heartbeatInterval(tc.heartbeatTimeout)
			s.Equal(tc.expected, interval)
			if tc.heartbeatTimeout > 0 {
				s.Less(interval, tc.heartbeatTimeout,
					"the interval must stay under the timeout it was derived from")
			}
		})
	}
}

// TestProcessWorkflowsWithProactiveFetching_HeartbeatsWithinShortTimeout verifies
// the activity heartbeats on a cadence derived from the heartbeat timeout it was
// scheduled with. A cadence fixed to the default outlives a shorter timeout, so
// the activity would time out mid-batch while making progress.
func (s *activitiesSuite) TestProcessWorkflowsWithProactiveFetching_HeartbeatsWithinShortTimeout() {
	const heartbeatTimeout = 40 * time.Millisecond

	// The task takes many heartbeat intervals to process, so a correct cadence
	// heartbeats during it while the default cadence (2.5s) would not.
	fakeWorker := func(
		ctx context.Context,
		taskCh chan task,
		respCh chan taskResponse,
		_ quotas.RequestRateLimiter,
		_ workflowservice.WorkflowServiceClient,
		_ metrics.Handler,
		_ log.Logger,
	) {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-taskCh:
				select {
				case <-time.After(20 * heartbeatTimeout):
				case <-ctx.Done():
					return
				}
				select {
				case respCh <- taskResponse{page: t.page}:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	a := &activities{}
	config := batchProcessorConfig{
		batchType:        enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW,
		concurrency:      1,
		heartbeatTimeout: heartbeatTimeout,
		initialExecutions: []*commonpb.WorkflowExecution{
			{WorkflowId: "wf-1", RunId: "run-1"},
		},
	}
	limiter := quotas.NewRequestRateLimiterAdapter(quotas.NewDefaultOutgoingRateLimiter(func() float64 { return 10000 }))

	var heartbeats atomic.Int32
	env := s.NewTestActivityEnvironment()
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, _ converter.EncodedValues) {
		heartbeats.Add(1)
	})
	runner := func(ctx context.Context) (HeartBeatDetails, error) {
		return a.processWorkflowsWithProactiveFetching(
			ctx, config, fakeWorker, limiter, nil, metrics.NoopMetricsHandler, log.NewTestLogger(), HeartBeatDetails{},
		)
	}
	env.RegisterActivity(runner)

	encoded, err := env.ExecuteActivity(runner)
	s.Require().NoError(err)
	var hbd HeartBeatDetails
	s.Require().NoError(encoded.Get(&hbd))
	s.Equal(1, hbd.SuccessCount)

	s.Positive(heartbeats.Load(),
		"the activity must heartbeat while processing a task that outlasts its heartbeat timeout")
}
