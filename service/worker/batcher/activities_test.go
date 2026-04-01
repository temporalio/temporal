package batcher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/common/testing/protomock"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Acceptance",
			query:          "A=B",
			expectedResult: fmt.Sprintf("(A=B) AND (%s)", statusRunningQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Acceptance with parenthesis",
			query:          "(A=B)",
			expectedResult: fmt.Sprintf("((A=B)) AND (%s)", statusRunningQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Acceptance with multiple conditions",
			query:          "(A=B) OR C=D",
			expectedResult: fmt.Sprintf("((A=B) OR C=D) AND (%s)", statusRunningQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Contains status - 1",
			query:          "ExecutionStatus=Completed",
			expectedResult: fmt.Sprintf("(ExecutionStatus=Completed) AND (%s)", statusRunningQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Contains status - 2",
			query:          "A=B OR ExecutionStatus='Completed'",
			expectedResult: fmt.Sprintf("(A=B OR ExecutionStatus='Completed') AND (%s)", statusRunningQueryFilter),
			batchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		},
		{
			name:           "Not supported batch type",
			query:          "A=B",
			expectedResult: "A=B",
			batchType:      enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED,
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
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      false,
		},
		{
			name:      "pinned version error for UPDATE_EXECUTION_OPTIONS returns true",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "pinned version error with different format for UPDATE_EXECUTION_OPTIONS returns true",
			err:       errors.New("Pinned version 'prod:v2.0.1' is not present in task queue 'activity-queue' of type 'Activity'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "error containing substring for UPDATE_EXECUTION_OPTIONS returns true",
			err:       fmt.Errorf("Some prefix: %s suffix", "is not present in task queue"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      true,
		},
		{
			name:      "unrelated error for UPDATE_EXECUTION_OPTIONS returns false",
			err:       errors.New("some other error that doesn't match"),
			batchType: enumspb.BATCH_OPERATION_TYPE_UPDATE_EXECUTION_OPTIONS,
			want:      false,
		},
		{
			name:      "pinned version error for different operation type returns false",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_TERMINATE,
			want:      false,
		},
		{
			name:      "pinned version error for SIGNAL operation returns false",
			err:       errors.New("Pinned version 'deployment-foo:build-123' is not present in task queue 'my-queue' of type 'Workflow'"),
			batchType: enumspb.BATCH_OPERATION_TYPE_SIGNAL,
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

	go a.startTaskProcessor(ctx, batchOperation, workerNamespace, taskCh, respCh, limiter, nil, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

	resp := <-respCh
	s.NoError(resp.err)
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

	go a.startTaskProcessor(ctx, batchOperation, "ns", taskCh, respCh, limiter, nil, s.mockFrontendClient, metrics.NoopMetricsHandler, log.NewTestLogger())

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
		_ sdkclient.Client,
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

func (s *activitiesSuite) TestUpdateHeartbeatCheckpoint() {
	firstToken := []byte("page-1")
	secondToken := []byte("page-2")
	thirdToken := []byte("page-3")

	tests := []struct {
		name           string
		hbd            HeartBeatDetails
		completedPage  *page
		expectedPage   int
		expectedToken  []byte
		expectedChange bool
	}{
		{
			name: "advance to prefetched page token",
			hbd:  HeartBeatDetails{},
			completedPage: &page{
				pageNumber:    0,
				nextPageToken: secondToken,
				next: &page{
					pageNumber: 1,
					pageToken:  firstToken,
				},
			},
			expectedPage:   1,
			expectedToken:  firstToken,
			expectedChange: true,
		},
		{
			name: "advance to unfetched next page",
			hbd:  HeartBeatDetails{},
			completedPage: &page{
				pageNumber:    2,
				nextPageToken: thirdToken,
			},
			expectedPage:   3,
			expectedToken:  thirdToken,
			expectedChange: true,
		},
		{
			name: "clear token on final page",
			hbd: HeartBeatDetails{
				CurrentPage: 2,
				PageToken:   secondToken,
			},
			completedPage: &page{
				pageNumber: 2,
			},
			expectedPage:   2,
			expectedToken:  nil,
			expectedChange: true,
		},
		{
			name: "no change when checkpoint unchanged",
			hbd: HeartBeatDetails{
				CurrentPage: 1,
				PageToken:   firstToken,
			},
			completedPage: &page{
				pageNumber:    0,
				nextPageToken: secondToken,
				next: &page{
					pageNumber: 1,
					pageToken:  firstToken,
				},
			},
			expectedPage:   1,
			expectedToken:  firstToken,
			expectedChange: false,
		},
	}

	for _, tc := range tests {
		s.Run(tc.name, func() {
			changed := updateHeartbeatCheckpoint(&tc.hbd, tc.completedPage)
			s.Equal(tc.expectedChange, changed)
			s.Equal(tc.expectedPage, tc.hbd.CurrentPage)
			s.Equal(tc.expectedToken, tc.hbd.PageToken)
		})
	}
}

// TestBatchActivityWithProtobuf_ResolvesRelativeTimestamps verifies that the activity resolves
// NOW()-based expressions in the visibility query using BatchStartTime as the anchor, then
// applies the batch-type-specific filter via adjustQueryBatchTypeEnum.
func (s *activitiesSuite) TestBatchActivityWithProtobuf_ResolvesRelativeTimestamps() {
	mockClientFactory := sdk.NewMockClientFactory(s.controller)
	mockSDKClient := mocksdk.NewMockClient(s.controller)
	a := &activities{
		activityDeps: activityDeps{
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         log.NewNoopLogger(),
			ClientFactory:  mockClientFactory,
			FrontendClient: s.mockFrontendClient,
		},
		namespace:   "test-namespace",
		namespaceID: "test-namespace-id",
		rps:         func(string) int { return 10 },
		concurrency: func(string) int { return 1 },
	}

	anchor := time.Date(2026, 3, 31, 17, 0, 0, 0, time.UTC)
	resolvedQuery := "StartTime > '2026-03-31T12:00:00Z'"
	expectedFinalQuery := fmt.Sprintf("(%s) AND (%s)", resolvedQuery, statusRunningQueryFilter)
	input := &batchspb.BatchOperationInput{
		NamespaceId:    "test-namespace-id",
		BatchType:      enumspb.BATCH_OPERATION_TYPE_TERMINATE,
		BatchStartTime: timestamppb.New(anchor),
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace:       "test-namespace",
			VisibilityQuery: "StartTime > NOW() - 5h",
		},
	}

	mockClientFactory.EXPECT().NewClient(gomock.Any()).Return(mockSDKClient)
	mockSDKClient.EXPECT().CountWorkflow(gomock.Any(), protomock.Eq(&workflowservice.CountWorkflowExecutionsRequest{
		Query: expectedFinalQuery,
	})).Return(&workflowservice.CountWorkflowExecutionsResponse{}, nil)
	mockSDKClient.EXPECT().ListWorkflow(gomock.Any(), protomock.Eq(&workflowservice.ListWorkflowExecutionsRequest{
		PageSize:      int32(pageSize),
		NextPageToken: nil,
		Query:         expectedFinalQuery,
	})).Return(&workflowservice.ListWorkflowExecutionsResponse{}, nil)

	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(a)

	f, err := env.ExecuteActivity(a.BatchActivityWithProtobuf, input)
	s.Require().NoError(err)
	var result HeartBeatDetails
	err = f.Get(&result)
	s.Require().NoError(err)
	s.Equal(int64(0), result.TotalEstimate)
}
