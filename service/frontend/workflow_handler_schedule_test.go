package frontend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// scheduleStartHistory is the part of history StartWorkflowExecution that matters
// here: a repeated request ID on the same workflow ID returns the existing run,
// even when the new start asks for a different workflow type.
type scheduleStartHistory struct {
	executions map[string]*scheduleWorkflow
}

type scheduleWorkflow struct {
	requestIDs map[string]struct{}
	wfType     string
	runID      string
	status     enumspb.WorkflowExecutionStatus
}

func newScheduleStartHistory() *scheduleStartHistory {
	return &scheduleStartHistory{executions: make(map[string]*scheduleWorkflow)}
}

func (h *scheduleStartHistory) start(
	_ context.Context,
	request *historyservice.StartWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	start := request.GetStartRequest()
	workflowID := start.GetWorkflowId()
	current := h.executions[workflowID]
	if current != nil {
		if _, reused := current.requestIDs[start.GetRequestId()]; reused {
			return &historyservice.StartWorkflowExecutionResponse{
				RunId:   current.runID,
				Started: true,
				Status:  current.status,
			}, nil
		}
		if current.status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
				"workflow execution already started",
				start.GetRequestId(),
				current.runID,
			)
		}
	}

	runID := uuid.NewString()
	h.executions[workflowID] = &scheduleWorkflow{
		requestIDs: map[string]struct{}{start.GetRequestId(): {}},
		wfType:     start.GetWorkflowType().GetName(),
		runID:      runID,
		status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId:   runID,
		Started: true,
		Status:  enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}, nil
}

func (h *scheduleStartHistory) describe(
	_ context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	workflowID := request.GetRequest().GetExecution().GetWorkflowId()
	current := h.executions[workflowID]
	if current == nil {
		return nil, serviceerror.NewNotFound("workflow not found")
	}
	return &historyservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: current.runID},
			Type:      &commonpb.WorkflowType{Name: current.wfType},
			Status:    current.status,
		},
	}, nil
}

// recordingSchedulerClient counts CHASM creates. A cross-stack retry must not add one.
type recordingSchedulerClient struct {
	creates int
}

func (c *recordingSchedulerClient) CreateSchedule(
	context.Context,
	*schedulerpb.CreateScheduleRequest,
	...grpc.CallOption,
) (*schedulerpb.CreateScheduleResponse, error) {
	c.creates++
	return &schedulerpb.CreateScheduleResponse{
		FrontendResponse: &workflowservice.CreateScheduleResponse{},
	}, nil
}

func (c *recordingSchedulerClient) CreateSentinel(
	context.Context,
	*schedulerpb.CreateSentinelRequest,
	...grpc.CallOption,
) (*schedulerpb.CreateSentinelResponse, error) {
	return &schedulerpb.CreateSentinelResponse{}, nil
}

func (c *recordingSchedulerClient) UpdateSchedule(context.Context, *schedulerpb.UpdateScheduleRequest, ...grpc.CallOption) (*schedulerpb.UpdateScheduleResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) PatchSchedule(context.Context, *schedulerpb.PatchScheduleRequest, ...grpc.CallOption) (*schedulerpb.PatchScheduleResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) DeleteSchedule(context.Context, *schedulerpb.DeleteScheduleRequest, ...grpc.CallOption) (*schedulerpb.DeleteScheduleResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) DescribeSchedule(context.Context, *schedulerpb.DescribeScheduleRequest, ...grpc.CallOption) (*schedulerpb.DescribeScheduleResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) ListScheduleMatchingTimes(context.Context, *schedulerpb.ListScheduleMatchingTimesRequest, ...grpc.CallOption) (*schedulerpb.ListScheduleMatchingTimesResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) CreateFromMigrationState(context.Context, *schedulerpb.CreateFromMigrationStateRequest, ...grpc.CallOption) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	panic("not implemented")
}
func (c *recordingSchedulerClient) MigrateToWorkflow(context.Context, *schedulerpb.MigrateToWorkflowRequest, ...grpc.CallOption) (*schedulerpb.MigrateToWorkflowResponse, error) {
	panic("not implemented")
}

func (s *WorkflowHandlerSuite) newScheduleHandlers(history *scheduleStartHistory) (v1 *WorkflowHandler, chasm *WorkflowHandler, recorder *recordingSchedulerClient) {
	s.mockNamespaceCache.EXPECT().GetNamespaceID(gomock.Any()).Return(s.testNamespaceID, nil).AnyTimes()
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockSearchAttributesProvider.EXPECT().
		GetSearchAttributes(gomock.Any(), false).
		Return(searchattribute.TestNameTypeMap(), nil).
		AnyTimes()
	s.mockVisibilityMgr.EXPECT().ValidateCustomSearchAttributes(gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(history.start).AnyTimes()
	s.mockHistoryClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(history.describe).AnyTimes()

	recorder = &recordingSchedulerClient{}
	newHandler := func(chasmCreate bool) *WorkflowHandler {
		config := s.newConfig()
		config.EnableSchedules = dc.GetBoolPropertyFnFilteredByNamespace(true)
		config.EnableChasm = dc.GetBoolPropertyFnFilteredByNamespace(true)
		config.EnableCHASMSchedulerSentinels = dc.GetBoolPropertyFnFilteredByNamespace(true)
		config.EnableCHASMSchedulerCreation = dc.GetBoolPropertyFnFilteredByNamespace(chasmCreate)
		config.CHASMSchedulerCreationRolloutPercent = dc.GetIntPropertyFnFilteredByNamespace(100)
		handler := s.getWorkflowHandler(config)
		handler.schedulerClient = recorder
		return handler
	}
	return newHandler(false), newHandler(true), recorder
}

func scheduleCreateRequest(namespaceName namespace.Name, scheduleID, requestID string) *workflowservice.CreateScheduleRequest {
	return &workflowservice.CreateScheduleRequest{
		Namespace:  namespaceName.String(),
		ScheduleId: scheduleID,
		RequestId:  requestID,
		Identity:   "test",
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "workflow-id",
						WorkflowType: &commonpb.WorkflowType{Name: "workflow-type"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: "task-queue"},
					},
				},
			},
		},
	}
}

func TestScheduleSentinelRequestID(t *testing.T) {
	t.Parallel()

	id := scheduleSentinelRequestID("caller", 1000)
	require.NotEqual(t, "caller", id)
	require.Equal(t, id, scheduleSentinelRequestID("caller", 1000))
	require.LessOrEqual(t, len(id), 1000)

	maxed := strings.Repeat("r", 1000)
	sentinel := scheduleSentinelRequestID(maxed, 1000)
	require.NotEqual(t, maxed, sentinel)
	require.LessOrEqual(t, len(sentinel), 1000)

	marked := strings.Repeat("a", 991) + scheduleSentinelRequestIDMark
	require.Len(t, marked, 1000)
	sentinel = scheduleSentinelRequestID(marked, 1000)
	require.NotEqual(t, marked, sentinel)
	require.LessOrEqual(t, len(sentinel), 1000)
}

func (s *WorkflowHandlerSuite) TestCreateSchedule_CHASMThenWorkflowSameRequestID() {
	history := newScheduleStartHistory()
	v1, chasmHandler, _ := s.newScheduleHandlers(history)
	requestID := uuid.NewString()
	scheduleID := "sched-chasm-then-v1"

	_, err := chasmHandler.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	s.NoError(err)

	resp, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Nil(resp)

	current := history.executions[scheduler.WorkflowIDPrefix+scheduleID]
	s.Equal(dummy.DummyWFTypeName, current.wfType)
	_, reusedCallerID := current.requestIDs[requestID]
	s.False(reusedCallerID)
}

func (s *WorkflowHandlerSuite) TestCreateSchedule_WorkflowThenCHASMSameRequestID() {
	history := newScheduleStartHistory()
	v1, chasmHandler, recorder := s.newScheduleHandlers(history)
	requestID := uuid.NewString()
	scheduleID := "sched-v1-then-chasm"

	_, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	s.NoError(err)
	s.Equal(0, recorder.creates)

	resp, err := chasmHandler.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Nil(resp)
	s.Equal(0, recorder.creates)
	s.Equal(scheduler.WorkflowType, history.executions[scheduler.WorkflowIDPrefix+scheduleID].wfType)
}

func (s *WorkflowHandlerSuite) TestCreateSchedule_WorkflowRetrySameRequestID() {
	history := newScheduleStartHistory()
	v1, _, recorder := s.newScheduleHandlers(history)
	requestID := uuid.NewString()
	scheduleID := "sched-v1-retry"

	first, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	s.NoError(err)
	s.NotNil(first)

	second, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	s.NoError(err)
	s.NotNil(second)
	s.Equal(0, recorder.creates)
	s.Equal(scheduler.WorkflowType, history.executions[scheduler.WorkflowIDPrefix+scheduleID].wfType)
}

func (s *WorkflowHandlerSuite) TestCreateSchedule_DedupedSentinelIsNotASchedule() {
	history := newScheduleStartHistory()
	v1, chasmHandler, recorder := s.newScheduleHandlers(history)
	requestID := uuid.NewString()
	scheduleID := "sched-existing-sentinel"
	workflowID := scheduler.WorkflowIDPrefix + scheduleID
	history.executions[workflowID] = &scheduleWorkflow{
		requestIDs: map[string]struct{}{requestID: {}},
		wfType:     dummy.DummyWFTypeName,
		runID:      uuid.NewString(),
		status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	resp, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Nil(resp)
	s.Equal(dummy.DummyWFTypeName, history.executions[workflowID].wfType)

	realID := "sched-existing-real"
	realWorkflowID := scheduler.WorkflowIDPrefix + realID
	history.executions[realWorkflowID] = &scheduleWorkflow{
		requestIDs: map[string]struct{}{requestID: {}},
		wfType:     scheduler.WorkflowType,
		runID:      uuid.NewString(),
		status:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	resp, err = chasmHandler.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, realID, requestID))
	s.ErrorAs(err, &alreadyStarted)
	s.Nil(resp)
	s.Equal(0, recorder.creates)
}

func (s *WorkflowHandlerSuite) TestCreateSchedule_MaxLengthRequestIDDoesNotCollide() {
	history := newScheduleStartHistory()
	v1, chasmHandler, _ := s.newScheduleHandlers(history)
	requestID := strings.Repeat("r", s.newConfig().MaxIDLengthLimit())
	scheduleID := "sched-max-request-id"

	_, err := chasmHandler.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	s.NoError(err)

	resp, err := v1.CreateSchedule(context.Background(), scheduleCreateRequest(s.testNamespace, scheduleID, requestID))
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Nil(resp)
	s.Equal(dummy.DummyWFTypeName, history.executions[scheduler.WorkflowIDPrefix+scheduleID].wfType)
}
