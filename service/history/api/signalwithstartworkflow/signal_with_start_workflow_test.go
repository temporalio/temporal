package signalwithstartworkflow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	signalWithStartWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext *historyi.MockShardContext

		namespaceID string
		workflowID  string

		currentContext       *historyi.MockWorkflowContext
		currentMutableState  *historyi.MockMutableState
		currentExecutionInfo *persistencespb.WorkflowExecutionInfo
		currentRunID         string
		timeSource           *clock.EventTimeSource
		metricsHandler       *metricstest.CaptureHandler
	}
)

func TestSignalWithStartWorkflowSuite(t *testing.T) {
	s := new(signalWithStartWorkflowSuite)
	suite.Run(t, s)
}

func (s *signalWithStartWorkflowSuite) SetupSuite() {
}

func (s *signalWithStartWorkflowSuite) TearDownSuite() {
}

func (s *signalWithStartWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.shardContext = historyi.NewMockShardContext(s.controller)

	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()

	s.currentContext = historyi.NewMockWorkflowContext(s.controller)
	s.currentMutableState = historyi.NewMockMutableState(s.controller)
	s.currentRunID = uuid.New().String()
	s.timeSource = clock.NewEventTimeSource()
	s.metricsHandler = metricstest.NewCaptureHandler()
	s.currentExecutionInfo = &persistencespb.WorkflowExecutionInfo{
		WorkflowId:    s.workflowID,
		ExecutionTime: timestamppb.New(s.timeSource.Now()),
	}

	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(s.metricsHandler).AnyTimes()
	s.shardContext.EXPECT().GetTimeSource().Return(s.timeSource).AnyTimes()

	s.currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(s.currentExecutionInfo).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()
	s.currentContext.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.currentRunID,
	}).AnyTimes()
}

func (s *signalWithStartWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_WorkflowCloseAttempted() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(true)
	s.currentMutableState.EXPECT().HasStartedWorkflowTask().Return(true)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.ErrorIs(consts.ErrWorkflowClosing, err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_Dedup() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(true)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_NewWorkflowTask() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL).Return(&historyi.WorkflowTaskInfo{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_WorkflowTaskAtExecutionTime() {
	s.assertWorkflowTaskScheduledAtExecutionTime(s.timeSource.Now())
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_WorkflowTaskAfterExecutionTime() {
	s.assertWorkflowTaskScheduledAtExecutionTime(s.timeSource.Now().Add(-time.Second))
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_WorkflowTaskBeforeExecutionTime() {
	s.assertWorkflowTaskScheduledAtExecutionTime(s.timeSource.Now().Add(time.Hour))
}

func (s *signalWithStartWorkflowSuite) assertWorkflowTaskScheduledAtExecutionTime(executionTime time.Time) {
	s.currentExecutionInfo.ExecutionTime = timestamppb.New(executionTime)

	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	if executionTime.After(s.timeSource.Now()) {
		s.currentMutableState.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
		s.currentMutableState.EXPECT().GetStartEvent(ctx).Return(signalWorkflowStartEvent(enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED, ""), nil)
	}
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL).Return(&historyi.WorkflowTaskInfo{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_ContinuedAsNewWorkflowTaskBackoff() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()
	s.currentExecutionInfo.ExecutionTime = timestamppb.New(s.timeSource.Now().Add(time.Hour))

	s.expectSignalWorkflowEvent(request)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	s.currentMutableState.EXPECT().GetStartEvent(ctx).Return(signalWorkflowStartEvent(enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW, uuid.NewString()), nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_RetryInitiatorDuringBackoff() {
	s.assertWorkflowTaskScheduledDuringBackoff(enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_CronInitiatorDuringBackoff() {
	s.assertWorkflowTaskScheduledDuringBackoff(enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_OverdueWorkflowTaskBackoff() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()
	s.currentExecutionInfo.ExecutionTime = timestamppb.New(s.timeSource.Now().Add(-time.Second))

	s.expectSignalWorkflowEvent(request)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL).Return(&historyi.WorkflowTaskInfo{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) assertWorkflowTaskScheduledDuringBackoff(initiator enumspb.ContinueAsNewInitiator) {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()
	s.currentExecutionInfo.ExecutionTime = timestamppb.New(s.timeSource.Now().Add(time.Hour))

	s.expectSignalWorkflowEvent(request)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowPendingOnWorkflowTaskBackoff().Return(true)
	s.currentMutableState.EXPECT().GetStartEvent(ctx).Return(signalWorkflowStartEvent(initiator, uuid.NewString()), nil)
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL).Return(&historyi.WorkflowTaskInfo{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) expectSignalWorkflowEvent(request *workflowservice.SignalWithStartWorkflowExecutionRequest) {
	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_NoNewWorkflowTask() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(true)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

// Tests SignalWithStart when the workflow is paused.
// Asserts that no new workflow task is scheduled.
func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_WhenPaused() {
	ctx := context.Background()
	currentWorkflowLease := api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(false)
	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
		request.GetRequestId(),
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(true)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, s.shardContext).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowLease,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) randomRequest() *workflowservice.SignalWithStartWorkflowExecutionRequest {
	var request workflowservice.SignalWithStartWorkflowExecutionRequest
	_ = fakedata.FakeStruct(&request)
	return &request
}

func signalWorkflowStartEvent(
	initiator enumspb.ContinueAsNewInitiator,
	continuedExecutionRunID string,
) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Initiator:               initiator,
				ContinuedExecutionRunId: continuedExecutionRunID,
			},
		},
	}
}
