package signalwithstartworkflow

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	signalWithStartWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext *historyi.MockShardContext

		namespaceID string
		workflowID  string

		currentContext      *historyi.MockWorkflowContext
		currentMutableState *historyi.MockMutableState
		currentRunID        string
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

	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()

	s.currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: s.workflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.currentRunID,
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
	s.Error(consts.ErrWorkflowClosing, err)
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
		request.GetLinks(),
	).Return(&historypb.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.currentMutableState.EXPECT().HadOrHasWorkflowTask().Return(true)
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
