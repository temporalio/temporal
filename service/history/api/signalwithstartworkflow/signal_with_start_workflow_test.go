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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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

		controller     *gomock.Controller
		shardContext   *historyi.MockShardContext
		metricsHandler *metricstest.CaptureHandler

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

	s.metricsHandler = metricstest.NewCaptureHandler()

	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(s.metricsHandler).AnyTimes()
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

// A retry handled by the current run resolves to that run, increments the deduplication metric, and
// leaves started false because this call did not create the run.
func (s *signalWithStartWorkflowSuite) TestDedupSignalWithStartRequest_Deduped() {
	ctx := context.Background()
	requestID := uuid.New().String()
	firstRunID := uuid.New().String()
	s.currentMutableState.EXPECT().IsSignalRequested(requestID).Return(true)
	s.currentMutableState.EXPECT().GetFirstRunID(ctx).Return(firstRunID, nil)
	s.currentContext.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID))

	capture := s.metricsHandler.StartCapture()
	defer s.metricsHandler.StopCapture(capture)

	outcome, err := dedupSignalWithStartRequest(ctx, s.shardContext, tests.GlobalNamespaceEntry, s.newCurrentWorkflowLease(), requestID)
	s.Require().NoError(err)
	s.Require().NotNil(outcome)
	s.Equal(s.currentRunID, outcome.runID)
	s.Equal(firstRunID, outcome.firstExecutionRunID)
	s.False(outcome.started)

	namespaceTag := metrics.NamespaceTag(tests.GlobalNamespaceEntry.Name().String())
	recordings := capture.Snapshot()[metrics.SignalWithStartWorkflowStartDeduped.Name()]
	s.Require().Len(recordings, 1)
	s.Equal(int64(1), recordings[0].Value)
	s.Equal(namespaceTag.Value, recordings[0].Tags[namespaceTag.Key])
}

// Deduplication uses only IsSignalRequested. ExecutionState.RequestIds never records SIGNALED events,
// so an ID found only there may belong to a plain StartWorkflowExecution and must not be used to
// deduplicate this signal.
func (s *signalWithStartWorkflowSuite) TestDedupSignalWithStartRequest_NotSignalRequested() {
	ctx := context.Background()
	requestID := uuid.New().String()
	s.currentMutableState.EXPECT().IsSignalRequested(requestID).Return(false)

	capture := s.metricsHandler.StartCapture()
	defer s.metricsHandler.StopCapture(capture)

	outcome, err := dedupSignalWithStartRequest(ctx, s.shardContext, tests.GlobalNamespaceEntry, s.newCurrentWorkflowLease(), requestID)
	s.Require().NoError(err)
	s.Nil(outcome)
	s.Empty(capture.Snapshot()[metrics.SignalWithStartWorkflowStartDeduped.Name()])
}

func (s *signalWithStartWorkflowSuite) TestDedupSignalWithStartRequest_GetFirstRunIDError() {
	ctx := context.Background()
	requestID := uuid.New().String()
	expectedErr := consts.ErrWorkflowClosing
	s.currentMutableState.EXPECT().IsSignalRequested(requestID).Return(true)
	s.currentMutableState.EXPECT().GetFirstRunID(ctx).Return("", expectedErr)

	capture := s.metricsHandler.StartCapture()
	defer s.metricsHandler.StopCapture(capture)

	outcome, err := dedupSignalWithStartRequest(ctx, s.shardContext, tests.GlobalNamespaceEntry, s.newCurrentWorkflowLease(), requestID)
	s.ErrorIs(err, expectedErr)
	s.Nil(outcome)
	s.Empty(capture.Snapshot()[metrics.SignalWithStartWorkflowStartDeduped.Name()])
}

func (s *signalWithStartWorkflowSuite) newCurrentWorkflowLease() api.WorkflowLease {
	return api.NewWorkflowLease(
		s.currentContext,
		wcache.NoopReleaseFn,
		s.currentMutableState,
	)
}

func (s *signalWithStartWorkflowSuite) randomRequest() *workflowservice.SignalWithStartWorkflowExecutionRequest {
	var request workflowservice.SignalWithStartWorkflowExecutionRequest
	_ = fakedata.FakeStruct(&request)
	return &request
}
