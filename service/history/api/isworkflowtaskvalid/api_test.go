package isworkflowtaskvalid

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

type (
	apiSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		workflowLease   api.WorkflowLease
		workflowContext *historyi.MockWorkflowContext
		mutableState    *historyi.MockMutableState
	}
)

func TestAPISuite(t *testing.T) {
	s := new(apiSuite)
	suite.Run(t, s)
}

func (s *apiSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.workflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.mutableState = historyi.NewMockMutableState(s.controller)
	s.workflowLease = api.NewWorkflowLease(
		s.workflowContext,
		func(err error) {},
		s.mutableState,
	)
}

func (s *apiSuite) TeardownTest() {
	s.controller.Finish()
}

func (s *apiSuite) TestWorkflowCompleted() {
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := isWorkflowTaskValid(s.workflowLease, rand.Int63(), 0)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *apiSuite) TestWorkflowRunning_WorkflowTaskNotStarted() {
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	s.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   common.EmptyEventID,
	})

	valid, err := isWorkflowTaskValid(s.workflowLease, workflowTaskScheduleEventID, 0)
	s.NoError(err)
	s.True(valid)
}

func (s *apiSuite) TestWorkflowRunning_WorkflowTaskStarted() {
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	s.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   workflowTaskScheduleEventID + 10,
	})

	valid, err := isWorkflowTaskValid(s.workflowLease, workflowTaskScheduleEventID, 0)
	s.NoError(err)
	s.False(valid)
}

func (s *apiSuite) TestWorkflowRunning_WorkflowTaskMissing() {
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	s.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(nil)

	valid, err := isWorkflowTaskValid(s.workflowLease, workflowTaskScheduleEventID, 0)
	s.NoError(err)
	s.False(valid)
}

func (s *apiSuite) TestWorkflowRunning_WorkflowTask_StampInvalid() {
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	workflowTaskScheduleEventID := rand.Int63()
	s.mutableState.EXPECT().GetWorkflowTaskByID(workflowTaskScheduleEventID).Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: workflowTaskScheduleEventID,
		StartedEventID:   common.EmptyEventID,
		Stamp:            1,
	})

	valid, err := isWorkflowTaskValid(s.workflowLease, workflowTaskScheduleEventID, 0)
	s.NoError(err)
	s.False(valid)
}
