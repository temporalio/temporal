package persistence

import (
	"testing"

	"github.com/stretchr/testify/suite"
	executionpb "go.temporal.io/temporal-proto/execution"
)

type (
	workflowStateStatusSuite struct {
		suite.Suite
	}
)

func TestWorkflowStateStatusSuite(t *testing.T) {
	s := new(workflowStateStatusSuite)
	suite.Run(t, s)
}

func (s *workflowStateStatusSuite) SetupSuite() {
}

func (s *workflowStateStatusSuite) TearDownSuite() {

}

func (s *workflowStateStatusSuite) SetupTest() {

}

func (s *workflowStateStatusSuite) TearDownTest() {

}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCreated, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateRunning, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Running,
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateCompleted, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(WorkflowStateZombie, status))
	}
}

// TODO

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCreated, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateRunning, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateCompleted, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []executionpb.WorkflowExecutionStatus{
		executionpb.WorkflowExecutionStatus_Completed,
		executionpb.WorkflowExecutionStatus_Failed,
		executionpb.WorkflowExecutionStatus_Canceled,
		executionpb.WorkflowExecutionStatus_Terminated,
		executionpb.WorkflowExecutionStatus_ContinuedAsNew,
		executionpb.WorkflowExecutionStatus_TimedOut,
	}

	s.Nil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, executionpb.WorkflowExecutionStatus_Running))

	for _, status := range statuses {
		s.NotNil(ValidateUpdateWorkflowStateStatus(WorkflowStateZombie, status))
	}
}
