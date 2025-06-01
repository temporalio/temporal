package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	workflowStateStatusSuite struct {
		suite.Suite
		*require.Assertions
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
	s.Assertions = require.New(s.T())
}

func (s *workflowStateStatusSuite) TearDownTest() {

}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.NotNil(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.Error(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.NoError(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, status))
	}
}

func (s *workflowStateStatusSuite) TestCreateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.Error(ValidateCreateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCreated() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.Error(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateRunning() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.Error(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateCompleted() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.Error(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.NoError(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, status))
	}
}

func (s *workflowStateStatusSuite) TestUpdateWorkflowStateStatus_WorkflowStateZombie() {
	statuses := []enumspb.WorkflowExecutionStatus{
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
	}

	s.NoError(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))

	for _, status := range statuses {
		s.Error(ValidateUpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, status))
	}
}
