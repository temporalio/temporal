package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/temporal-proto/common"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
)

type (
	statsComputerSuite struct {
		sc *statsComputer
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestStatsComputerSuite(t *testing.T) {
	s := new(statsComputerSuite)
	suite.Run(t, s)
}

// TODO need to add more tests
func (s *statsComputerSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.sc = &statsComputer{}
}

func (s *statsComputerSuite) createRequest() *InternalUpdateWorkflowExecutionRequest {
	return &InternalUpdateWorkflowExecutionRequest{
		UpdateWorkflowMutation: InternalWorkflowMutation{
			ExecutionInfo: &InternalWorkflowExecutionInfo{},
		},
	}
}

func (s *statsComputerSuite) TestStatsWithStartedEvent() {
	ms := s.createRequest()
	namespaceID := "A"
	execution := executionpb.WorkflowExecution{
		WorkflowId: "test-workflow-id",
		RunId:      "run_id",
	}
	workflowType := &commonpb.WorkflowType{
		Name: "test-workflow-type-name",
	}
	taskList := &tasklistpb.TaskList{
		Name: "test-tasklist",
	}

	ms.UpdateWorkflowMutation.ExecutionInfo.NamespaceID = namespaceID
	ms.UpdateWorkflowMutation.ExecutionInfo.WorkflowID = execution.GetWorkflowId()
	ms.UpdateWorkflowMutation.ExecutionInfo.RunID = execution.GetRunId()
	ms.UpdateWorkflowMutation.ExecutionInfo.WorkflowTypeName = workflowType.GetName()
	ms.UpdateWorkflowMutation.ExecutionInfo.TaskList = taskList.GetName()

	expectedSize := len(execution.GetWorkflowId()) + len(workflowType.GetName()) + len(taskList.GetName())

	stats := s.sc.computeMutableStateUpdateStats(ms)
	s.Equal(stats.ExecutionInfoSize, expectedSize)
}
