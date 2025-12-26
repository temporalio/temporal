package deployment

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/mock/gomock"
)

type deploymentSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment
}

func TestDeploymentSuite(t *testing.T) {
	suite.Run(t, new(deploymentSuite))
}

func (s *deploymentSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(DeploymentWorkflow)
}

func (s *deploymentSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

// TestRegisterTaskQueueInDeployment tests the case when a task-queue
// is registered in a deployment
// func (s *deploymentSuite) TestRegisterTaskQueueInDeployment() {
// )
// }

// TestRegisterTaskQueuesInDeployment tests the case when multiple task-queues
// are registered (non-concurrently) in a deployment
// func (s *deploymentSuite) TestRegisterTaskQueuesInDeployment() {
// )
// }

// TestRegisterTaskQueuesInDeploymentConcurrent tests the case when multiple task-queues
// are registered concurrently in a deployment
// func (s *deploymentSuite) TestRegisterTaskQueuesInDeploymentConcurrent() {
// )
// }

// TestRegisterTaskQueuesExceedLimit tests the case when the number of registered task-queues
// exceed the allowed per-deployment limit
// func (s *deploymentSuite) TestRegisterTaskQueuesExceedLimit() {
// )
// }

// TestStartDeploymentWorkflowExceedLimit tests the case when the number of
// deployment workflow executions exceed the allowed namespace limit
// func (s *deploymentSuite) TestRegisterTaskQueuesExceedLimit() {
// )
// }
