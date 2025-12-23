package deployment

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/mock/gomock"
)

type deploymentSeriesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment
}

func TestDeploymentSeriesSuite(t *testing.T) {
	suite.Run(t, new(deploymentSeriesSuite))
}

func (s *deploymentSeriesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(DeploymentSeriesWorkflow)
}

func (s *deploymentSeriesSuite) TearDownTest() {
	s.controller.Finish()
	s.env.AssertExpectations(s.T())
}

/*
func (d *deploymentSeriesSuite) TestStartDeploymentSeriesWorkflow() {}





*/
