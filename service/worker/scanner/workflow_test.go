package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal/activity"
	"go.temporal.io/temporal/workflow"
	"go.uber.org/zap"

	"go.temporal.io/temporal/testsuite"
	"go.temporal.io/temporal/worker"

	"github.com/temporalio/temporal/common/metrics"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
)

type scannerWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestScannerWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(scannerWorkflowTestSuite))
}

func (s *scannerWorkflowTestSuite) registerWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflowWithOptions(TaskListScannerWorkflow, workflow.RegisterOptions{Name: tlScannerWFTypeName})
	env.RegisterWorkflowWithOptions(HistoryScannerWorkflow, workflow.RegisterOptions{Name: historyScannerWFTypeName})
	env.RegisterActivityWithOptions(TaskListScavengerActivity, activity.RegisterOptions{Name: taskListScavengerActivityName})
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
}

func (s *scannerWorkflowTestSuite) registerActivities(env *testsuite.TestActivityEnvironment) {
	env.RegisterActivityWithOptions(TaskListScavengerActivity, activity.RegisterOptions{Name: taskListScavengerActivityName})
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
}

func (s *scannerWorkflowTestSuite) TestWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(taskListScavengerActivityName, mock.Anything).Return(nil)
	env.ExecuteWorkflow(tlScannerWFTypeName)
	s.True(env.IsWorkflowCompleted())
}

func (s *scannerWorkflowTestSuite) TestScavengerActivity() {
	env := s.NewTestActivityEnvironment()
	s.registerActivities(env)
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockResource := resource.NewTest(controller, metrics.Worker)
	defer mockResource.Finish(s.T())

	mockResource.TaskMgr.On("ListTaskList", mock.Anything).Return(&p.ListTaskListResponse{}, nil)
	ctx := scannerContext{
		Resource:  mockResource,
		zapLogger: zap.NewNop(),
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), scannerContextKey, ctx),
	})
	tlScavengerHBInterval = time.Millisecond * 10
	_, err := env.ExecuteActivity(taskListScavengerActivityName)
	s.NoError(err)
}
