package scanner

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/mock/gomock"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
)

type scannerWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestScannerWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(scannerWorkflowTestSuite))
}

func (s *scannerWorkflowTestSuite) registerWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflowWithOptions(TaskQueueScannerWorkflow, workflow.RegisterOptions{Name: tqScannerWFTypeName})
	env.RegisterWorkflowWithOptions(HistoryScannerWorkflow, workflow.RegisterOptions{Name: historyScannerWFTypeName})
	env.RegisterActivityWithOptions(TaskQueueScavengerActivity, activity.RegisterOptions{Name: taskQueueScavengerActivityName})
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
}

func (s *scannerWorkflowTestSuite) registerActivities(env *testsuite.TestActivityEnvironment) {
	env.RegisterActivityWithOptions(TaskQueueScavengerActivity, activity.RegisterOptions{Name: taskQueueScavengerActivityName})
	env.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
}

func (s *scannerWorkflowTestSuite) TestWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(taskQueueScavengerActivityName, mock.Anything).Return(nil)
	env.ExecuteWorkflow(tqScannerWFTypeName)
	s.True(env.IsWorkflowCompleted())
}

func (s *scannerWorkflowTestSuite) TestScavengerActivity() {
	env := s.NewTestActivityEnvironment()
	s.registerActivities(env)
	controller := gomock.NewController(s.T())
	defer controller.Finish()
	mockResource := resourcetest.NewTest(controller, primitives.WorkerService)

	mockResource.TaskMgr.EXPECT().ListTaskQueue(gomock.Any(), gomock.Any()).Return(&p.ListTaskQueueResponse{}, nil)

	ctx := scannerContext{
		logger:           mockResource.GetLogger(),
		metricsHandler:   mockResource.GetMetricsHandler(),
		executionManager: mockResource.GetExecutionManager(),
		taskManager:      mockResource.GetTaskManager(),
		historyClient:    mockResource.GetHistoryClient(),
		hostInfo:         mockResource.GetHostInfo(),
		cfg: &Config{
			TaskQueueScannerPerHostQPS: dynamicconfig.GetIntPropertyFn(10),
			PersistenceMaxQPS:          dynamicconfig.GetIntPropertyFn(10),
		},
		rateLimiter: quotas.NewDefaultOutgoingRateLimiter(func() float64 {
			return float64(100)
		}),
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), scannerContextKey, ctx),
	})
	tlScavengerHBInterval = time.Millisecond * 10
	_, err := env.ExecuteActivity(taskQueueScavengerActivityName)
	s.NoError(err)
}
