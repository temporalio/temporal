package scanner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.uber.org/mock/gomock"
)

type scannerWorkflowTestSuite struct {
	parallelsuite.Suite[*scannerWorkflowTestSuite]
}

func TestScannerWorkflowTestSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, new(scannerWorkflowTestSuite)) //nolint:staticcheck // SA1019: suite mutates scanner heartbeat interval package global.
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
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(taskQueueScavengerActivityName, mock.Anything).Return(nil)
	env.ExecuteWorkflow(tqScannerWFTypeName)
	s.True(env.IsWorkflowCompleted())
}

func (s *scannerWorkflowTestSuite) TestScavengerActivity() {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
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
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), scannerContextKey, ctx),
	})
	tlScavengerHBInterval = time.Millisecond * 10
	_, err := env.ExecuteActivity(taskQueueScavengerActivityName)
	s.NoError(err)
}
