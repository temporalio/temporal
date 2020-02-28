// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
