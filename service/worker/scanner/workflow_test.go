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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/uber-go/tally"
	"go.temporal.io/temporal/testsuite"
	"go.temporal.io/temporal/worker"
	"go.uber.org/zap"
)

type scannerWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestScannerWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(scannerWorkflowTestSuite))
}

func (s *scannerWorkflowTestSuite) TestWorkflow() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(taskListScavengerActivityName, mock.Anything).Return(nil)
	env.ExecuteWorkflow(tlScannerWFTypeName)
	s.True(env.IsWorkflowCompleted())
}

func (s *scannerWorkflowTestSuite) TestScavengerActivity() {
	env := s.NewTestActivityEnvironment()
	taskDB := &mocks.TaskManager{}
	taskDB.On("ListTaskList", mock.Anything).Return(&p.ListTaskListResponse{}, nil)
	ctx := scannerContext{
		taskDB:        taskDB,
		domainDB:      &mocks.MetadataManager{},
		metricsClient: metrics.NewClient(tally.NoopScope, metrics.Worker),
		zapLogger:     zap.NewNop(),
		logger:        loggerimpl.NewLogger(zap.NewNop()),
	}
	env.SetTestTimeout(time.Second * 5)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), scannerContextKey, ctx),
	})
	tlScavengerHBInterval = time.Millisecond * 10
	_, err := env.ExecuteActivity(taskListScavengerActivityName)
	s.NoError(err)
}
