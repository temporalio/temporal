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

package archiver

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

var (
	workflowTestMetrics *mmocks.Client
	workflowTestLogger  log.Logger
	workflowTestHandler *MockHandler
	workflowTestPump    *PumpMock
	workflowTestConfig  *Config
)

type workflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func (s *workflowSuite) SetupSuite() {
	workflow.Register(archivalWorkflowTest)
}

func TestWorkflowSuite(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	workflowTestMetrics = &mmocks.Client{}
	workflowTestLogger = loggerimpl.NewLogger(zap.NewNop())
	workflowTestHandler = &MockHandler{}
	workflowTestPump = &PumpMock{}
	workflowTestConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(0),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(0),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
}

func (s *workflowSuite) TestArchivalWorkflow_Fail_HashesDoNotEqual() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(3)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(3)).Once()
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverPumpedNotEqualHandledCount).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{9, 7, 0}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes: []uint64{8, 7, 0},
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	_, ok := env.GetWorkflowError().(*workflow.ContinueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Exit_TimeoutWithoutSignals() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(0)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(0)).Once()
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStoppingCount).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes:          []uint64{},
		TimeoutWithoutSignals: true,
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Success() {
	workflowTestMetrics.On("IncCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverWorkflowStartedCount).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.CadenceLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("StartTimer", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverHandleAllRequestsLatency).Return(metrics.NopStopwatch()).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumPumpedRequestsCount, int64(5)).Once()
	workflowTestMetrics.On("AddCounter", metrics.ArchiverArchivalWorkflowScope, metrics.ArchiverNumHandledRequestsCount, int64(5)).Once()
	workflowTestHandler.On("Start").Once()
	workflowTestHandler.On("Finished").Return([]uint64{1, 2, 3, 4, 5}).Once()
	workflowTestPump.On("Run").Return(PumpResult{
		PumpedHashes: []uint64{1, 2, 3, 4, 5},
	}).Once()

	env := s.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	_, ok := env.GetWorkflowError().(*workflow.ContinueAsNewError)
	s.True(ok, "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestReplayArchiveHistoryWorkflow() {
	logger, _ := zap.NewDevelopment()
	globalLogger = workflowTestLogger
	globalMetricsClient = metrics.NewClient(tally.NewTestScope("replay", nil), metrics.Worker)
	globalConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(50),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(1000),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
	err := worker.ReplayWorkflowHistoryFromJSONFile(logger, "testdata/archival_workflow_history_v1.json")
	s.NoError(err)
}

func archivalWorkflowTest(ctx workflow.Context) error {
	return archivalWorkflowHelper(ctx, workflowTestLogger, workflowTestMetrics, workflowTestConfig, workflowTestHandler, workflowTestPump, nil)
}
