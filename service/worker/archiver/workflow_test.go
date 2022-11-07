// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var (
	workflowTestMetrics *metrics.MockMetricsHandler
	workflowTestLogger  log.Logger
	workflowTestHandler *MockHandler
	workflowTestPump    *MockPump
	workflowTestConfig  *Config
)

type workflowSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller *gomock.Controller
}

func (s *workflowSuite) registerWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflow(archivalWorkflowTest)
	env.RegisterWorkflowWithOptions(archivalWorkflow, workflow.RegisterOptions{Name: archivalWorkflowFnName})

	env.RegisterActivityWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	env.RegisterActivityWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	env.RegisterActivityWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})
}

func TestWorkflowSuite(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	workflowTestMetrics = metrics.NewMockMetricsHandler(s.controller)
	workflowTestLogger = log.NewNoopLogger()
	workflowTestHandler = NewMockHandler(s.controller)
	workflowTestPump = NewMockPump(s.controller)
	workflowTestConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(0),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(0),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}
}

func (s *workflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowSuite) TestArchivalWorkflow_Fail_HashesDoNotEqual() {
	workflowTestMetrics.EXPECT().WithTags(gomock.Any()).Return(workflowTestMetrics)
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverWorkflowStartedCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ServiceLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ArchiverHandleAllRequestsLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	pumpedRequestCounter := metrics.NewMockCounterMetric(s.controller)
	pumpedRequestCounter.EXPECT().Record(int64(3))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumPumpedRequestsCount.GetMetricName()).Return(pumpedRequestCounter)
	handledRequestCounter := metrics.NewMockCounterMetric(s.controller)
	handledRequestCounter.EXPECT().Record(int64(3))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumHandledRequestsCount.GetMetricName()).Return(handledRequestCounter)
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverPumpedNotEqualHandledCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	workflowTestHandler.EXPECT().Start()
	workflowTestHandler.EXPECT().Finished().Return([]uint64{9, 7, 0})
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes: []uint64{8, 7, 0},
	})

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	var continueAsNewErr *workflow.ContinueAsNewError
	s.True(errors.As(err, &continueAsNewErr), "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Exit_TimeoutWithoutSignals() {
	workflowTestMetrics.EXPECT().WithTags(gomock.Any()).Return(workflowTestMetrics)
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverWorkflowStartedCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ServiceLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ArchiverHandleAllRequestsLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	pumpedRequestCounter := metrics.NewMockCounterMetric(s.controller)
	pumpedRequestCounter.EXPECT().Record(int64(0))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumPumpedRequestsCount.GetMetricName()).Return(pumpedRequestCounter)
	handledRequestCounter := metrics.NewMockCounterMetric(s.controller)
	handledRequestCounter.EXPECT().Record(int64(0))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumHandledRequestsCount.GetMetricName()).Return(handledRequestCounter)
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverWorkflowStoppingCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	workflowTestHandler.EXPECT().Start()
	workflowTestHandler.EXPECT().Finished().Return([]uint64{})
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes:          []uint64{},
		TimeoutWithoutSignals: true,
	})

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestArchivalWorkflow_Success() {
	workflowTestMetrics.EXPECT().WithTags(gomock.Any()).Return(workflowTestMetrics)
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverWorkflowStartedCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ServiceLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	workflowTestMetrics.EXPECT().Timer(metrics.ArchiverHandleAllRequestsLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc)
	pumpedRequestCounter := metrics.NewMockCounterMetric(s.controller)
	pumpedRequestCounter.EXPECT().Record(int64(5))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumPumpedRequestsCount.GetMetricName()).Return(pumpedRequestCounter)
	handledRequestCounter := metrics.NewMockCounterMetric(s.controller)
	handledRequestCounter.EXPECT().Record(int64(5))
	workflowTestMetrics.EXPECT().Counter(metrics.ArchiverNumHandledRequestsCount.GetMetricName()).Return(handledRequestCounter)
	workflowTestHandler.EXPECT().Start()
	workflowTestHandler.EXPECT().Finished().Return([]uint64{1, 2, 3, 4, 5})
	workflowTestPump.EXPECT().Run().Return(PumpResult{
		PumpedHashes: []uint64{1, 2, 3, 4, 5},
	})

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.ExecuteWorkflow(archivalWorkflowTest)

	s.True(env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	var continueAsNew *workflow.ContinueAsNewError
	s.True(errors.As(err, &continueAsNew), "Called ContinueAsNew")
	env.AssertExpectations(s.T())
}

func (s *workflowSuite) TestReplayArchiveHistoryWorkflow() {
	logger := log.NewTestLogger()
	globalLogger = workflowTestLogger
	globalMetricsHandler = workflowTestMetrics

	globalConfig = &Config{
		ArchiverConcurrency:           dynamicconfig.GetIntPropertyFn(50),
		ArchivalsPerIteration:         dynamicconfig.GetIntPropertyFn(1000),
		TimeLimitPerArchivalIteration: dynamicconfig.GetDurationPropertyFn(MaxArchivalIterationTimeout()),
	}

	replayer := worker.NewWorkflowReplayer()
	s.registerWorkflowsForReplayer(replayer)
	err := replayer.ReplayWorkflowHistoryFromJSONFile(log.NewSdkLogger(logger), "testdata/archival_workflow_history_v1.json")
	s.NoError(err)
}

func archivalWorkflowTest(ctx workflow.Context) error {
	return archivalWorkflowHelper(ctx, workflowTestLogger, workflowTestMetrics, workflowTestConfig, workflowTestHandler, workflowTestPump, nil)
}

func (s *workflowSuite) registerWorkflowsForReplayer(env worker.WorkflowReplayer) {
	env.RegisterWorkflowWithOptions(archivalWorkflow, workflow.RegisterOptions{Name: archivalWorkflowFnName})
}
