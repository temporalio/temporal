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

package scanner

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
)

type scannerTestSuite struct {
	suite.Suite
}

func TestScanner(t *testing.T) {
	suite.Run(t, new(scannerTestSuite))
}

func (s *scannerTestSuite) TestScannerEnabled() {
	type expectedScanner struct {
		WFTypeName    string
		TaskQueueName string
	}
	executionScanner := expectedScanner{
		WFTypeName:    executionsScannerWFTypeName,
		TaskQueueName: executionsScannerTaskQueueName,
	}
	_ = executionScanner
	taskQueueScanner := expectedScanner{
		WFTypeName:    tqScannerWFTypeName,
		TaskQueueName: tqScannerTaskQueueName,
	}
	historyScanner := expectedScanner{
		WFTypeName:    historyScannerWFTypeName,
		TaskQueueName: historyScannerTaskQueueName,
	}

	type testCase struct {
		Name                     string
		ExecutionsScannerEnabled bool
		TaskQueueScannerEnabled  bool
		HistoryScannerEnabled    bool
		DefaultStore             string
		ExpectedScanners         []expectedScanner
	}

	for _, c := range []testCase{
		{
			Name:                     "NothingEnabledNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "NothingEnabledSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "HistoryScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "HistoryScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "TaskQueueScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{}, // TODO: enable task queue scanner for NoSQL?
		},
		{
			Name:                     "TaskQueueScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{taskQueueScanner},
		},
		{
			Name:                     "ExecutionsScannerNoSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{executionScanner},
		},
		{
			Name:                     "ExecutionsScannerSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{executionScanner},
		},
		{
			Name:                     "AllScannersSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner, taskQueueScanner, executionScanner},
		},
	} {
		s.Run(c.Name, func() {
			ctrl := gomock.NewController(s.T())
			mockWorkerFactory := sdk.NewMockWorkerFactory(ctrl)
			mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
			mockSdkClient := mocksdk.NewMockClient(ctrl)
			mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
			scanner := New(
				log.NewNoopLogger(),
				&Config{
					MaxConcurrentActivityExecutionSize: func() int {
						return 1
					},
					MaxConcurrentWorkflowTaskExecutionSize: func() int {
						return 1
					},
					MaxConcurrentActivityTaskPollers: func() int {
						return 1
					},
					MaxConcurrentWorkflowTaskPollers: func() int {
						return 1
					},
					ExecutionsScannerEnabled: func() bool {
						return c.ExecutionsScannerEnabled
					},
					HistoryScannerEnabled: func() bool {
						return c.HistoryScannerEnabled
					},
					TaskQueueScannerEnabled: func() bool {
						return c.TaskQueueScannerEnabled
					},
					Persistence: &config.Persistence{
						DefaultStore: c.DefaultStore,
						DataStores: map[string]config.DataStore{
							config.StoreTypeNoSQL: {},
							config.StoreTypeSQL: {
								SQL: &config.SQL{},
							},
						},
					},
				},
				mockSdkClientFactory,
				metrics.NoopClient,
				p.NewMockExecutionManager(ctrl),
				p.NewMockTaskManager(ctrl),
				historyservicemock.NewMockHistoryServiceClient(ctrl),
				mockNamespaceRegistry,
				mockWorkerFactory,
			)
			for _, sc := range c.ExpectedScanners {
				worker := mocksdk.NewMockWorker(ctrl)
				worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().Start()
				mockWorkerFactory.EXPECT().New(gomock.Any(), sc.TaskQueueName, gomock.Any()).Return(worker)
				mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
				mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), sc.WFTypeName, gomock.Any())
			}
			err := scanner.Start()
			s.NoError(err)
			scanner.Stop()
		})
	}
}
