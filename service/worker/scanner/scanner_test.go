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
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/client"

	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/scanner/build_ids"
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
	buildIdScavenger := expectedScanner{
		WFTypeName:    build_ids.BuildIdScavangerWorkflowName,
		TaskQueueName: build_ids.BuildIdScavengerTaskQueueName,
	}

	type testCase struct {
		Name                     string
		ExecutionsScannerEnabled bool
		TaskQueueScannerEnabled  bool
		HistoryScannerEnabled    bool
		BuildIdScavengerEnabled  bool
		DefaultStore             string
		ExpectedScanners         []expectedScanner
	}

	for _, c := range []testCase{
		{
			Name:                     "NothingEnabledNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "NothingEnabledSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{},
		},
		{
			Name:                     "HistoryScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "HistoryScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner},
		},
		{
			Name:                     "TaskQueueScannerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{}, // TODO: enable task queue scanner for NoSQL?
		},
		{
			Name:                     "TaskQueueScannerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{taskQueueScanner},
		},
		{
			Name:                     "ExecutionsScannerNoSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{executionScanner},
		},
		{
			Name:                     "ExecutionsScannerSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  false,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{executionScanner},
		},
		{
			Name:                     "BuildIdScavengerNoSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeNoSQL,
			ExpectedScanners:         []expectedScanner{buildIdScavenger},
		},
		{
			Name:                     "BuildIdScavengerSQL",
			ExecutionsScannerEnabled: false,
			TaskQueueScannerEnabled:  false,
			HistoryScannerEnabled:    false,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{buildIdScavenger},
		},
		{
			Name:                     "AllScannersSQL",
			ExecutionsScannerEnabled: true,
			TaskQueueScannerEnabled:  true,
			HistoryScannerEnabled:    true,
			BuildIdScavengerEnabled:  true,
			DefaultStore:             config.StoreTypeSQL,
			ExpectedScanners:         []expectedScanner{historyScanner, taskQueueScanner, executionScanner, buildIdScavenger},
		},
	} {
		s.Run(c.Name, func() {
			ctrl := gomock.NewController(s.T())
			mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
			mockSdkClient := mocksdk.NewMockClient(ctrl)
			mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
			mockAdminClient := adminservicemock.NewMockAdminServiceClient(ctrl)
			scanner := New(
				log.NewNoopLogger(),
				&Config{
					MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
					MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
					HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(c.HistoryScannerEnabled),
					BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(c.BuildIdScavengerEnabled),
					ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(c.ExecutionsScannerEnabled),
					TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(c.TaskQueueScannerEnabled),
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
				metrics.NoopMetricsHandler,
				p.NewMockExecutionManager(ctrl),
				// These nils are irrelevant since they're only used by the build ID scavenger which is not tested here.
				nil,
				nil,
				p.NewMockTaskManager(ctrl),
				historyservicemock.NewMockHistoryServiceClient(ctrl),
				mockAdminClient,
				nil,
				mockNamespaceRegistry,
				"active-cluster",
			)
			var wg sync.WaitGroup
			for _, sc := range c.ExpectedScanners {
				wg.Add(1)
				worker := mocksdk.NewMockWorker(ctrl)
				worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
				worker.EXPECT().Start()
				mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), sc.TaskQueueName, gomock.Any()).Return(worker)
				mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
				mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), sc.WFTypeName,
					gomock.Any()).Do(func(
					_ context.Context,
					_ client.StartWorkflowOptions,
					_ string,
					_ ...interface{},
				) {
					wg.Done()
				})
			}
			err := scanner.Start()
			s.NoError(err)
			wg.Wait()
			scanner.Stop()
		})
	}
}

// TestScannerWorkflow tests that the scanner can be shut down even when it hasn't finished starting.
// This fixes a rare issue that can occur when Stop() is called quickly after Start(). When Start() is called, the
// scanner starts a new goroutine for each scanner type. In that goroutine, an sdk client is created which dials the
// frontend service. If the test driver calls Stop() on the server, then the server stops the frontend service and the
// history service. In some cases, the frontend services stops before the sdk client has finished connecting to it.
// This causes the startWorkflow() call to fail with an error.  However, startWorkflowWithRetry retries the call for
// a whole minute, which causes the test to take a long time to fail. So, instead we immediately cancel all async
// requests when Stop() is called.
func (s *scannerTestSuite) TestScannerShutdown() {
	ctrl := gomock.NewController(s.T())

	logger := log.NewTestLogger()
	mockSdkClientFactory := sdk.NewMockClientFactory(ctrl)
	mockSdkClient := mocksdk.NewMockClient(ctrl)
	mockNamespaceRegistry := namespace.NewMockRegistry(ctrl)
	mockAdminClient := adminservicemock.NewMockAdminServiceClient(ctrl)
	worker := mocksdk.NewMockWorker(ctrl)
	scanner := New(
		logger,
		&Config{
			MaxConcurrentActivityExecutionSize:     dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskExecutionSize: dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentActivityTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			MaxConcurrentWorkflowTaskPollers:       dynamicconfig.GetIntPropertyFn(1),
			HistoryScannerEnabled:                  dynamicconfig.GetBoolPropertyFn(true),
			ExecutionsScannerEnabled:               dynamicconfig.GetBoolPropertyFn(false),
			TaskQueueScannerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			BuildIdScavengerEnabled:                dynamicconfig.GetBoolPropertyFn(false),
			Persistence: &config.Persistence{
				DefaultStore: config.StoreTypeNoSQL,
				DataStores: map[string]config.DataStore{
					config.StoreTypeNoSQL: {},
				},
			},
		},
		mockSdkClientFactory,
		metrics.NoopMetricsHandler,
		p.NewMockExecutionManager(ctrl),
		// These nils are irrelevant since they're only used by the build ID scavenger which is not tested here.
		nil,
		nil,
		p.NewMockTaskManager(ctrl),
		historyservicemock.NewMockHistoryServiceClient(ctrl),
		mockAdminClient,
		nil,
		mockNamespaceRegistry,
		"active-cluster",
	)
	mockSdkClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	worker.EXPECT().RegisterActivityWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().RegisterWorkflowWithOptions(gomock.Any(), gomock.Any()).AnyTimes()
	worker.EXPECT().Start()
	mockSdkClientFactory.EXPECT().NewWorker(gomock.Any(), gomock.Any(), gomock.Any()).Return(worker)
	var wg sync.WaitGroup
	wg.Add(1)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		_ client.StartWorkflowOptions,
		_ string,
		_ ...interface{},
	) (client.WorkflowRun, error) {
		wg.Done()
		<-ctx.Done()
		return nil, ctx.Err()
	})
	err := scanner.Start()
	s.NoError(err)
	wg.Wait()
	scanner.Stop()
}
