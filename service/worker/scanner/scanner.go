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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
)

const (
	// scannerStartUpDelay is to let services warm up
	scannerStartUpDelay = time.Second * 4
)

type (
	// Config defines the configuration for scanner
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn

		// PersistenceMaxQPS the max rate of calls to persistence
		PersistenceMaxQPS dynamicconfig.IntPropertyFn
		// Persistence contains the persistence configuration
		Persistence *config.Persistence
		// TaskQueueScannerEnabled indicates if taskQueue scanner should be started as part of scanner
		TaskQueueScannerEnabled dynamicconfig.BoolPropertyFn
		// HistoryScannerEnabled indicates if history scanner should be started as part of scanner
		HistoryScannerEnabled dynamicconfig.BoolPropertyFn
		// ExecutionsScannerEnabled indicates if executions scanner should be started as part of scanner
		ExecutionsScannerEnabled dynamicconfig.BoolPropertyFn
	}

	// scannerContext is the context object that get's
	// passed around within the scanner workflows / activities
	scannerContext struct {
		cfg              *Config
		logger           log.Logger
		sdkSystemClient  sdkclient.Client
		metricsClient    metrics.Client
		executionManager persistence.ExecutionManager
		taskManager      persistence.TaskManager
		historyClient    historyservice.HistoryServiceClient
	}

	// Scanner is the background sub-system that does full scans
	// of database tables to cleanup resources, monitor anamolies
	// and emit stats for analytics
	Scanner struct {
		context scannerContext
	}
)

// New returns a new instance of scanner daemon
// Scanner is the background sub-system that does full
// scans of database tables in an attempt to cleanup
// resources, monitor system anamolies and emit stats
// for analysis and alerting
func New(
	logger log.Logger,
	cfg *Config,
	sdkSystemClient sdkclient.Client,
	metricsClient metrics.Client,
	executionManager persistence.ExecutionManager,
	taskManager persistence.TaskManager,
	historyClient historyservice.HistoryServiceClient,
) *Scanner {
	return &Scanner{
		context: scannerContext{
			cfg:              cfg,
			sdkSystemClient:  sdkSystemClient,
			logger:           logger,
			metricsClient:    metricsClient,
			executionManager: executionManager,
			taskManager:      taskManager,
			historyClient:    historyClient,
		},
	}
}

// Start starts the scanner
func (s *Scanner) Start() error {
	workerOpts := worker.Options{
		MaxConcurrentActivityExecutionSize:     s.context.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: s.context.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       s.context.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       s.context.cfg.MaxConcurrentWorkflowTaskPollers(),

		BackgroundActivityContext: context.WithValue(context.Background(), scannerContextKey, s.context),
	}

	var workerTaskQueueNames []string
	if s.context.cfg.ExecutionsScannerEnabled() {
		go s.startWorkflowWithRetry(executionsScannerWFStartOptions, executionsScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, executionsScannerTaskQueueName)
	}

	if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeSQL && s.context.cfg.TaskQueueScannerEnabled() {
		go s.startWorkflowWithRetry(tlScannerWFStartOptions, tqScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, tqScannerTaskQueueName)
	} else if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeNoSQL && s.context.cfg.HistoryScannerEnabled() {
		go s.startWorkflowWithRetry(historyScannerWFStartOptions, historyScannerWFTypeName)
		workerTaskQueueNames = append(workerTaskQueueNames, historyScannerTaskQueueName)
	}

	for _, tl := range workerTaskQueueNames {
		work := worker.New(s.context.sdkSystemClient, tl, workerOpts)

		work.RegisterWorkflowWithOptions(TaskQueueScannerWorkflow, workflow.RegisterOptions{Name: tqScannerWFTypeName})
		work.RegisterWorkflowWithOptions(HistoryScannerWorkflow, workflow.RegisterOptions{Name: historyScannerWFTypeName})
		work.RegisterWorkflowWithOptions(ExecutionsScannerWorkflow, workflow.RegisterOptions{Name: executionsScannerWFTypeName})
		work.RegisterActivityWithOptions(TaskQueueScavengerActivity, activity.RegisterOptions{Name: taskQueueScavengerActivityName})
		work.RegisterActivityWithOptions(HistoryScavengerActivity, activity.RegisterOptions{Name: historyScavengerActivityName})
		work.RegisterActivityWithOptions(ExecutionsScavengerActivity, activity.RegisterOptions{Name: executionsScavengerActivityName})

		if err := work.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scanner) startWorkflowWithRetry(
	options sdkclient.StartWorkflowOptions,
	workflowType string,
	workflowArgs ...interface{},
) {

	// let history / matching service warm up
	time.Sleep(scannerStartUpDelay)

	policy := backoff.NewExponentialRetryPolicy(time.Second)
	policy.SetMaximumInterval(time.Minute)
	policy.SetExpirationInterval(backoff.NoInterval)
	err := backoff.ThrottleRetry(func() error {
		return s.startWorkflow(s.context.sdkSystemClient, options, workflowType, workflowArgs...)
	}, policy, func(err error) bool {
		return true
	})
	if err != nil {
		s.context.logger.Fatal("unable to start scanner", tag.WorkflowType(workflowType), tag.Error(err))
	}
}

func (s *Scanner) startWorkflow(
	client sdkclient.Client,
	options sdkclient.StartWorkflowOptions,
	workflowType string,
	workflowArgs ...interface{},
) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, err := client.ExecuteWorkflow(ctx, options, workflowType, workflowArgs...)
	cancel()
	if err != nil {
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			return nil
		}
		s.context.logger.Error("error starting "+workflowType+" workflow", tag.Error(err))
		return err
	}
	s.context.logger.Info(workflowType + " workflow successfully started")
	return nil
}
