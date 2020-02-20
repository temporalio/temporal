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
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/shared"
	cclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/uber/cadence/service/worker/scanner/executions"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	// scannerStartUpDelay is to let services warm up
	scannerStartUpDelay = time.Second * 4
)

var (
	defaultExecutionsScannerParams = executions.ScannerWorkflowParams{
		// fullExecutionsScanDefaultQuery indicates the visibility scanner should scan through all open workflows
		VisibilityQuery: "SELECT * from elasticSearch.executions WHERE state IS open", // TODO: depending on if we go straight to ES or through frontend this query will look different
	}
)

type (
	// Config defines the configuration for scanner
	Config struct {
		// PersistenceMaxQPS the max rate of calls to persistence
		PersistenceMaxQPS dynamicconfig.IntPropertyFn
		// Persistence contains the persistence configuration
		Persistence *config.Persistence
		// ClusterMetadata contains the metadata for this cluster
		ClusterMetadata cluster.Metadata
		// TaskListScannerEnabled indicates if taskList scanner should be started as part of scanner
		TaskListScannerEnabled dynamicconfig.BoolPropertyFn
		// HistoryScannerEnabled indicates if history scanner should be started as part of scanner
		HistoryScannerEnabled dynamicconfig.BoolPropertyFn
		// ExecutionsScannerEnabled indicates if executions scanner should be started as part of scanner
		ExecutionsScannerEnabled dynamicconfig.BoolPropertyFn
	}

	// BootstrapParams contains the set of params needed to bootstrap
	// the scanner sub-system
	BootstrapParams struct {
		// Config contains the configuration for scanner
		Config Config
		// TallyScope is an instance of tally metrics scope
		TallyScope tally.Scope
	}

	// scannerContext is the context object that get's
	// passed around within the scanner workflows / activities
	scannerContext struct {
		resource.Resource
		cfg        Config
		tallyScope tally.Scope
		zapLogger  *zap.Logger
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
	resource resource.Resource,
	params *BootstrapParams,
) *Scanner {

	cfg := params.Config
	zapLogger, err := zap.NewProduction()
	if err != nil {
		resource.GetLogger().Fatal("failed to initialize zap logger", tag.Error(err))
	}
	return &Scanner{
		context: scannerContext{
			Resource:   resource,
			cfg:        cfg,
			tallyScope: params.TallyScope,
			zapLogger:  zapLogger,
		},
	}
}

// Start starts the scanner
func (s *Scanner) Start() error {
	workerOpts := worker.Options{
		Logger:                                 s.context.zapLogger,
		MetricsScope:                           s.context.tallyScope,
		MaxConcurrentActivityExecutionSize:     maxConcurrentActivityExecutionSize,
		MaxConcurrentDecisionTaskExecutionSize: maxConcurrentDecisionTaskExecutionSize,
		BackgroundActivityContext:              context.WithValue(context.Background(), scannerContextKey, s.context),
	}

	var workerTaskListNames []string
	if s.context.cfg.ExecutionsScannerEnabled() {
		workerTaskListNames = append(workerTaskListNames, executionsScannerTaskListName)
		go s.startWorkflowWithRetry(executionsScannerWFStartOptions, executionsScannerWFTypeName, defaultExecutionsScannerParams)
	}

	if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeSQL && s.context.cfg.TaskListScannerEnabled() {
		go s.startWorkflowWithRetry(tlScannerWFStartOptions, tlScannerWFTypeName)
		workerTaskListNames = append(workerTaskListNames, tlScannerTaskListName)
	} else if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeCassandra && s.context.cfg.HistoryScannerEnabled() {
		go s.startWorkflowWithRetry(historyScannerWFStartOptions, historyScannerWFTypeName)
		workerTaskListNames = append(workerTaskListNames, historyScannerTaskListName)
	}

	for _, tl := range workerTaskListNames {
		if err := worker.New(s.context.GetSDKClient(), common.SystemLocalDomainName, tl, workerOpts).Start(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scanner) startWorkflowWithRetry(
	options cclient.StartWorkflowOptions,
	workflowType string,
	workflowArgs ...interface{},
) {

	// let history / matching service warm up
	time.Sleep(scannerStartUpDelay)

	sdkClient := cclient.NewClient(s.context.GetSDKClient(), common.SystemLocalDomainName, &cclient.Options{})
	policy := backoff.NewExponentialRetryPolicy(time.Second)
	policy.SetMaximumInterval(time.Minute)
	policy.SetExpirationInterval(backoff.NoInterval)
	err := backoff.Retry(func() error {
		return s.startWorkflow(sdkClient, options, workflowType, workflowArgs)
	}, policy, func(err error) bool {
		return true
	})
	if err != nil {
		s.context.GetLogger().Fatal("unable to start scanner", tag.WorkflowType(workflowType), tag.Error(err))
	}
}

func (s *Scanner) startWorkflow(
	client cclient.Client,
	options cclient.StartWorkflowOptions,
	workflowType string,
	workflowArgs ...interface{},
) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, err := client.StartWorkflow(ctx, options, workflowType, workflowArgs)
	cancel()
	if err != nil {
		if _, ok := err.(*shared.WorkflowExecutionAlreadyStartedError); ok {
			return nil
		}
		s.context.GetLogger().Error("error starting "+workflowType+" workflow", tag.Error(err))
		return err
	}
	s.context.GetLogger().Info(workflowType + " workflow successfully started")
	return nil
}
