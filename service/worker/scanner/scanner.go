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
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	cclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/worker"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

const (
	// scannerStartUpDelay is to let services warm up
	scannerStartUpDelay = time.Second * 4
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
	cfg.Persistence.SetMaxQPS(cfg.Persistence.DefaultStore, cfg.PersistenceMaxQPS())
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

	var workerTaskListName string
	if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeSQL {
		go s.startWorkflowWithRetry(tlScannerWFStartOptions, tlScannerWFTypeName)
		workerTaskListName = tlScannerTaskListName
	} else if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeCassandra {
		go s.startWorkflowWithRetry(historyScannerWFStartOptions, historyScannerWFTypeName)
		workerTaskListName = historyScannerTaskListName
	}

	return worker.New(s.context.GetSDKClient(), common.SystemLocalDomainName, workerTaskListName, workerOpts).Start()
}

func (s *Scanner) startWorkflowWithRetry(
	options cclient.StartWorkflowOptions,
	workflowType string,
) {

	// let history / matching service warm up
	time.Sleep(scannerStartUpDelay)

	sdkClient := cclient.NewClient(s.context.GetSDKClient(), common.SystemLocalDomainName, &cclient.Options{})
	policy := backoff.NewExponentialRetryPolicy(time.Second)
	policy.SetMaximumInterval(time.Minute)
	policy.SetExpirationInterval(backoff.NoInterval)
	err := backoff.Retry(func() error {
		return s.startWorkflow(sdkClient, options, workflowType)
	}, policy, func(err error) bool {
		return true
	})
	if err != nil {
		s.context.GetLogger().Fatal("unable to start scanner", tag.Error(err))
	}
}

func (s *Scanner) startWorkflow(
	client cclient.Client,
	options cclient.StartWorkflowOptions,
	workflowType string,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, err := client.StartWorkflow(ctx, options, workflowType)
	cancel()
	if err != nil {
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			return nil
		}
		s.context.GetLogger().Error("error starting "+workflowType+" workflow", tag.Error(err))
		return err
	}
	s.context.GetLogger().Info(workflowType + " workflow successfully started")
	return nil
}
