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
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	pfactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	cclient "go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
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
		// SDKClient is an instance of cadence sdk client
		SDKClient workflowserviceclient.Interface
		// clientBean is an instance of ClientBean
		ClientBean client.Bean
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		Logger        log.Logger
		// TallyScope is an instance of tally metrics scope
		TallyScope tally.Scope
	}

	// scannerContext is the context object that get's
	// passed around within the scanner workflows / activities
	scannerContext struct {
		taskDB        p.TaskManager
		domainDB      p.MetadataManager
		historyDB     p.HistoryV2Manager
		cfg           Config
		sdkClient     workflowserviceclient.Interface
		clientBean    client.Bean
		metricsClient metrics.Client
		tallyScope    tally.Scope
		logger        log.Logger
		zapLogger     *zap.Logger
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
func New(params *BootstrapParams) *Scanner {
	cfg := params.Config
	cfg.Persistence.SetMaxQPS(cfg.Persistence.DefaultStore, cfg.PersistenceMaxQPS())
	zapLogger, err := zap.NewProduction()
	if err != nil {
		params.Logger.Fatal("failed to initialize zap logger", tag.Error(err))
	}
	return &Scanner{
		context: scannerContext{
			cfg:           cfg,
			sdkClient:     params.SDKClient,
			clientBean:    params.ClientBean,
			metricsClient: params.MetricsClient,
			tallyScope:    params.TallyScope,
			zapLogger:     zapLogger,
			logger:        params.Logger,
		},
	}
}

// Start starts the scanner
func (s *Scanner) Start() error {
	if err := s.buildContext(); err != nil {
		return err
	}
	workerOpts := worker.Options{
		Logger:                                 s.context.zapLogger,
		MetricsScope:                           s.context.tallyScope,
		MaxConcurrentActivityExecutionSize:     maxConcurrentActivityExecutionSize,
		MaxConcurrentDecisionTaskExecutionSize: maxConcurrentDecisionTaskExecutionSize,
		BackgroundActivityContext:              context.WithValue(context.Background(), scannerContextKey, s.context),
	}

	if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeSQL {
		go s.startWorkflowWithRetry(tlScannerWFStartOptions, tlScannerWFTypeName)
	} else if s.context.cfg.Persistence.DefaultStoreType() == config.StoreTypeCassandra {
		go s.startWorkflowWithRetry(historyScannerWFStartOptions, historyScannerWFTypeName)
	}

	worker := worker.New(s.context.sdkClient, common.SystemLocalDomainName, tlScannerTaskListName, workerOpts)
	return worker.Start()
}

func (s *Scanner) startWorkflowWithRetry(options cclient.StartWorkflowOptions, wfType string) error {
	client := cclient.NewClient(s.context.sdkClient, common.SystemLocalDomainName, &cclient.Options{})
	policy := backoff.NewExponentialRetryPolicy(time.Second)
	policy.SetMaximumInterval(time.Minute)
	policy.SetExpirationInterval(backoff.NoInterval)
	return backoff.Retry(func() error {
		return s.startWorkflow(client, options, wfType)
	}, policy, func(err error) bool {
		return true
	})
}

func (s *Scanner) startWorkflow(client cclient.Client, options cclient.StartWorkflowOptions, wfType string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	_, err := client.StartWorkflow(ctx, options, wfType)
	cancel()
	if err != nil {
		if _, ok := err.(*shared.WorkflowExecutionAlreadyStartedError); ok {
			return nil
		}
		s.context.logger.Error("error starting "+wfType+" workflow", tag.Error(err))
		return err
	}
	s.context.logger.Info(wfType + " workflow successfully started")
	return nil
}

func (s *Scanner) buildContext() error {
	cfg := &s.context.cfg
	pFactory := pfactory.New(cfg.Persistence, cfg.ClusterMetadata.GetCurrentClusterName(), s.context.metricsClient, s.context.logger)
	domainDB, err := pFactory.NewMetadataManager()
	if err != nil {
		return err
	}
	taskDB, err := pFactory.NewTaskManager()
	if err != nil {
		return err
	}
	historyDB, err := pFactory.NewHistoryV2Manager()
	if err != nil {
		return err
	}
	s.context.taskDB = taskDB
	s.context.domainDB = domainDB
	s.context.historyDB = historyDB
	return nil
}
