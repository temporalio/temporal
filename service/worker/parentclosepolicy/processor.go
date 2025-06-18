package parentclosepolicy

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
)

type (
	// Config defines the configuration for parent close policy worker
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn
		NumParentClosePolicySystemWorkflows    dynamicconfig.IntPropertyFn
	}

	// BootstrapParams contains the set of params needed to bootstrap the sub-system
	BootstrapParams struct {
		SdkClientFactory sdk.ClientFactory
		// MetricsHandler is an instance of metrics object for emitting stats
		MetricsHandler metrics.Handler
		// Logger is the logger
		Logger log.Logger
		// Config contains the configuration for scanner
		Config Config
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
		// CurrentCluster is the name of current cluster
		CurrentCluster string

		HostInfo membership.HostInfo
	}

	// Processor is the background sub-system that execute workflow for ParentClosePolicy
	Processor struct {
		sdkClientFactory sdk.ClientFactory
		clientBean       client.Bean
		metricsHandler   metrics.Handler
		cfg              Config
		logger           log.Logger
		currentCluster   string
		hostInfo         membership.HostInfo
	}
)

// New returns a new instance as daemon
func New(params *BootstrapParams) *Processor {
	return &Processor{
		sdkClientFactory: params.SdkClientFactory,
		metricsHandler:   params.MetricsHandler.WithTags(metrics.OperationTag(metrics.ParentClosePolicyProcessorScope)),
		cfg:              params.Config,
		logger:           log.With(params.Logger, tag.ComponentBatcher),
		clientBean:       params.ClientBean,
		currentCluster:   params.CurrentCluster,
		hostInfo:         params.HostInfo,
	}
}

// Start starts the scanner
func (s *Processor) Start() error {
	svcClient := s.sdkClientFactory.GetSystemClient()
	processorWorker := s.sdkClientFactory.NewWorker(svcClient, processorTaskQueueName, getWorkerOptions(s))
	processorWorker.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	processorWorker.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	return processorWorker.Start()
}

func getWorkerOptions(p *Processor) worker.Options {
	ctx := context.WithValue(context.Background(), processorContextKey, p)
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo)

	return worker.Options{
		MaxConcurrentActivityExecutionSize:     p.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: p.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       p.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       p.cfg.MaxConcurrentWorkflowTaskPollers(),
		BackgroundActivityContext:              ctx,
		Identity:                               "temporal-system@" + p.hostInfo.Identity(),
	}
}
