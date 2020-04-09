package parentclosepolicy

import (
	"context"

	"go.temporal.io/temporal/activity"
	sdkclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/workflow"

	"go.temporal.io/temporal/worker"

	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	// BootstrapParams contains the set of params needed to bootstrap
	// the sub-system
	BootstrapParams struct {
		// Config contains the configuration for scanner
		// ServiceClient is an instance of temporal service client
		ServiceClient sdkclient.Client
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		Logger        log.Logger
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
	}

	// Processor is the background sub-system that execute workflow for ParentClosePolicy
	Processor struct {
		svcClient     sdkclient.Client
		clientBean    client.Bean
		metricsClient metrics.Client
		logger        log.Logger
	}
)

// New returns a new instance as daemon
func New(params *BootstrapParams) *Processor {
	return &Processor{
		svcClient:     params.ServiceClient,
		metricsClient: params.MetricsClient,
		logger:        params.Logger.WithTags(tag.ComponentBatcher),
		clientBean:    params.ClientBean,
	}
}

// Start starts the scanner
func (s *Processor) Start() error {
	ctx := context.WithValue(context.Background(), processorContextKey, s)
	workerOpts := worker.Options{
		BackgroundActivityContext: ctx,
	}
	processorWorker := worker.New(s.svcClient, processorTaskListName, workerOpts)

	processorWorker.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	processorWorker.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	return processorWorker.Start()
}
