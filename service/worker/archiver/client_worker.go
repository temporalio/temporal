package archiver

import (
	"context"
	"time"

	"go.temporal.io/temporal/activity"
	sdkclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/worker"
	"go.temporal.io/temporal/workflow"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	// ClientWorker is a temporal client worker
	ClientWorker interface {
		Start() error
		Stop()
	}

	clientWorker struct {
		worker         worker.Worker
		namespaceCache cache.NamespaceCache
	}

	// BootstrapContainer contains everything need for bootstrapping
	BootstrapContainer struct {
		PublicClient     sdkclient.Client
		MetricsClient    metrics.Client
		Logger           log.Logger
		HistoryV2Manager persistence.HistoryManager
		NamespaceCache   cache.NamespaceCache
		Config           *Config
		ArchiverProvider provider.ArchiverProvider
	}

	// Config for ClientWorker
	Config struct {
		ArchiverConcurrency           dynamicconfig.IntPropertyFn
		ArchivalsPerIteration         dynamicconfig.IntPropertyFn
		TimeLimitPerArchivalIteration dynamicconfig.DurationPropertyFn
	}

	contextKey int
)

const (
	workflowIDPrefix                = "temporal-archival"
	decisionTaskList                = "temporal-archival-tl"
	signalName                      = "temporal-archival-signal"
	archivalWorkflowFnName          = "archivalWorkflow"
	workflowStartToCloseTimeout     = time.Hour * 24 * 30
	workflowTaskStartToCloseTimeout = time.Minute

	bootstrapContainerKey contextKey = iota
)

// these globals exist as a work around because no primitive exists to pass such objects to workflow code
var (
	globalLogger        log.Logger
	globalMetricsClient metrics.Client
	globalConfig        *Config
)

// NewClientWorker returns a new ClientWorker
func NewClientWorker(container *BootstrapContainer) ClientWorker {
	globalLogger = container.Logger.WithTags(tag.ComponentArchiver, tag.WorkflowNamespace(common.SystemLocalNamespace))
	globalMetricsClient = container.MetricsClient
	globalConfig = container.Config
	actCtx := context.WithValue(context.Background(), bootstrapContainerKey, container)
	wo := worker.Options{
		BackgroundActivityContext: actCtx,
	}
	clientWorker := &clientWorker{
		worker:         worker.New(container.PublicClient, decisionTaskList, wo),
		namespaceCache: container.NamespaceCache,
	}

	clientWorker.worker.RegisterWorkflowWithOptions(archivalWorkflow, workflow.RegisterOptions{Name: archivalWorkflowFnName})
	clientWorker.worker.RegisterActivityWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	clientWorker.worker.RegisterActivityWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	clientWorker.worker.RegisterActivityWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})

	return clientWorker
}

// Start the ClientWorker
func (w *clientWorker) Start() error {
	if err := w.worker.Start(); err != nil {
		w.worker.Stop()
		return err
	}
	return nil
}

// Stop the ClientWorker
func (w *clientWorker) Stop() {
	w.worker.Stop()
	w.namespaceCache.Stop()
}
