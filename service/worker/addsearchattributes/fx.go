package addsearchattributes

import (
	"context"

	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type (
	// addSearchAttributes represent background work needed for adding search attributes
	addSearchAttributes struct {
		initParams
	}

	initParams struct {
		fx.In
		EsClient       esclient.Client
		Manager        searchattribute.Manager
		MetricsHandler metrics.Handler
		Logger         log.Logger
	}
)

var Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

func newComponent(params initParams) workercommon.WorkerComponent {
	return &addSearchAttributes{initParams: params}
}

func (wc *addSearchAttributes) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(AddSearchAttributesWorkflow, workflow.RegisterOptions{Name: WorkflowName})
}

func (wc *addSearchAttributes) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *addSearchAttributes) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.activities())
}

func (wc *addSearchAttributes) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.AddSearchAttributesActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypeAPI),
		},
	}
}

func (wc *addSearchAttributes) activities() *activities {
	return &activities{
		esClient:       wc.EsClient,
		saManager:      wc.Manager,
		metricsHandler: wc.MetricsHandler.WithTags(metrics.OperationTag(metrics.AddSearchAttributesWorkflowScope)),
		logger:         wc.Logger,
	}
}
