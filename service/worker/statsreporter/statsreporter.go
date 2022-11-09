package statsreporter

import (
	"context"
	"go.temporal.io/server/common/membership"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ns "go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/internal/goro"
)

const listNamespacePageSize = 100

type StatsReporter struct {
	status                int32
	reporter              *goro.Handle
	visibilityRateLimiter quotas.RateLimiter

	Logger                log.Logger
	MetricsHandler        metrics.MetricsHandler
	MetadataManager       persistence.MetadataManager
	VisibilityManager     manager.VisibilityManager
	BaseReportInterval    dynamicconfig.DurationPropertyFn
	EmitOpenWorkflowCount dynamicconfig.BoolPropertyFnWithNamespaceFilter
	CountWorkflowMaxQPS   dynamicconfig.IntPropertyFn
	ServiceResolver       membership.ServiceResolver
}

// Start is called to start replicator
func (r *StatsReporter) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.reporter = goro.NewHandle(context.Background()).Go(r.queryLoop)
	r.visibilityRateLimiter = quotas.NewDefaultOutgoingRateLimiter(
		func() float64 { return float64(r.CountWorkflowMaxQPS()) },
	)
	r.Logger.Info("Stats reporter started.")
}

// Stop is called to stop replicator
func (r *StatsReporter) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.reporter.Cancel()
	<-r.reporter.Done()
}

func (r *StatsReporter) queryLoop(ctx context.Context) error {
	timer := time.NewTicker(r.waitDurationTillNextReport())

	for {
		select {
		case <-timer.C:
			r.reportNamespaceStats(ctx)
			timer.Reset(r.waitDurationTillNextReport())
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}
}

func (r *StatsReporter) waitDurationTillNextReport() time.Duration {
	// BaseReportInterval is the expectation of wait across all workers.
	// BaseReportInterval*2 is the max wait across workers.
	// BaseReportInterval*2*worker_count is the total time each worker should wait.
	// Then we pick a random number within the total time to distribute the load evenly.
	totalWait := r.BaseReportInterval() * time.Duration(r.ServiceResolver.MemberCount()) * 2
	return time.Duration(math.Round(float64(totalWait) * rand.Float64()))
}

func (r *StatsReporter) reportNamespaceStats(ctx context.Context) {
	var nextPageToken []byte
	for {
		listRequest := &persistence.ListNamespacesRequest{
			PageSize:      listNamespacePageSize,
			NextPageToken: nextPageToken,
		}
		listResponse, err := r.MetadataManager.ListNamespaces(ctx, listRequest)
		if err != nil {
			r.Logger.Error("Failed to list namespace.", tag.Error(err))
			return
		}

		for _, namespace := range listResponse.Namespaces {
			if r.EmitOpenWorkflowCount(namespace.Namespace.Info.GetName()) {
				r.emitOpenWorkflowCountForNamespace(ctx, namespace.Namespace.Info.GetName(), namespace.Namespace.Info.GetId())
			}
		}

		if listResponse.NextPageToken == nil {
			return
		}
		nextPageToken = listResponse.NextPageToken
	}
}

func (r *StatsReporter) emitOpenWorkflowCountForNamespace(ctx context.Context, namespace string, namespaceID string) {
	if err := r.visibilityRateLimiter.Wait(ctx); err != nil {
		r.Logger.Error("Failed to wait for visibility rate limiter.", tag.Error(err))
		return
	}
	countRequest := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: ns.ID(namespaceID),
		Namespace:   ns.Name(namespace),
		Query:       "ExecutionStatus='Running'",
	}
	countResponse, err := r.VisibilityManager.CountWorkflowExecutions(ctx, countRequest)
	if err != nil {
		r.Logger.Warn("Failed to count workflow executions.", tag.WorkflowNamespace(namespace), tag.WorkflowNamespaceID(namespaceID), tag.Error(err))
		return
	}

	count := countResponse.Count
	r.MetricsHandler.Gauge(metrics.NamespaceOpenWorkflowsGauge.GetMetricName()).Record(float64(count), metrics.NamespaceTag(namespace))
	r.Logger.Info("Open workflow count.", tag.Counter(int(count)), tag.WorkflowNamespace(namespace), tag.WorkflowNamespaceID(namespaceID))
}
