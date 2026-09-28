package matching

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
)

var _ matchingservice.MatchingServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          matchingservice.MatchingServiceClient
	metricsHandler  metrics.Handler
	logger          log.Logger
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of matchingservice.MatchingServiceClient that emits metrics
func NewMetricClient(
	client matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
	throttledLogger log.Logger,
) matchingservice.MatchingServiceClient {
	return &metricClient{
		client:          client,
		metricsHandler:  metricsHandler,
		logger:          logger,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) emitForwardedSourceStats(
	metricsHandler metrics.Handler,
	forwardedFrom string,
	taskQueue *taskqueuepb.TaskQueue,
) {
	if taskQueue == nil {
		return
	}

	switch {
	case forwardedFrom != "":
		metrics.MatchingClientForwardedCounter.With(metricsHandler).Record(1)
	default:
		// TODO: confirmed from metrics, it seems this error does happen at the moment...
		// it means some mangled name come here; need to check why
		_, err := tqid.NewTaskQueueFamily("", taskQueue.GetName())
		if err != nil {
			c.logger.Info("invalid tq name", tag.Error(err), tag.String("proto", taskQueue.GetName()))
			metrics.MatchingClientInvalidTaskQueueName.With(metricsHandler).Record(1)
		}
	}
}

func (c *metricClient) startMetricsRecording(
	ctx context.Context,
	operation string,
) (metrics.Handler, time.Time) {
	caller := headers.GetCallerInfo(ctx).CallerName
	handler := c.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller), metrics.ServiceRoleTag(metrics.MatchingRoleTagValue))
	metrics.ClientRequests.With(handler).Record(1)
	return handler, time.Now().UTC()
}

func (c *metricClient) finishMetricsRecording(
	metricsHandler metrics.Handler,
	startTime time.Time,
	err error,
) {
	if err != nil {
		switch err.(type) {
		case *serviceerrors.StickyWorkerUnavailable,
			*serviceerror.Canceled,
			*serviceerror.DeadlineExceeded,
			*serviceerror.NotFound,
			*serviceerror.QueryFailed,
			*serviceerror.NamespaceNotFound,
			*serviceerror.NewerBuildExists,
			*serviceerror.WorkflowExecutionAlreadyStarted,
			*serviceerror.ResourceExhausted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("matching client encountered error", tag.Error(err), tag.ServiceErrorType(err))
		}
		metrics.ClientFailures.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))
	}
	metrics.ClientLatency.With(metricsHandler).Record(time.Since(startTime))
}

func (c *metricClient) Route(p tqid.Partition) (string, error) {
	// Ideally we wouldn't do a type-check here and require c.client to have
	// Route, but it would require changing too many types all over the place.
	// This isn't called in a hot path.
	rc, ok := c.client.(RoutingClient)
	if !ok {
		return "", errors.New("not routing client")
	}
	return rc.Route(p)
}

// Stop forwards a deterministic shutdown to the wrapped client. See
// clientImpl.Stop. It is only invoked via client.Bean.Close.
func (c *metricClient) Stop() {
	if s, ok := c.client.(interface{ Stop() }); ok {
		s.Stop()
	}
}
