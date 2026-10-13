package operator

import (
	"context"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ operatorservice.OperatorServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          operatorservice.OperatorServiceClient
	metricsHandler  metrics.Handler
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of operatorservice.OperatorServiceClient that emits metrics
func NewMetricClient(
	client operatorservice.OperatorServiceClient,
	metricsHandler metrics.Handler,
	throttledLogger log.Logger,
) operatorservice.OperatorServiceClient {
	return &metricClient{
		client:          client,
		metricsHandler:  metricsHandler,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) startMetricsRecording(
	ctx context.Context,
	operation string,
) (metrics.Handler, time.Time) {
	caller := headers.GetCallerInfo(ctx).CallerName
	metricsHandler := c.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller), metrics.ServiceRoleTag(metrics.OperatorRoleTagValue))
	metrics.ClientRequests.With(metricsHandler).Record(1)
	return metricsHandler, time.Now().UTC()
}

func (c *metricClient) finishMetricsRecording(
	metricsHandler metrics.Handler,
	startTime time.Time,
	err error,
) {
	if err != nil {
		switch err.(type) {
		case *serviceerror.Canceled,
			*serviceerror.DeadlineExceeded,
			*serviceerror.NotFound,
			*serviceerror.QueryFailed,
			*serviceerror.NamespaceNotFound,
			*serviceerror.WorkflowNotReady,
			*serviceerror.WorkflowExecutionAlreadyStarted,
			*serviceerror.ResourceExhausted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("operator client encountered error", tag.Error(err), tag.ServiceErrorType(err))
		}
		metrics.ClientFailures.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))
	}
	metrics.ClientLatency.With(metricsHandler).Record(time.Since(startTime))
}
