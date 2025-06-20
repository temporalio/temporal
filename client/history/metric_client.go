package history

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

var _ historyservice.HistoryServiceClient = (*metricClient)(nil)

type metricClient struct {
	client          historyservice.HistoryServiceClient
	metricsHandler  metrics.Handler
	logger          log.Logger
	throttledLogger log.Logger
}

// NewMetricClient creates a new instance of historyservice.HistoryServiceClient that emits metrics
func NewMetricClient(
	client historyservice.HistoryServiceClient,
	metricsHandler metrics.Handler,
	logger log.Logger,
	throttledLogger log.Logger,
) historyservice.HistoryServiceClient {
	return &metricClient{
		client:          client,
		metricsHandler:  metricsHandler,
		logger:          logger,
		throttledLogger: throttledLogger,
	}
}

func (c *metricClient) StreamWorkflowReplicationMessages(
	ctx context.Context,
	opts ...grpc.CallOption,
) (_ historyservice.HistoryService_StreamWorkflowReplicationMessagesClient, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.HistoryClientStreamWorkflowReplicationMessagesScope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	return c.client.StreamWorkflowReplicationMessages(ctx, opts...)
}

func (c *metricClient) startMetricsRecording(
	ctx context.Context,
	operation string,
) (metrics.Handler, time.Time) {
	caller := headers.GetCallerInfo(ctx).CallerName
	metricsHandler := c.metricsHandler.WithTags(metrics.OperationTag(operation), metrics.NamespaceTag(caller), metrics.ServiceRoleTag(metrics.HistoryRoleTagValue))
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
			*serviceerror.WorkflowExecutionAlreadyStarted:
			// noop - not interest and too many logs
		default:
			c.throttledLogger.Info("history client encountered error", tag.Error(err), tag.ServiceErrorType(err))
		}
		metrics.ClientFailures.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))
	}
	metrics.ClientLatency.With(metricsHandler).Record(time.Since(startTime))
}
