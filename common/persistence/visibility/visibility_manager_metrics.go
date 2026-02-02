package visibility

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

var _ manager.VisibilityManager = (*visibilityManagerMetrics)(nil)

type visibilityManagerMetrics struct {
	metricHandler metrics.Handler
	logger        log.Logger
	delegate      manager.VisibilityManager

	slowQueryThreshold             dynamicconfig.DurationPropertyFn
	visibilityPluginNameMetricsTag metrics.Tag
	visibilityIndexNameMetricsTag  metrics.Tag
}

func NewVisibilityManagerMetrics(
	delegate manager.VisibilityManager,
	metricHandler metrics.Handler,
	logger log.Logger,
	slowQueryThreshold dynamicconfig.DurationPropertyFn,
	visibilityPluginNameMetricsTag metrics.Tag,
	visibilityIndexNameMetricsTag metrics.Tag,
) *visibilityManagerMetrics {
	return &visibilityManagerMetrics{
		metricHandler: metricHandler,
		logger:        logger,
		delegate:      delegate,

		slowQueryThreshold:             slowQueryThreshold,
		visibilityPluginNameMetricsTag: visibilityPluginNameMetricsTag,
		visibilityIndexNameMetricsTag:  visibilityIndexNameMetricsTag,
	}
}

func (m *visibilityManagerMetrics) Close() {
	m.delegate.Close()
}

func (m *visibilityManagerMetrics) GetReadStoreName(nsName namespace.Name) string {
	return m.delegate.GetReadStoreName(nsName)
}

func (m *visibilityManagerMetrics) GetStoreNames() []string {
	return m.delegate.GetStoreNames()
}

func (m *visibilityManagerMetrics) HasStoreName(stName string) bool {
	return m.delegate.HasStoreName(stName)
}

func (m *visibilityManagerMetrics) GetIndexName() string {
	return m.delegate.GetIndexName()
}

func (m *visibilityManagerMetrics) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return m.delegate.ValidateCustomSearchAttributes(searchAttributes)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope)
	err := m.delegate.RecordWorkflowExecutionStarted(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope)
	err := m.delegate.RecordWorkflowExecutionClosed(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceUpsertWorkflowExecutionScope)
	err := m.delegate.UpsertWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope)
	err := m.delegate.DeleteWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceListWorkflowExecutionsScope)
	response, err := m.delegate.ListWorkflowExecutions(ctx, request)
	elapsed := time.Since(startTime)
	if elapsed > m.slowQueryThreshold() {
		m.logger.Warn("List query exceeded threshold",
			tag.NewDurationTag("duration", elapsed),
			tag.NewStringTag("visibility-query", request.Query),
			tag.NewStringerTag("namespace", request.Namespace),
		)
	}
	metrics.VisibilityPersistenceLatency.With(handler).Record(elapsed)
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceListChasmExecutionsScope)
	response, err := m.delegate.ListChasmExecutions(ctx, request)
	elapsed := time.Since(startTime)
	if elapsed > m.slowQueryThreshold() {
		m.logger.Warn("List query exceeded threshold",
			tag.NewDurationTag("duration", elapsed),
			tag.NewStringTag("visibility-query", request.Query),
			tag.NewStringerTag("namespace", request.Namespace),
		)
	}
	metrics.VisibilityPersistenceLatency.With(handler).Record(elapsed)
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceCountWorkflowExecutionsScope)
	response, err := m.delegate.CountWorkflowExecutions(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) CountChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceCountChasmExecutionsScope)
	response, err := m.delegate.CountChasmExecutions(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceGetWorkflowExecutionScope)
	response, err := m.delegate.GetWorkflowExecution(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return response, m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	handler, startTime := m.tagScope(metrics.VisibilityPersistenceAddSearchAttributesScope)
	err := m.delegate.AddSearchAttributes(ctx, request)
	metrics.VisibilityPersistenceLatency.With(handler).Record(time.Since(startTime))
	return m.updateErrorMetric(handler, err)
}

func (m *visibilityManagerMetrics) tagScope(operation string) (metrics.Handler, time.Time) {
	taggedHandler := m.metricHandler.WithTags(metrics.OperationTag(operation), m.visibilityPluginNameMetricsTag, m.visibilityIndexNameMetricsTag)
	metrics.VisibilityPersistenceRequests.With(taggedHandler).Record(1)
	return taggedHandler, time.Now().UTC()
}

func (m *visibilityManagerMetrics) updateErrorMetric(handler metrics.Handler, err error) error {
	if err == nil {
		return nil
	}

	metrics.VisibilityPersistenceErrorWithType.With(handler).Record(1, metrics.ServiceErrorTypeTag(err))
	switch err := err.(type) {
	case *serviceerror.InvalidArgument,
		*persistence.TimeoutError,
		*persistence.ConditionFailedError,
		*serviceerror.NotFound:
		// no-op

	case *serviceerror.ResourceExhausted:
		metrics.VisibilityPersistenceResourceExhausted.With(handler).Record(
			1, metrics.ResourceExhaustedCauseTag(err.Cause), metrics.ResourceExhaustedScopeTag(err.Scope))
	default:
		m.logger.Error("Operation failed with an error.", tag.Error(err))
		metrics.VisibilityPersistenceFailures.With(handler).Record(1)
	}

	return err
}
