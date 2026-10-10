package visibility

import (
	"context"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

var _ manager.VisibilityManager = (*visibilityManagerMetrics)(nil)
var _ manager.AdminVisibilityManager = (*visibilityManagerMetrics)(nil)

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
	return writeMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope,
		m.delegate.RecordWorkflowExecutionStarted,
		request,
	)
}

func (m *visibilityManagerMetrics) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	return writeMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope,
		m.delegate.RecordWorkflowExecutionClosed,
		request,
	)
}

func (m *visibilityManagerMetrics) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	return writeMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceUpsertWorkflowExecutionScope,
		m.delegate.UpsertWorkflowExecution,
		request,
	)
}

func (m *visibilityManagerMetrics) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	return writeMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceDeleteWorkflowExecutionScope,
		m.delegate.DeleteWorkflowExecution,
		request,
	)
}

func (m *visibilityManagerMetrics) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceListWorkflowExecutionsScope,
		m.delegate.ListWorkflowExecutions,
		request,
		request.Namespace,
		request.Query,
	)
}

func (m *visibilityManagerMetrics) ListChasmExecutions(
	ctx context.Context,
	request *visibilityservice.ListChasmExecutionsRequest,
) (*visibilityservice.ListChasmExecutionsResponse, error) {
	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceListChasmExecutionsScope,
		m.delegate.ListChasmExecutions,
		request,
		namespace.Name(request.Namespace),
		request.Query,
	)
}

func (m *visibilityManagerMetrics) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceCountWorkflowExecutionsScope,
		m.delegate.CountWorkflowExecutions,
		request,
		request.Namespace,
		request.Query,
	)
}

func (m *visibilityManagerMetrics) CountChasmExecutions(
	ctx context.Context,
	request *visibilityservice.CountChasmExecutionsRequest,
) (*visibilityservice.CountChasmExecutionsResponse, error) {
	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceCountChasmExecutionsScope,
		m.delegate.CountChasmExecutions,
		request,
		namespace.Name(request.Namespace),
		request.Query,
	)
}

func (m *visibilityManagerMetrics) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceGetWorkflowExecutionScope,
		m.delegate.GetWorkflowExecution,
		request,
		request.Namespace,
		"",
	)
}

func (m *visibilityManagerMetrics) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	return writeMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceAddSearchAttributesScope,
		m.delegate.AddSearchAttributes,
		request,
	)
}

// ListExecutions implements [manager.AdminVisibilityManager].
func (m *visibilityManagerMetrics) ListExecutions(
	ctx context.Context,
	request *manager.AdminListExecutionsRequest,
) (*manager.AdminListExecutionsResponse, error) {
	adminManager, ok := m.delegate.(manager.AdminVisibilityManager)
	if !ok {
		return nil, manager.ErrNotAdminVisibilityManager
	}

	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceListExecutionsScope,
		adminManager.ListExecutions,
		request,
		request.Namespace,
		request.Query,
	)
}

// CountExecutions implements [manager.AdminVisibilityManager].
func (m *visibilityManagerMetrics) CountExecutions(
	ctx context.Context,
	request *manager.AdminCountExecutionsRequest,
) (*manager.AdminCountExecutionsResponse, error) {
	adminManager, ok := m.delegate.(manager.AdminVisibilityManager)
	if !ok {
		return nil, manager.ErrNotAdminVisibilityManager
	}

	return readMethodWrapper(
		ctx,
		m,
		metrics.VisibilityPersistenceCountExecutionsScope,
		adminManager.CountExecutions,
		request,
		request.Namespace,
		request.Query,
	)
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

func readMethodWrapper[RequestT any, ResponseT any](
	ctx context.Context,
	m *visibilityManagerMetrics,
	operation string,
	method func(context.Context, *RequestT) (*ResponseT, error),
	request *RequestT,
	namespaceName namespace.Name,
	query string,
) (*ResponseT, error) {
	handler, startTime := m.tagScope(operation)
	response, err := method(ctx, request)
	elapsed := time.Since(startTime)
	if elapsed > m.slowQueryThreshold() {
		m.logger.Warn(
			"visibility operation latency exceeded threshold",
			tag.Operation(operation),
			tag.Duration("duration", elapsed),
			tag.String("visibility-query", query),
			tag.Stringer("namespace", namespaceName),
		)
	}
	metrics.VisibilityPersistenceLatency.With(handler).Record(elapsed)
	return response, m.updateErrorMetric(handler, err)
}

func writeMethodWrapper[RequestT any](
	ctx context.Context,
	m *visibilityManagerMetrics,
	operation string,
	method func(context.Context, *RequestT) error,
	request *RequestT,
) error {
	handler, startTime := m.tagScope(operation)
	err := method(ctx, request)
	elapsed := time.Since(startTime)
	metrics.VisibilityPersistenceLatency.With(handler).Record(elapsed)
	metrics.ContextCounterAdd(ctx, metrics.TaskPersistenceLatency.Name(), elapsed.Nanoseconds())
	return m.updateErrorMetric(handler, err)
}
