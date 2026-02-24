package visibility

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
)

const (
	RateLimitDefaultToken = 1
)

var _ manager.VisibilityManager = (*visibilityManagerRateLimited)(nil)

type visibilityManagerRateLimited struct {
	delegate         manager.VisibilityManager
	readRateLimiter  quotas.RequestRateLimiter
	writeRateLimiter quotas.RequestRateLimiter
}

func NewVisibilityManagerRateLimited(
	delegate manager.VisibilityManager,
	readMaxQPS dynamicconfig.IntPropertyFn,
	writeMaxQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
) *visibilityManagerRateLimited {
	return &visibilityManagerRateLimited{
		delegate:         delegate,
		readRateLimiter:  newPriorityRateLimiter(readMaxQPS, operatorRPSRatio),
		writeRateLimiter: newPriorityRateLimiter(writeMaxQPS, operatorRPSRatio),
	}
}

func (m *visibilityManagerRateLimited) Close() {
	m.delegate.Close()
}

func (m *visibilityManagerRateLimited) GetReadStoreName(nsName namespace.Name) string {
	return m.delegate.GetReadStoreName(nsName)
}

func (m *visibilityManagerRateLimited) GetStoreNames() []string {
	return m.delegate.GetStoreNames()
}

func (m *visibilityManagerRateLimited) HasStoreName(stName string) bool {
	return m.delegate.HasStoreName(stName)
}

func (m *visibilityManagerRateLimited) GetIndexName() string {
	return m.delegate.GetIndexName()
}

func (m *visibilityManagerRateLimited) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return m.delegate.ValidateCustomSearchAttributes(searchAttributes)
}

// Below are write APIs.

func (m *visibilityManagerRateLimited) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	if ok := allow(ctx, "RecordWorkflowExecutionStarted", m.writeRateLimiter); !ok {
		return persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.RecordWorkflowExecutionStarted(ctx, request)
}

func (m *visibilityManagerRateLimited) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	if ok := allow(ctx, "RecordWorkflowExecutionClosed", m.writeRateLimiter); !ok {
		return persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.RecordWorkflowExecutionClosed(ctx, request)
}

func (m *visibilityManagerRateLimited) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	if ok := allow(ctx, "UpsertWorkflowExecution", m.writeRateLimiter); !ok {
		return persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.UpsertWorkflowExecution(ctx, request)
}

func (m *visibilityManagerRateLimited) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	if ok := allow(ctx, "DeleteWorkflowExecution", m.writeRateLimiter); !ok {
		return persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.DeleteWorkflowExecution(ctx, request)
}

// Below are read APIs.
func (m *visibilityManagerRateLimited) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := allow(ctx, "ListWorkflowExecutions", m.readRateLimiter); !ok {
		return nil, persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.ListWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	if ok := allow(ctx, "ListChasmExecutions", m.readRateLimiter); !ok {
		return nil, persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.ListChasmExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	if ok := allow(ctx, "CountWorkflowExecutions", m.readRateLimiter); !ok {
		return nil, persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.CountWorkflowExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) CountChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	if ok := allow(ctx, "CountChasmExecutions", m.readRateLimiter); !ok {
		return nil, persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.CountChasmExecutions(ctx, request)
}

func (m *visibilityManagerRateLimited) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	if ok := allow(ctx, "GetWorkflowExecution", m.readRateLimiter); !ok {
		return nil, persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.GetWorkflowExecution(ctx, request)
}

func (m *visibilityManagerRateLimited) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	if ok := allow(ctx, "AddSearchAttributes", m.readRateLimiter); !ok {
		return persistence.ErrPersistenceSystemLimitExceeded
	}
	return m.delegate.AddSearchAttributes(ctx, request)
}

func allow(
	ctx context.Context,
	api string,
	rateLimiter quotas.RequestRateLimiter,
) bool {
	callerInfo := headers.GetCallerInfo(ctx)
	// Currently only CallerType is used. See common/persistence/visibility/quotas.go for rate limiter details.
	return rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		api,
		RateLimitDefaultToken,
		callerInfo.CallerName,
		callerInfo.CallerType,
		-1,
		callerInfo.CallOrigin,
	))
}
