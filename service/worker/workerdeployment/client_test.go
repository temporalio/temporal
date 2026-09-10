package workerdeployment

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.uber.org/mock/gomock"
)

// newAutoCreateRateLimiter builds the same per-namespace limiter used in production
// (see fx.go) but with a caller-controlled RPS so tests can exhaust it deterministically.
func newAutoCreateRateLimiter(rps float64) quotas.RequestRateLimiter {
	return quotas.NewNamespaceRequestRateLimiter(func(req quotas.Request) quotas.RequestRateLimiter {
		return quotas.NewRequestRateLimiterAdapter(
			quotas.NewDefaultIncomingRateLimiter(func() float64 { return rps }),
		)
	})
}

// drain exhausts the limiter's burst for the given namespace so the next Allow is denied.
func drain(limiter quotas.RequestRateLimiter, nsName string) {
	req := quotas.NewRequest("countWorkerDeployments", 1, nsName, "", 0, "")
	for limiter.Allow(time.Now(), req) {
	}
}

func newRateLimitTestClient(t *testing.T, vis manager.VisibilityManager, history historyservice.HistoryServiceClient, limiter quotas.RequestRateLimiter) *ClientImpl {
	t.Helper()
	return &ClientImpl{
		historyClient:                   history,
		visibilityManager:               vis,
		maxIDLengthLimit:                func() int { return testMaxIDLengthLimit },
		maxDeployments:                  func(string) int { return 0 }, // any count >= 0 trips the limit
		autoCreateVisibilityRateLimiter: limiter,
		metricsHandler:                  metrics.NoopMetricsHandler,
	}
}

// expectDeploymentNotFound makes workerDeploymentExists report the deployment as new.
func expectDeploymentNotFound(history *historyservicemock.MockHistoryServiceClient) {
	history.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, &serviceerror.NotFound{}).AnyTimes()
}

func callRegister(d *ClientImpl, ns *namespace.Namespace, requestID string) error {
	_, err := d.updateWithStartWorkerDeployment(
		context.Background(),
		ns,
		testDeployment,
		nil,
		"test-identity",
		requestID,
		0,
	)
	return err
}

// When the per-namespace limiter is exhausted, an auto-create request is rejected with a
// retryable ResourceExhausted error and the expensive visibility count is never run.
func TestAutoCreateRateLimited_DeniesAndSkipsVisibility(t *testing.T) {
	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, testNamespace)
	vis := manager.NewMockVisibilityManager(controller)
	history := historyservicemock.NewMockHistoryServiceClient(controller)
	expectDeploymentNotFound(history)
	// The visibility count must NOT be called on the rate-limited path.
	vis.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Times(0)

	limiter := newAutoCreateRateLimiter(1.0)
	drain(limiter, ns.Name().String())

	d := newRateLimitTestClient(t, vis, history, limiter)
	err := callRegister(d, ns, AutoCreateRequestIDPrefix+"abc")

	var resourceExhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &resourceExhausted)
	require.Contains(t, err.Error(), "rate limited")
}

// When the limiter has capacity, an auto-create request proceeds to the visibility count
// (here the count trips the deployment limit, proving the query ran).
func TestAutoCreateRateLimited_AllowsReachesVisibility(t *testing.T) {
	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, testNamespace)
	vis := manager.NewMockVisibilityManager(controller)
	history := historyservicemock.NewMockHistoryServiceClient(controller)
	expectDeploymentNotFound(history)
	vis.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).
		Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil).Times(1)

	d := newRateLimitTestClient(t, vis, history, newAutoCreateRateLimiter(1.0))
	err := callRegister(d, ns, AutoCreateRequestIDPrefix+"abc")

	var resourceExhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &resourceExhausted)
	require.Contains(t, err.Error(), "reached maximum deployments")
}

// Explicit user APIs use UUID request IDs (no auto-create prefix) and must bypass the
// limiter entirely, reaching the visibility count even when the limiter is exhausted.
func TestAutoCreateRateLimited_NonAutoCreateBypassesLimiter(t *testing.T) {
	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, testNamespace)
	vis := manager.NewMockVisibilityManager(controller)
	history := historyservicemock.NewMockHistoryServiceClient(controller)
	expectDeploymentNotFound(history)
	vis.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).
		Return(&manager.CountWorkflowExecutionsResponse{Count: 0}, nil).Times(1)

	limiter := newAutoCreateRateLimiter(1.0)
	drain(limiter, ns.Name().String())

	d := newRateLimitTestClient(t, vis, history, limiter)
	err := callRegister(d, ns, uuid.NewString())

	var resourceExhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &resourceExhausted)
	require.Contains(t, err.Error(), "reached maximum deployments")
}
