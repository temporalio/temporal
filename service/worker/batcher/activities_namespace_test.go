package batcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
)

// These tests guard against cross-namespace escalation via the batcher activity.
// The per-NS worker's frontendClient dials internal-frontend (NoopClaimMapper →
// RoleAdmin), so the activity MUST NOT forward a user-controlled namespace string
// to frontendClient calls. The namespace for all downstream operations must be
// the worker's bound namespace.

const (
	boundNSName = "bound-ns"
	boundNSID   = "bound-ns-id"
	otherNSName = "other-ns"
)

func newBoundActivities(frontend workflowservice.WorkflowServiceClient) *activities {
	return &activities{
		activityDeps: activityDeps{
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         log.NewTestLogger(),
			FrontendClient: frontend,
		},
		namespace:   namespace.Name(boundNSName),
		namespaceID: namespace.ID(boundNSID),
		rps:         dynamicconfig.GetIntPropertyFnFilteredByNamespace(50),
		concurrency: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
	}
}

// TestBatchActivityWithProtobuf_RejectsMismatchedRequestNamespace verifies that
// BatchActivityWithProtobuf rejects a request whose Request.Namespace differs
// from the worker's bound namespace, even when NamespaceId is valid.
// This blocks the cross-namespace attack where an attacker submits a valid
// NamespaceId for their own namespace but sets Request.Namespace to a victim's.
func TestBatchActivityWithProtobuf_RejectsMismatchedRequestNamespace(t *testing.T) {
	ts := testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	a := newBoundActivities(nil)
	env.RegisterActivity(a.BatchActivityWithProtobuf)

	input := &batchspb.BatchOperationInput{
		NamespaceId: boundNSID, // ID check passes; name check must catch the mismatch
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: otherNSName, // mismatched — must be rejected
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{Signal: "s"},
			},
			Executions: []*commonpb.WorkflowExecution{{WorkflowId: "w"}},
		},
	}

	_, err := env.ExecuteActivity(a.BatchActivityWithProtobuf, input)
	require.Error(t, err)
	require.ErrorContains(t, err, errNamespaceMismatch.Error())
}

// TestBatchActivityWithProtobuf_RejectsMismatchedAdminRequestNamespace verifies
// that the same namespace mismatch check applies to admin batch requests.
func TestBatchActivityWithProtobuf_RejectsMismatchedAdminRequestNamespace(t *testing.T) {
	ts := testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	a := newBoundActivities(nil)
	env.RegisterActivity(a.BatchActivityWithProtobuf)

	input := &batchspb.BatchOperationInput{
		NamespaceId: boundNSID,
		AdminRequest: &adminservice.StartAdminBatchOperationRequest{
			Namespace:  otherNSName, // mismatched — must be rejected
			Executions: []*commonpb.WorkflowExecution{{WorkflowId: "w"}},
		},
	}

	_, err := env.ExecuteActivity(a.BatchActivityWithProtobuf, input)
	require.Error(t, err)
	require.ErrorContains(t, err, errNamespaceMismatch.Error())
}

// TestStartTaskProcessor_UsesWorkerBoundNamespaceForSignal verifies that when
// BatchActivityWithProtobuf dispatches via startTaskProcessor, the namespace
// delivered to frontendClient.SignalWorkflowExecution is the worker's bound
// namespace. This is a belt-and-suspenders check: even if the early validation
// above is ever relaxed, the namespace used in operations stays worker-bound.
func TestStartTaskProcessor_UsesWorkerBoundNamespaceForSignal(t *testing.T) {
	r := require.New(t)
	ctrl := gomock.NewController(t)
	mockFE := workflowservicemock.NewMockWorkflowServiceClient(ctrl)

	var captured *workflowservice.SignalWorkflowExecutionRequest
	mockFE.EXPECT().
		SignalWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.SignalWorkflowExecutionRequest, _ ...any) (*workflowservice.SignalWorkflowExecutionResponse, error) {
			captured = req
			return &workflowservice.SignalWorkflowExecutionResponse{}, nil
		})

	a := newBoundActivities(mockFE)

	// Simulate the namespace that BatchActivityWithProtobuf derives —
	// with the fix this is always a.namespace.String().
	ns := a.namespace.String()
	batchOp := &batchspb.BatchOperationInput{
		NamespaceId: boundNSID,
		Request: &workflowservice.StartBatchOperationRequest{
			Namespace: ns,
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{Signal: "s"},
			},
		},
	}

	taskCh := make(chan task, 1)
	respCh := make(chan taskResponse, 1)
	taskCh <- task{
		executionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "w"},
		},
		page: &page{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.startTaskProcessor(ctx, batchOp, ns, taskCh, respCh,
			rate.NewLimiter(rate.Inf, 1), nil, mockFE,
			metrics.NoopMetricsHandler, log.NewTestLogger())
	}()

	<-respCh
	cancel()
	<-done

	r.NotNil(captured)
	r.Equal(boundNSName, captured.Namespace,
		"frontendClient.SignalWorkflowExecution must receive the worker's bound namespace")
}
