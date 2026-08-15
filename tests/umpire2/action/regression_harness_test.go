package action

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	testmonitor "go.temporal.io/server/tests/testcore/monitor"
	"go.temporal.io/server/tests/umpire2/fact"
)

func TestResponsePolicyEmitsCanonicalCallbackFact(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
	})
	require.NoError(t, err)
	observer := &capturingFactObserver{}
	policy := NewResponsePolicy()
	policy.setFactObserver("namespace-id", observer)
	policy.setStart(&nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil)

	_, err = policy.Handler().OnStartOperation(context.Background(), "service", "operation", nil, nexus.StartOperationOptions{
		CallbackURL: "https://secret.example/callback",
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
			"authorization":                 "secret",
		},
		RequestID: "handler-request-id",
	})
	require.NoError(t, err)
	require.Len(t, observer.facts, 2)
	observed, ok := observer.facts[0].(*fact.NexusCallbackObservation)
	require.True(t, ok)
	require.Equal(t, "operation-id", observed.OperationID)
	require.NotContains(t, observed.CallbackID, "secret")
}

type capturingFactObserver struct {
	facts []umpirefw.Fact
}

func (o *capturingFactObserver) ObserveFact(_ context.Context, observed umpirefw.Fact) error {
	o.facts = append(o.facts, observed)
	return nil
}

func TestResponsePolicyDefersStartResponseUntilReleased(t *testing.T) {
	policy := NewResponsePolicy()
	policy.setDeferredStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "token"}, nil)
	type startResult struct {
		response nexus.HandlerStartOperationResult[any]
		err      error
	}
	result := make(chan startResult, 1)

	go func() {
		response, err := policy.Handler().OnStartOperation(
			context.Background(),
			"service",
			"operation",
			nil,
			nexus.StartOperationOptions{},
		)
		result <- startResult{response: response, err: err}
	}()

	select {
	case <-policy.captured:
	case <-t.Context().Done():
		require.Fail(t, "handler did not capture callback")
	}
	select {
	case <-result:
		require.Fail(t, "handler returned before release")
	default:
	}

	policy.releaseDeferredStart()
	select {
	case result := <-result:
		require.NoError(t, result.err)
		_, ok := result.response.(*nexus.HandlerStartOperationResultAsync)
		require.True(t, ok)
	case <-t.Context().Done():
		require.Fail(t, "handler did not return after release")
	}
}

func TestResponsePolicyEmitsDeferredStartResponse(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
	})
	require.NoError(t, err)
	observer := &capturingFactObserver{}
	policy := NewResponsePolicy()
	policy.setFactObserver("namespace-id", observer)
	policy.setDeferredStart(&nexus.HandlerStartOperationResultAsync{OperationToken: "operation-token"}, nil)
	result := make(chan error, 1)

	go func() {
		_, err := policy.Handler().OnStartOperation(
			context.Background(),
			"service",
			"operation",
			nil,
			nexus.StartOperationOptions{
				CallbackURL:    "https://callback",
				CallbackHeader: nexus.Header{commonnexus.CallbackTokenHeader: token},
				RequestID:      "request-id",
			},
		)
		result <- err
	}()

	select {
	case <-policy.captured:
	case <-t.Context().Done():
		require.Fail(t, "handler did not capture callback")
	}
	policy.releaseDeferredStart()
	require.NoError(t, <-result)
	require.Len(t, observer.facts, 2)
	response, ok := observer.facts[1].(*fact.NexusStartResponse)
	require.True(t, ok)
	require.Equal(t, "async_success", response.ResponseKind)
	require.NotZero(t, response.ObservedAt)
}

func TestResponsePolicyFailsOneCancellationRequest(t *testing.T) {
	policy := NewResponsePolicy()
	policy.setNextCancelError(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "cancel rejected"))
	handler := policy.Handler()

	err := handler.OnCancelOperation(context.Background(), "service", "operation", "token", nexus.CancelOperationOptions{})
	require.Error(t, err)
	require.NoError(t, handler.OnCancelOperation(context.Background(), "service", "operation", "token", nexus.CancelOperationOptions{}))
}

func TestAwaitAtomsStopsAtFirstSafetyViolation(t *testing.T) {
	monitor := &safetyViolationMonitor{
		state:      umpirefw.NewModelState(),
		violations: []umpirefw.Violation{{Rule: "test-rule", Message: "invariant failed"}},
	}
	path := &regressionPath{
		environment:  &safetyRegressionEnvironment{monitor: monitor},
		localFacts:   map[string]bool{},
		activityOps:  map[string]string{},
		activityRuns: map[string]string{},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := path.awaitAtoms(ctx, []coreregress.CompletedAtom{{Predicate: "never.true"}}, coreregress.Bindings{}, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "test-rule: invariant failed")
	require.NotErrorIs(t, err, context.DeadlineExceeded)
}

func TestNexusOperationOptionsPreserveStartToCloseTimeoutKind(t *testing.T) {
	options, err := nexusOperationOptions(coreregress.CompletedAction{
		Realization: RegressionNexusSchedule,
		Arguments: []coreregress.Argument{
			coreregress.Symbol("operation"),
			coreregress.Literal(2 * time.Second),
		},
	})
	require.NoError(t, err)
	require.Equal(t, workflow.NexusOperationOptions{StartToCloseTimeout: 2 * time.Second}, options)
}

func TestRegressionHarnessPreflightRejectsUnsupportedRealizations(t *testing.T) {
	tests := map[string]struct {
		path coreregress.CompletedPath
		want error
	}{
		"action": {path: coreregress.CompletedPath{
			Steps: []coreregress.CompletedStep{{Action: coreregress.CompletedAction{Realization: "missing.action"}}},
		}, want: coreregress.ErrMissingRealization},
		"action mode": {path: coreregress.CompletedPath{
			Steps: []coreregress.CompletedStep{{Action: coreregress.CompletedAction{Realization: RegressionObserve}, Mode: coreregress.ProactiveAction}},
		}, want: coreregress.ErrRealizationModeMismatch},
		"policy": {path: coreregress.CompletedPath{
			Steps:    []coreregress.CompletedStep{{Action: coreregress.CompletedAction{Realization: RegressionNexusCancel}}},
			Policies: []coreregress.CompletedPolicy{{Realization: "missing.policy", Start: 0, End: 1}},
		}, want: coreregress.ErrMissingRealization},
		"resource": {path: coreregress.CompletedPath{
			Resources: []coreregress.CompletedResource{{Name: "missing", Realization: "missing.resource"}},
		}, want: coreregress.ErrMissingRealization},
	}
	harness := NewRegressionHarness(nil, nil)
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := harness.Preflight(coreregress.Suite{Paths: []coreregress.CompletedPath{test.path}, PathCount: 1})
			require.ErrorIs(t, err, test.want)
			var compileErr *coreregress.CompileError
			require.ErrorAs(t, err, &compileErr)
		})
	}
}

func TestRegressionPathQualifiesMonitorSafetyByEvidenceProfile(t *testing.T) {
	path := &regressionPath{modelVersion: "model/v1", environmentProfile: umpirefw.PublicAPIProfile()}
	claims, err := path.QualifiedVerdicts(context.Background(), coreregress.QuiescenceCheckpoint, false)
	require.NoError(t, err)
	require.Equal(t, umpirefw.ClaimUnsupported, claims[0].Status)
	require.Equal(t, []string{"source:in-process"}, claims[0].Omissions)

	path.environmentProfile = umpirefw.InProcessProfile()
	claims, err = path.QualifiedVerdicts(context.Background(), coreregress.QuiescenceCheckpoint, false)
	require.NoError(t, err)
	require.Equal(t, umpirefw.ClaimEstablished, claims[0].Status)
}

func TestObserveWorkflowRunIDGroundsServerMintedValue(t *testing.T) {
	bindings := coreregress.Bindings{"handler": "handler-id"}
	atom := coreregress.CompletedAtom{
		Predicate: "workflow.run_id",
		Arguments: []coreregress.Argument{
			coreregress.Symbol("handler"),
			coreregress.Symbol("run"),
		},
	}
	response := &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "handler-id", RunId: "run-id"},
	}}

	require.True(t, observeWorkflowRunID(atom, bindings, response))
	require.Equal(t, coreregress.Bindings{"handler": "handler-id", "run": "run-id"}, bindings)
}

type safetyRegressionEnvironment struct {
	RegressionEnvironment
	monitor testmonitor.Monitor
}

func (*safetyRegressionEnvironment) NamespaceID() namespace.ID { return namespace.ID("namespace-id") }
func (e *safetyRegressionEnvironment) GetMonitor() testmonitor.Monitor {
	return e.monitor
}

type safetyViolationMonitor struct {
	testmonitor.Monitor
	state      *umpirefw.ModelState
	violations []umpirefw.Violation
}

func (m *safetyViolationMonitor) ModelState() *umpirefw.ModelState { return m.state }
func (m *safetyViolationMonitor) CheckNamespaceSafety(context.Context, string) []umpirefw.Violation {
	return m.violations
}
