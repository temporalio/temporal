package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

const (
	nexusBatchIdentity = "batch-identity"
	nexusBatchReason   = "batch-reason"
	// Distinct from nexusBatchReason so assertions catch the job reason being forwarded instead of the op's.
	nexusBatchJobReason = "batch-job-reason"
	// Identity used to close an operation before the batch runs, so a batch
	// that skips it can be told apart from one that touched it.
	nexusPreBatchIdentity = "pre-batch-identity"
)

type NexusStandaloneBatchTerminateTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneBatchTerminateTestSuite]
}

func TestNexusStandaloneBatchTerminateTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneBatchTerminateTestSuite{})
}

// newNexusStandaloneBatchEnv builds a standalone Nexus operation env with the
// batcher worker running and Nexus batch operations gated by enabled.
func newNexusStandaloneBatchEnv(t *testing.T, enabled bool) *NexusTestEnv {
	return newNexusTestEnv(t, true,
		testcore.WithWorkerService("batch operations"),
		// The default per-namespace limit is 1; tests may start several batches.
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableBatchOperationsForStandaloneNexusOperations, enabled),
	)
}

type startedNexusOperation struct {
	operationID string
	runID       string
}

func nexusEndpointQuery(endpoint string) string {
	return fmt.Sprintf("Endpoint = '%s'", endpoint)
}

// newNexusBatchRequest returns a batch request without its Operation set.
func newNexusBatchRequest(env *NexusTestEnv, jobID string) *workflowservice.StartBatchOperationRequest {
	return &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		JobId:     jobID,
		Reason:    nexusBatchJobReason,
	}
}

// startBatchTarget starts an operation against endpoint. Endpoints from
// createRandomNexusEndpoint target a task queue with no poller, so the
// operation never starts and stays running.
func (env *NexusTestEnv) startBatchTarget(s parallelsuite.Scope, endpoint, operationID string) startedNexusOperation {
	resp, err := env.startNexusOperation(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: operationID,
		Endpoint:    endpoint,
	})
	s.NoError(err)
	return startedNexusOperation{operationID: operationID, runID: resp.GetRunId()}
}

// startRunningAndClosedBatchTargets starts two operations against endpoint and
// terminates one of them, then waits for visibility to settle on both statuses.
func (env *NexusTestEnv) startRunningAndClosedBatchTargets(s parallelsuite.Scope, endpoint string) (running, closed startedNexusOperation) {
	running = env.startBatchTarget(s, endpoint, "running-op")
	closed = env.startBatchTarget(s, endpoint, "closed-op")
	_, err := env.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: closed.operationID,
		RunId:       closed.runID,
		RequestId:   uuid.NewString(),
		Identity:    nexusPreBatchIdentity,
	})
	s.NoError(err)

	parallelsuite.Await(s, func(s parallelsuite.Scope) {
		listResp, err := env.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     nexusEndpointQuery(endpoint),
		})
		s.NoError(err)
		statuses := make(map[string]enumspb.NexusOperationExecutionStatus, len(listResp.GetOperations()))
		for _, op := range listResp.GetOperations() {
			statuses[op.GetOperationId()] = op.GetStatus()
		}
		s.Equal(map[string]enumspb.NexusOperationExecutionStatus{
			running.operationID: enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING,
			closed.operationID:  enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED,
		}, statuses)
	}, testcore.WaitForESToSettle, 100*time.Millisecond)
	return running, closed
}

// awaitBatchCompleted waits for the batch to complete having processed wantTargets operations.
func (env *NexusTestEnv) awaitBatchCompleted(s parallelsuite.Scope, jobID string, wantTargets int64) {
	parallelsuite.Await(s, func(s parallelsuite.Scope) {
		desc, err := env.FrontendClient().DescribeBatchOperation(s.Context(), &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		s.NoError(err)
		s.Equal(enumspb.BATCH_OPERATION_STATE_COMPLETED, desc.GetState())
		s.Equal(wantTargets, desc.GetTotalOperationCount())
		s.Equal(wantTargets, desc.GetCompleteOperationCount())
	}, 10*time.Second, 100*time.Millisecond)
}

// requireUntouchedByBatch verifies op still reflects only its pre-batch termination.
func (env *NexusTestEnv) requireUntouchedByBatch(s parallelsuite.Scope, op startedNexusOperation) {
	resp := env.describeNexusOperation(s.Context(), s.TB(), op.operationID)
	s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, resp.GetInfo().GetStatus())
	s.Equal(nexusPreBatchIdentity, resp.GetFailure().GetTerminatedFailureInfo().GetIdentity())
	s.Nil(resp.GetInfo().GetCancellationInfo())
}

// nexusBatchTargetSelector scopes a batch to ops: by visibility query, the
// deprecated Executions field, or TargetExecutions. It returns the query and
// executions DescribeBatchOperation is expected to report.
type nexusBatchTargetSelector struct {
	name  string
	apply func(s parallelsuite.Scope, env *NexusTestEnv, endpoint string, ops []startedNexusOperation, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution)
}

func nexusBatchTargetSelectors() []nexusBatchTargetSelector {
	targetExecutions := func(ops []startedNexusOperation) []*commonpb.Execution {
		executions := make([]*commonpb.Execution, 0, len(ops))
		for _, op := range ops {
			executions = append(executions, &commonpb.Execution{
				Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
				BusinessId: op.operationID,
				RunId:      op.runID,
			})
		}
		return executions
	}
	return []nexusBatchTargetSelector{
		{
			name: "VisibilityQuery",
			apply: func(s parallelsuite.Scope, env *NexusTestEnv, endpoint string, ops []startedNexusOperation, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				query := nexusEndpointQuery(endpoint)
				parallelsuite.Await(s, func(s parallelsuite.Scope) {
					resp, err := env.FrontendClient().ListNexusOperationExecutions(s.Context(), &workflowservice.ListNexusOperationExecutionsRequest{
						Namespace: env.Namespace().String(),
						Query:     query,
					})
					s.NoError(err)
					s.Require().Len(resp.GetOperations(), len(ops))
				}, testcore.WaitForESToSettle, 100*time.Millisecond)
				req.VisibilityQuery = query
				return query, nil
			},
		},
		{
			name: "Executions",
			apply: func(s parallelsuite.Scope, env *NexusTestEnv, endpoint string, ops []startedNexusOperation, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				legacy := make([]*commonpb.WorkflowExecution, 0, len(ops))
				for _, op := range ops {
					legacy = append(legacy, &commonpb.WorkflowExecution{WorkflowId: op.operationID, RunId: op.runID})
				}
				//nolint:staticcheck // Exercises the deprecated field, which is treated as Nexus targets.
				req.Executions = legacy
				return "", targetExecutions(ops)
			},
		},
		{
			name: "TargetExecutions",
			apply: func(s parallelsuite.Scope, env *NexusTestEnv, endpoint string, ops []startedNexusOperation, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				req.TargetExecutions = targetExecutions(ops)
				return "", req.TargetExecutions
			},
		},
	}
}

func setNexusBatchTerminateOperation(req *workflowservice.StartBatchOperationRequest) {
	req.Operation = &workflowservice.StartBatchOperationRequest_TerminateNexusOperationsOperation{
		TerminateNexusOperationsOperation: &batchpb.BatchOperationTerminateNexusOperations{
			Identity: nexusBatchIdentity,
			Reason:   nexusBatchReason,
		},
	}
}

func (env *NexusTestEnv) awaitBatchTerminated(s parallelsuite.Scope, op startedNexusOperation) {
	parallelsuite.Await(s, func(s parallelsuite.Scope) {
		resp := env.describeNexusOperation(s.Context(), s.AssertionT(), op.operationID)
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED, resp.GetInfo().GetStatus())
		s.Equal(nexusBatchReason, resp.GetFailure().GetMessage())
		s.Equal(nexusBatchIdentity, resp.GetFailure().GetTerminatedFailureInfo().GetIdentity())
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *NexusStandaloneBatchTerminateTestSuite) TestNexusOperationBatchTerminate_Success() {
	for _, selector := range nexusBatchTargetSelectors() {
		s.Run(selector.name, func(s *NexusStandaloneBatchTerminateTestSuite) {
			env := newNexusStandaloneBatchEnv(s.T(), true)
			endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
			ops := make([]startedNexusOperation, 0, 3)
			for i := range 3 {
				ops = append(ops, env.startBatchTarget(s, endpoint, fmt.Sprintf("op-%d", i)))
			}

			jobID := uuid.NewString()
			req := newNexusBatchRequest(env, jobID)
			setNexusBatchTerminateOperation(req)
			expectedQuery, expectedExecutions := selector.apply(s, env, endpoint, ops, req)
			_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
			s.NoError(err)

			assertBatchOperationType(s.Context(), s.T(), env.TestEnv, jobID, enumspb.BATCH_OPERATION_TYPE_TERMINATE_NEXUS_OPERATION, expectedQuery, expectedExecutions)
			for _, op := range ops {
				env.awaitBatchTerminated(s, op)
			}
		})
	}
}

// TestNexusOperationBatchTerminate_ExcludesNonRunning verifies that batch
// terminate only targets open operations.
func (s *NexusStandaloneBatchTerminateTestSuite) TestNexusOperationBatchTerminate_ExcludesNonRunning() {
	env := newNexusStandaloneBatchEnv(s.T(), true)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
	running, closed := env.startRunningAndClosedBatchTargets(s, endpoint)

	jobID := uuid.NewString()
	req := newNexusBatchRequest(env, jobID)
	setNexusBatchTerminateOperation(req)
	// Query intentionally omits ExecutionStatus; the server adds it for terminate.
	req.VisibilityQuery = nexusEndpointQuery(endpoint)
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
	s.NoError(err)

	env.awaitBatchCompleted(s, jobID, 1)
	env.awaitBatchTerminated(s, running)
	env.requireUntouchedByBatch(s, closed)
}

// TestNexusOperationBatchTerminate_TargetExecutionTypeMismatch verifies that
// Nexus batch operations reject TargetExecutions of another execution type.
func (s *NexusStandaloneBatchTerminateTestSuite) TestNexusOperationBatchTerminate_TargetExecutionTypeMismatch() {
	env := newNexusStandaloneBatchEnv(s.T(), true)
	req := newNexusBatchRequest(env, uuid.NewString())
	setNexusBatchTerminateOperation(req)
	req.TargetExecutions = []*commonpb.Execution{{
		Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
		BusinessId: "some-activity-id",
		RunId:      uuid.NewString(),
	}}
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "target_executions[0]")
}

func (s *NexusStandaloneBatchTerminateTestSuite) TestBatchOperationsForStandaloneNexusOperationsDisabled() {
	tests := []struct {
		name         string
		setOperation func(req *workflowservice.StartBatchOperationRequest)
	}{
		{
			name:         "terminate",
			setOperation: setNexusBatchTerminateOperation,
		},
		{
			name: "cancel",
			setOperation: func(req *workflowservice.StartBatchOperationRequest) {
				req.Operation = &workflowservice.StartBatchOperationRequest_CancelNexusOperationsOperation{
					CancelNexusOperationsOperation: &batchpb.BatchOperationCancelNexusOperations{},
				}
			},
		},
		{
			name: "delete",
			setOperation: func(req *workflowservice.StartBatchOperationRequest) {
				req.Operation = &workflowservice.StartBatchOperationRequest_DeleteNexusOperationsOperation{
					DeleteNexusOperationsOperation: &batchpb.BatchOperationDeleteNexusOperations{},
				}
			},
		},
	}

	env := newNexusStandaloneBatchEnv(s.T(), false)
	for _, test := range tests {
		s.Run(test.name, func(s *NexusStandaloneBatchTerminateTestSuite) {
			req := newNexusBatchRequest(env, uuid.NewString())
			test.setOperation(req)
			req.VisibilityQuery = "Endpoint = 'some-endpoint'"
			_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
			var invalidArgument *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgument)
			s.ErrorContains(err, "not supported for this namespace")
		})
	}
}
