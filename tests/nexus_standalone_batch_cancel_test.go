package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type NexusStandaloneBatchCancelTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneBatchCancelTestSuite]
}

func TestNexusStandaloneBatchCancelTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneBatchCancelTestSuite{})
}

func setNexusBatchCancelOperation(req *workflowservice.StartBatchOperationRequest) {
	req.Operation = &workflowservice.StartBatchOperationRequest_CancelNexusOperationsOperation{
		CancelNexusOperationsOperation: &batchpb.BatchOperationCancelNexusOperations{
			Identity: nexusBatchIdentity,
			Reason:   nexusBatchReason,
		},
	}
}

// awaitBatchCancelRequested waits for the batch's cancellation to be recorded.
// Targets never start (their endpoint targets a task queue with no poller), so
// the operation stays running.
func (env *NexusTestEnv) awaitBatchCancelRequested(s parallelsuite.Scope, op startedNexusOperation) {
	parallelsuite.Await(s, func(s parallelsuite.Scope) {
		info := env.describeNexusOperation(s.Context(), s.AssertionT(), op.operationID).GetInfo()
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING, info.GetStatus())
		s.Equal(nexusBatchReason, info.GetCancellationInfo().GetReason())
	}, 10*time.Second, 100*time.Millisecond)
}

// terminateOnCleanup terminates op when the test ends. A cancellation is only
// sent once an operation starts, and op never starts because its task queue has
// no worker, so op would otherwise keep retrying dispatch until its
// schedule-to-close timeout.
func (env *NexusTestEnv) terminateOnCleanup(s parallelsuite.Scope, op startedNexusOperation) {
	s.TB().Cleanup(func() {
		// Fresh context: the test's context may be canceled by the time cleanup runs.
		_, _ = env.FrontendClient().TerminateNexusOperationExecution(testcore.NewContext(), &workflowservice.TerminateNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
			RequestId:   uuid.NewString(),
		})
	})
}

func (s *NexusStandaloneBatchCancelTestSuite) TestNexusOperationBatchCancel_Success() {
	for _, selector := range nexusBatchTargetSelectors() {
		s.Run(selector.name, func(s *NexusStandaloneBatchCancelTestSuite) {
			env := newNexusStandaloneBatchEnv(s.T(), true)
			endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
			ops := make([]startedNexusOperation, 0, 3)
			for i := range 3 {
				op := env.startBatchTarget(s, endpoint, fmt.Sprintf("op-%d", i))
				env.terminateOnCleanup(s, op)
				ops = append(ops, op)
			}

			jobID := uuid.NewString()
			req := newNexusBatchRequest(env, jobID)
			setNexusBatchCancelOperation(req)
			expectedQuery, expectedExecutions := selector.apply(s, env, endpoint, ops, req)
			_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
			s.NoError(err)

			assertBatchOperationType(s.Context(), s.T(), env.TestEnv, jobID, enumspb.BATCH_OPERATION_TYPE_CANCEL_NEXUS_OPERATION, expectedQuery, expectedExecutions)
			for _, op := range ops {
				env.awaitBatchCancelRequested(s, op)
			}
		})
	}
}

// TestNexusOperationBatchCancel_ExcludesNonRunning verifies that batch cancel
// only targets open operations.
func (s *NexusStandaloneBatchCancelTestSuite) TestNexusOperationBatchCancel_ExcludesNonRunning() {
	env := newNexusStandaloneBatchEnv(s.T(), true)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
	running, closed := env.startRunningAndClosedBatchTargets(s, endpoint)
	env.terminateOnCleanup(s, running)

	jobID := uuid.NewString()
	req := newNexusBatchRequest(env, jobID)
	setNexusBatchCancelOperation(req)
	// Query intentionally omits ExecutionStatus; the server adds it for cancel.
	req.VisibilityQuery = nexusEndpointQuery(endpoint)
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
	s.NoError(err)

	env.awaitBatchCompleted(s, jobID, 1)
	env.awaitBatchCancelRequested(s, running)
	env.requireUntouchedByBatch(s, closed)
}
