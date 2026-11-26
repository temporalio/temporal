package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type NexusStandaloneBatchDeleteTestSuite struct {
	parallelsuite.Suite[*NexusStandaloneBatchDeleteTestSuite]
}

func TestNexusStandaloneBatchDeleteTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusStandaloneBatchDeleteTestSuite{})
}

func setNexusBatchDeleteOperation(req *workflowservice.StartBatchOperationRequest) {
	req.Operation = &workflowservice.StartBatchOperationRequest_DeleteNexusOperationsOperation{
		DeleteNexusOperationsOperation: &batchpb.BatchOperationDeleteNexusOperations{},
	}
}

func (env *NexusTestEnv) awaitBatchDeleted(s parallelsuite.Scope, op startedNexusOperation) {
	parallelsuite.Await(s, func(s parallelsuite.Scope) {
		_, err := env.FrontendClient().DescribeNexusOperationExecution(s.Context(), &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: op.operationID,
			RunId:       op.runID,
		})
		var notFound *serviceerror.NotFound
		s.Require().ErrorAs(err, &notFound)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *NexusStandaloneBatchDeleteTestSuite) TestNexusOperationBatchDelete_Success() {
	for _, selector := range nexusBatchTargetSelectors() {
		s.Run(selector.name, func(s *NexusStandaloneBatchDeleteTestSuite) {
			env := newNexusStandaloneBatchEnv(s.T(), true)
			endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
			ops := make([]startedNexusOperation, 0, 3)
			for i := range 3 {
				ops = append(ops, env.startBatchTarget(s, endpoint, fmt.Sprintf("op-%d", i)))
			}

			jobID := uuid.NewString()
			req := newNexusBatchRequest(env, jobID)
			setNexusBatchDeleteOperation(req)
			expectedQuery, expectedExecutions := selector.apply(s, env, endpoint, ops, req)
			_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
			s.NoError(err)

			assertBatchOperationType(s.Context(), s.T(), env.TestEnv, jobID, enumspb.BATCH_OPERATION_TYPE_DELETE_NEXUS_OPERATION, expectedQuery, expectedExecutions)
			for _, op := range ops {
				env.awaitBatchDeleted(s, op)
			}
		})
	}
}

// TestNexusOperationBatchDelete_IncludesNonRunning verifies that, unlike
// terminate and cancel, batch delete also targets closed operations.
func (s *NexusStandaloneBatchDeleteTestSuite) TestNexusOperationBatchDelete_IncludesNonRunning() {
	env := newNexusStandaloneBatchEnv(s.T(), true)
	endpoint := env.createRandomNexusEndpoint(s.Context(), s.T()).GetSpec().GetName()
	running, closed := env.startRunningAndClosedBatchTargets(s, endpoint)

	jobID := uuid.NewString()
	req := newNexusBatchRequest(env, jobID)
	setNexusBatchDeleteOperation(req)
	// Query intentionally omits ExecutionStatus; delete must not add it.
	req.VisibilityQuery = nexusEndpointQuery(endpoint)
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), req)
	s.NoError(err)

	env.awaitBatchCompleted(s, jobID, 2)
	env.awaitBatchDeleted(s, running)
	env.awaitBatchDeleted(s, closed)
}
