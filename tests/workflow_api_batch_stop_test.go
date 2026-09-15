package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchStopClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchStopClientTestSuite]
}

func TestWorkflowAPIBatchStopClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchStopClientTestSuite{})
}

// TestWorkflowBatchStop_RejectsNonBatchWorkflow verifies that StopBatchOperation
// will not terminate a workflow that is not a batch job. The job ID is a
// caller-supplied workflow ID and the terminate it performs is an in-process
// call that skips the authorization check a TerminateWorkflowExecution API call
// goes through, so a missing batch-job check would let anyone permitted to stop
// batch operations terminate any workflow in the namespace by ID.
func (s *WorkflowAPIBatchStopClientTestSuite) TestWorkflowBatchStop_RejectsNonBatchWorkflow() {
	env := newWorkflowBatchEnv(s.T())
	t := s.T()
	ctx := s.Context()

	workflowType := testcore.RandomizeStr(t.Name())
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(fmt.Sprintf("%s-victim", t.Name())),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowType)
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}

	// Wait until the workflow is running so the failure below cannot be confused
	// with the workflow not existing yet.
	//nolint:forbidigo // for tests with waits
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}, 10*time.Second, 100*time.Millisecond)

	// Pass the running workflow's ID as the batch job ID.
	_, err = env.FrontendClient().StopBatchOperation(ctx, &workflowservice.StopBatchOperationRequest{
		Namespace: env.Namespace().String(),
		JobId:     run.GetID(),
		Reason:    "test",
		Identity:  "batch-stopper",
	})
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)

	// The workflow must be untouched.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus())
}

// TestWorkflowBatchStop_UnknownJobID verifies that stopping a job ID that names
// no workflow at all is reported as not found rather than some other error.
func (s *WorkflowAPIBatchStopClientTestSuite) TestWorkflowBatchStop_UnknownJobID() {
	env := newWorkflowBatchEnv(s.T())
	ctx := s.Context()

	_, err := env.FrontendClient().StopBatchOperation(ctx, &workflowservice.StopBatchOperationRequest{
		Namespace: env.Namespace().String(),
		JobId:     uuid.NewString(),
		Reason:    "test",
		Identity:  "batch-stopper",
	})
	var notFoundErr *serviceerror.NotFound
	s.ErrorAs(err, &notFoundErr)
}
