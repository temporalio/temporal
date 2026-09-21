package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
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

// batchStopThrottleRPS holds the batcher to one operation per second so the job
// in TestWorkflowBatchStop_StopsRunningBatchJob is still running when it is
// stopped; the terminates themselves take milliseconds.
const batchStopThrottleRPS = 1

// batchStopTargetCount is how many workflows that job targets. At
// batchStopThrottleRPS per second the job needs roughly this many seconds to
// run, which is the window the stop has to land in.
const batchStopTargetCount = 10

// TestWorkflowBatchStop_StopsRunningBatchJob verifies the accept path of the
// job-ID check: a real batch job is recognized and actually stopped. The tests
// above only cover what StopBatchOperation refuses, and the unit test for the
// accept path builds the DescribeWorkflowExecution response itself, so nothing
// else checks that a genuine batcher workflow — carrying the workflow type and
// namespace division the server really writes at start time — gets past the
// check.
func (s *WorkflowAPIBatchStopClientTestSuite) TestWorkflowBatchStop_StopsRunningBatchJob() {
	env := newWorkflowBatchEnv(s.T())
	t := s.T()
	ctx := s.Context()

	env.OverrideDynamicConfig(dynamicconfig.BatcherRPS, batchStopThrottleRPS)

	workflowType := testcore.RandomizeStr(t.Name())
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	executions := make([]*commonpb.WorkflowExecution, 0, batchStopTargetCount)
	for i := range batchStopTargetCount {
		run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i)),
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowType)
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		})
	}

	// Targeted explicitly rather than by query, so the job cannot finish early
	// by finding nothing while visibility is still catching up.
	jobID := uuid.NewString()
	_, err := env.FrontendClient().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:        env.Namespace().String(),
		TargetExecutions: toTargetExecutions(executions),
		Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{
				Identity: "batch-terminator",
			},
		},
		JobId:  jobID,
		Reason: "test",
	})
	s.NoError(err)

	// Wait for the job to be running, so the stop exercises the accept path
	// rather than racing the job's own start.
	//nolint:forbidigo // for tests with waits
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		return err == nil && desc.GetState() == enumspb.BATCH_OPERATION_STATE_RUNNING
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().StopBatchOperation(ctx, &workflowservice.StopBatchOperationRequest{
		Namespace: env.Namespace().String(),
		JobId:     jobID,
		Reason:    "test stop",
		Identity:  "batch-stopper",
	})
	s.NoError(err)

	// A stopped job is a terminated batcher workflow, which reports as failed.
	//nolint:forbidigo // for tests with waits
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		return err == nil && desc.GetState() == enumspb.BATCH_OPERATION_STATE_FAILED
	}, 10*time.Second, 100*time.Millisecond)
}
