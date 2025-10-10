package tests

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

func TestUserMetadata(t *testing.T) {
	t.Parallel()

	// Setup shared test cluster for all subtests
	cluster, logger := testcore.NewTestClusterForTestGroup(t)

	// Helper function to prepare test metadata
	prepareTestUserMetadata := func() *sdkpb.UserMetadata {
		return &sdkpb.UserMetadata{
			Summary: &commonpb.Payload{
				Metadata: map[string][]byte{"test_summary_key": []byte(`test_summary_val`)},
				Data:     []byte(`Test summary Data`),
			},
			Details: &commonpb.Payload{
				Metadata: map[string][]byte{"test_details_key": []byte(`test_details_val`)},
				Data:     []byte(`Test Details Data`),
			},
		}
	}

	// Helper function to get workflow execution info
	getDescribeWorkflowExecutionInfo := func(client workflowservice.WorkflowServiceClient, namespace string, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return client.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
	}

	t.Run("StartWorkflowExecution records UserMetadata", func(t *testing.T) {
		t.Parallel()

		f := testcore.NewTestFixture(t, cluster, logger)
		tv := testvars.New(t)
		metadata := prepareTestUserMetadata()

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    f.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := f.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		require.NoError(t, err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(f.FrontendClient(), f.Namespace().String(), tv.WorkflowID(), we.RunId)
		require.NoError(t, err)
		require.EqualExportedValues(t, metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	t.Run("SignalWithStartWorkflowExecution records UserMetadata", func(t *testing.T) {
		t.Parallel()

		f := testcore.NewTestFixture(t, cluster, logger)
		tv := testvars.New(t)
		metadata := prepareTestUserMetadata()

		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    f.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := f.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		require.NoError(t, err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(f.FrontendClient(), f.Namespace().String(), tv.WorkflowID(), we.RunId)
		require.NoError(t, err)
		require.EqualExportedValues(t, metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	t.Run("ExecuteMultiOperation records UserMetadata", func(t *testing.T) {
		t.Parallel()

		f := testcore.NewTestFixture(t, cluster, logger)
		tv := testvars.New(t)
		metadata := prepareTestUserMetadata()

		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    f.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         f.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: f.Namespace().String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				{ // start workflow operation
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
						StartWorkflow: startWorkflowRequest,
					},
				},
				{ // update workflow operation
					Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
						UpdateWorkflow: updateWorkflowRequest,
					},
				},
			},
		}

		_, err := f.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), request)
		require.NoError(t, err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(f.FrontendClient(), f.Namespace().String(), tv.WorkflowID(), "")
		require.NoError(t, err)
		require.EqualExportedValues(t, metadata, describeInfo.ExecutionConfig.UserMetadata)
	})
}
