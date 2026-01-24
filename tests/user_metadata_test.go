package tests

import (
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
)

func TestUserMetadata(t *testing.T) {
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

	t.Run("StartWorkflowExecution records UserMetadata", func(t *testing.T) {
		s := testcore.NewEnv(t)
		metadata := prepareTestUserMetadata()
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   s.Tv().WorkflowID(),
			WorkflowType: s.Tv().WorkflowType(),
			TaskQueue:    s.Tv().TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: s.Tv().WorkflowID(),
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	t.Run("SignalWithStartWorkflowExecution records UserMetadata", func(t *testing.T) {
		s := testcore.NewEnv(t)
		metadata := prepareTestUserMetadata()
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   s.Tv().WorkflowID(),
			WorkflowType: s.Tv().WorkflowType(),
			TaskQueue:    s.Tv().TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: s.Tv().WorkflowID(),
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	t.Run("ExecuteMultiOperation records UserMetadata", func(t *testing.T) {
		s := testcore.NewEnv(t)
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   s.Tv().WorkflowID(),
			WorkflowType: s.Tv().WorkflowType(),
			TaskQueue:    s.Tv().TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: s.Tv().WorkflowID()},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.Namespace().String(),
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

		_, err := s.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: s.Tv().WorkflowID(),
				RunId:      "",
			},
		})
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})
}
