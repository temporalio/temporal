package tests

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type UserMetadataSuite struct {
	testcore.FunctionalTestBase
}

func TestUserMetadataSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UserMetadataSuite))
}

func (s *UserMetadataSuite) TestUserMetadata() {
	getDescribeWorkflowExecutionInfo := func(client workflowservice.WorkflowServiceClient, namespace string, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return client.DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
	}
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

	s.Run("StartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("SignalWithStartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("ExecuteMultiOperation records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
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
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), "")
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

}
