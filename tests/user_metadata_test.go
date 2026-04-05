package tests

import (
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type UserMetadataSuite struct {
	parallelsuite.Suite[*UserMetadataSuite]
}

func TestUserMetadataSuite(t *testing.T) {
	parallelsuite.Run(t, &UserMetadataSuite{})
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

	s.Run("StartWorkflowExecution records UserMetadata", func(s *UserMetadataSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()
		metadata := prepareTestUserMetadata()
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(env.FrontendClient(), env.Namespace().String(), tv.WorkflowID(), we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("SignalWithStartWorkflowExecution records UserMetadata", func(s *UserMetadataSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()
		metadata := prepareTestUserMetadata()
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := env.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(env.FrontendClient(), env.Namespace().String(), tv.WorkflowID(), we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("ExecuteMultiOperation records UserMetadata", func(s *UserMetadataSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: env.Namespace().String(),
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

		_, err := env.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(env.FrontendClient(), env.Namespace().String(), tv.WorkflowID(), "")
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

}
