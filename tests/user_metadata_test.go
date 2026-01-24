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
	"google.golang.org/protobuf/proto"
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
		return client.DescribeWorkflowExecution(testcore.NewContext(), workflowservice.DescribeWorkflowExecutionRequest_builder{
			Namespace: namespace,
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: workflowID,
				RunId:      runID,
			}.Build(),
		}.Build())
	}
	prepareTestUserMetadata := func() *sdkpb.UserMetadata {
		return sdkpb.UserMetadata_builder{
			Summary: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_summary_key": []byte(`test_summary_val`)},
				Data:     []byte(`Test summary Data`),
			}.Build(),
			Details: commonpb.Payload_builder{
				Metadata: map[string][]byte{"test_details_key": []byte(`test_details_val`)},
				Data:     []byte(`Test Details Data`),
			}.Build(),
		}.Build()
	}

	s.Run("StartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		request := workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}.Build()

		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), we.GetRunId())
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.GetExecutionConfig().GetUserMetadata())
	})

	s.Run("SignalWithStartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		request := workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}.Build()

		we, err := s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), we.GetRunId())
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.GetExecutionConfig().GetUserMetadata())
	})

	s.Run("ExecuteMultiOperation records UserMetadata", func() {
		tv := testvars.New(s.T())
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}.Build()
		updateWorkflowRequest := workflowservice.UpdateWorkflowExecutionRequest_builder{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{WorkflowId: tv.WorkflowID()}.Build(),
			Request: updatepb.Request_builder{
				Meta:  updatepb.Meta_builder{UpdateId: "UPDATE_ID"}.Build(),
				Input: updatepb.Input_builder{Name: "NAME"}.Build(),
			}.Build(),
		}.Build()
		request := workflowservice.ExecuteMultiOperationRequest_builder{
			Namespace: s.Namespace().String(),
			Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
				workflowservice.ExecuteMultiOperationRequest_Operation_builder{ // start workflow operation
					StartWorkflow: proto.ValueOrDefault(startWorkflowRequest),
				}.Build(),
				workflowservice.ExecuteMultiOperationRequest_Operation_builder{ // update workflow operation
					UpdateWorkflow: proto.ValueOrDefault(updateWorkflowRequest),
				}.Build(),
			},
		}.Build()

		_, err := s.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.FrontendClient(), s.Namespace().String(), tv.WorkflowID(), "")
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.GetExecutionConfig().GetUserMetadata())
	})

}
