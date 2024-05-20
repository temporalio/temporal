package tests

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/testvars"
)

func (s *FunctionalSuite) TestUserMetadata() {
	getDescribeWorkflowExecutionInfo := func(engine FrontendClient, namespace string, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
		return engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
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
		tv := testvars.New(s.T().Name())
		id := tv.WorkflowID("functional-user-metadata-StartWorkflowExecution")
		metadata := prepareTestUserMetadata()
		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.namespace,
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}

		we, err := s.engine.StartWorkflowExecution(NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.engine, s.namespace, id, we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("SignalWithStartWorkflowExecution records UserMetadata", func() {
		tv := testvars.New(s.T().Name())
		id := tv.WorkflowID("functional-user-metadata-SignalWithStartWorkflowExecution")
		metadata := prepareTestUserMetadata()
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.namespace,
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			SignalName:   "TEST-SIGNAL",
			UserMetadata: metadata,
		}

		we, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.engine, s.namespace, id, we.RunId)
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

	s.Run("ExecuteMultiOperation records UserMetadata", func() {
		tv := testvars.New(s.T().Name())
		id := tv.WorkflowID("functional-user-metadata-ExecuteMultiOperation")
		metadata := prepareTestUserMetadata()
		startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.New(),
			Namespace:    s.namespace,
			WorkflowId:   id,
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			UserMetadata: metadata,
		}
		updateWorkflowRequest := &workflowservice.UpdateWorkflowExecutionRequest{
			Namespace:         s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id},
			Request: &updatepb.Request{
				Meta:  &updatepb.Meta{UpdateId: "UPDATE_ID"},
				Input: &updatepb.Input{Name: "NAME"},
			},
		}
		request := &workflowservice.ExecuteMultiOperationRequest{
			Namespace: s.namespace,
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

		_, err := s.engine.ExecuteMultiOperation(NewContext(), request)
		s.NoError(err)

		// Verify that the UserMetadata associated with the start event is returned in the describe response.
		describeInfo, err := getDescribeWorkflowExecutionInfo(s.engine, s.namespace, id, "")
		s.NoError(err)
		s.EqualExportedValues(metadata, describeInfo.ExecutionConfig.UserMetadata)
	})

}
