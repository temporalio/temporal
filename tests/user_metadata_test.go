package tests

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func getDescribeWorkflowExecutionInfo(engine FrontendClient, namespace string, workflowID string, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
}

func prepareTestUserMetadata() *sdkpb.UserMetadata {
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

// TestUserMetadataStartWorkflowExecution verifies that the user metadata passed in StartWorkflowExecution request
// is correctly recorded and returned in the describe workflow response.
func (s *FunctionalSuite) TestUserMetadataStartWorkflowExecution() {
	id := "functional-user-metadata-StartWorkflowExecution"
	metadata := prepareTestUserMetadata()
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   id,
		WorkflowType: &commonpb.WorkflowType{Name: "functional-user-metadata-workflow-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "functional-user-metadata-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		UserMetadata: metadata,
	}

	we, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	// Verify that the UserMetadata associated with the start event is returned in the describe response.
	describeInfo, err := getDescribeWorkflowExecutionInfo(s.engine, s.namespace, id, we.RunId)
	s.NoError(err)
	s.NotNil(describeInfo.ExecutionConfig.UserMetadata)
	s.EqualExportedValues(describeInfo.ExecutionConfig.UserMetadata, metadata)
}

// TestUserMetadataSignalWithStartWorkflowExecution verifies that the user metadata attached to SignalWithStartWorkflowExecution request
// is correctly recorded and returned in the describe workflow response.
func (s *FunctionalSuite) TestUserMetadataSignalWithStartWorkflowExecution() {
	id := "functional-user-metadata-SignalWithStartWorkflowExecution"
	metadata := prepareTestUserMetadata()
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   id,
		WorkflowType: &commonpb.WorkflowType{Name: "functional-user-metadata-workflow-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "functional-user-metadata-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		SignalName:   "TEST-SIGNAL",
		UserMetadata: metadata,
	}

	we, err := s.engine.SignalWithStartWorkflowExecution(NewContext(), request)
	s.NoError(err)

	// Verify that the UserMetadata associated with the start event is returned in the describe response.
	describeInfo, err := getDescribeWorkflowExecutionInfo(s.engine, s.namespace, id, we.RunId)
	s.NoError(err)
	s.NotNil(describeInfo.ExecutionConfig.UserMetadata)
	s.EqualExportedValues(describeInfo.ExecutionConfig.UserMetadata, metadata)
}

// TestUserMetadataExecuteMultiOperation issues a multi operation that includes start workflow with metadata attached.
// It verifies that the sent metadata is correctly recorded and returned in the describe workflow response.
func (s *FunctionalSuite) TestUserMetadataExecuteMultiOperation() {
	id := "functional-user-metadata-ExecuteMultiOperation"
	metadata := prepareTestUserMetadata()
	startWorkflowRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.New(),
		Namespace:    s.namespace,
		WorkflowId:   id,
		WorkflowType: &commonpb.WorkflowType{Name: "functional-user-metadata-workflow-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "functional-user-metadata-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
	s.NotNil(describeInfo.ExecutionConfig.UserMetadata)
	s.EqualExportedValues(describeInfo.ExecutionConfig.UserMetadata, metadata)
}
