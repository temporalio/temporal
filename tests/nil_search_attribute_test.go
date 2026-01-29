package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NilSearchAttributeFilterTestSuite struct {
	testcore.FunctionalTestBase
}

func TestNilSearchAttributeFilterTestSuite(t *testing.T) {
	s := new(NilSearchAttributeFilterTestSuite)
	suite.Run(t, s)
}

func (s *NilSearchAttributeFilterTestSuite) TestWorkflowStart_NilSearchAttributesFiltered() {
	workflowID := "nil-sa-filter-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Create search attributes with a mix of valid and nil values
	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)

	validPayload := payload.EncodeString("valid-value")

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": validPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

	// Get the workflow history and check the started event
	historyResp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.Require().NoError(err)

	var startedEvent *historypb.HistoryEvent
	for _, event := range historyResp.History.Events {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			startedEvent = event
			break
		}
	}

	s.Require().NotNil(startedEvent)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Require().NotNil(attrs)

	// Verify nil search attributes are filtered out
	if attrs.SearchAttributes != nil {
		// The valid key should be present
		s.NotNil(attrs.SearchAttributes.IndexedFields["CustomKeywordField"])
		// The nil key should NOT be present
		_, hasNilKey := attrs.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should be filtered out from history event")
	}

	// Terminate the workflow
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Reason: "test cleanup",
	})
	s.Require().NoError(err)
}

func (s *NilSearchAttributeFilterTestSuite) TestWorkflowStart_AllNilSearchAttributesFiltered() {
	workflowID := "nil-sa-filter-all-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Create search attributes with only nil values
	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": nilPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

	// Get the workflow history and check the started event
	historyResp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.Require().NoError(err)

	var startedEvent *historypb.HistoryEvent
	for _, event := range historyResp.History.Events {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			startedEvent = event
			break
		}
	}

	s.Require().NotNil(startedEvent)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.Require().NotNil(attrs)

	// When all search attributes are nil, the entire SearchAttributes should be nil
	s.Nil(attrs.SearchAttributes, "SearchAttributes should be nil when all values are nil")

	// Terminate the workflow
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Reason: "test cleanup",
	})
	s.Require().NoError(err)
}

func (s *NilSearchAttributeFilterTestSuite) TestDescribeWorkflow_NilSearchAttributesNotVisible() {
	workflowID := "nil-sa-filter-describe-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.TaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Create search attributes with a mix of valid and nil values
	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)

	validPayload := payload.EncodeString("valid-value")

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": validPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

	// Describe the workflow and check search attributes in mutable state
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.Require().NoError(err)
	s.Require().NotNil(descResp)

	// Verify nil search attributes are not visible in mutable state
	if descResp.WorkflowExecutionInfo.SearchAttributes != nil {
		// The valid key should be present
		s.NotNil(descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomKeywordField"])
		// The nil key should NOT be present
		_, hasNilKey := descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should not be visible in mutable state")
	}

	// Terminate the workflow
	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Reason: "test cleanup",
	})
	s.Require().NoError(err)
}
