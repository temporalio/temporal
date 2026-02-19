package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkflowStart_NilSearchAttributesFiltered(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-sa-filter-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	if attrs.SearchAttributes != nil {
		s.NotNil(attrs.SearchAttributes.IndexedFields["CustomKeywordField"])
		_, hasNilKey := attrs.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should be filtered out from history event")
	}

	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.Require().NoError(err)
}

func TestWorkflowStart_AllNilSearchAttributesFiltered(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-sa-filter-all-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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
	s.Nil(attrs.SearchAttributes, "SearchAttributes should be nil when all values are nil")

	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.Require().NoError(err)
}

func TestDescribeWorkflow_NilSearchAttributesNotVisible(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-sa-filter-describe-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

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

	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.Require().NoError(err)
	s.Require().NotNil(descResp)

	if descResp.WorkflowExecutionInfo.SearchAttributes != nil {
		s.NotNil(descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomKeywordField"])
		_, hasNilKey := descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should not be visible in mutable state")
	}

	_, err = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.Require().NoError(err)
}

func TestWorkflowStart_NilMemoFiltered(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-memo-filter-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)
	validPayload := payload.EncodeString("valid-value")

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"ValidKey": validPayload,
			"NilKey":   nilPayload,
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
		Memo:                memo,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

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

	if attrs.Memo != nil {
		s.NotNil(attrs.Memo.Fields["ValidKey"])
		_, hasNilKey := attrs.Memo.Fields["NilKey"]
		s.False(hasNilKey, "nil memo key should be filtered out from history event")
	}

	_, _ = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}

func TestWorkflowStart_AllNilMemoFiltered(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-memo-filter-all-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"NilKey1": nilPayload,
			"NilKey2": nilPayload,
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
		Memo:                memo,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

	historyResp, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: we.GetRunId()},
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
	s.Nil(attrs.Memo, "Memo should be nil when all values are nil")

	_, _ = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}

func TestDescribeWorkflow_NilMemoNotVisible(t *testing.T) {
	s := testcore.NewEnv(t)
	workflowID := "nil-memo-filter-describe-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: s.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.Require().NoError(err)
	validPayload := payload.EncodeString("valid-value")

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"ValidKey": validPayload,
			"NilKey":   nilPayload,
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
		Memo:                memo,
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Require().NoError(err)
	s.Require().NotNil(we.GetRunId())

	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: we.GetRunId()},
	})
	s.Require().NoError(err)
	s.Require().NotNil(descResp)

	if descResp.WorkflowExecutionInfo.Memo != nil {
		s.NotNil(descResp.WorkflowExecutionInfo.Memo.Fields["ValidKey"])
		_, hasNilKey := descResp.WorkflowExecutionInfo.Memo.Fields["NilKey"]
		s.False(hasNilKey, "nil memo key should not be visible in mutable state / describe")
	}

	_, _ = s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}
