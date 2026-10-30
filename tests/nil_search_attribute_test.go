package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type NilSearchAttributeSuite struct {
	parallelsuite.Suite[*NilSearchAttributeSuite]
}

func TestNilSearchAttributeSuite(t *testing.T) {
	parallelsuite.Run(t, &NilSearchAttributeSuite{})
}

func (s *NilSearchAttributeSuite) TestWorkflowStart_NilSearchAttributesFiltered() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-sa-filter-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)
	validPayload := sadefs.MustEncodeValue("valid-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": validPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	historyResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)

	startedEvent := s.RequireHistoryEvent(historyResp.History.Events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attrs)

	if attrs.SearchAttributes != nil {
		s.NotNil(attrs.SearchAttributes.IndexedFields["CustomKeywordField"])
		_, hasNilKey := attrs.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should be filtered out from history event")
	}

	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.NoError(err)
}

func (s *NilSearchAttributeSuite) TestWorkflowStart_AllNilSearchAttributesFiltered() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-sa-filter-all-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": nilPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	historyResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)

	startedEvent := s.RequireHistoryEvent(historyResp.History.Events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attrs)
	s.Nil(attrs.SearchAttributes, "SearchAttributes should be nil when all values are nil")

	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.NoError(err)
}

func (s *NilSearchAttributeSuite) TestDescribeWorkflow_NilSearchAttributesNotVisible() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-sa-filter-describe-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-sa-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)
	validPayload := sadefs.MustEncodeValue("valid-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)

	searchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"CustomKeywordField": validPayload,
			"CustomTextField":    nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		SearchAttributes:    searchAttributes,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)
	s.NotNil(descResp)

	if descResp.WorkflowExecutionInfo.SearchAttributes != nil {
		s.NotNil(descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomKeywordField"])
		_, hasNilKey := descResp.WorkflowExecutionInfo.SearchAttributes.IndexedFields["CustomTextField"]
		s.False(hasNilKey, "nil search attribute key should not be visible in mutable state")
	}

	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:            "test cleanup",
	})
	s.NoError(err)
}

func (s *NilSearchAttributeSuite) TestWorkflowStart_NilMemoFiltered() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-memo-filter-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)
	validPayload := sadefs.MustEncodeValue("valid-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"ValidKey": validPayload,
			"NilKey":   nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		Memo:                memo,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	historyResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)

	startedEvent := s.RequireHistoryEvent(historyResp.History.Events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attrs)

	if attrs.Memo != nil {
		s.NotNil(attrs.Memo.Fields["ValidKey"])
		_, hasNilKey := attrs.Memo.Fields["NilKey"]
		s.False(hasNilKey, "nil memo key should be filtered out from history event")
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}

func (s *NilSearchAttributeSuite) TestWorkflowStart_AllNilMemoFiltered() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-memo-filter-all-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"NilKey1": nilPayload,
			"NilKey2": nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		Memo:                memo,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	historyResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: we.GetRunId()},
	})
	s.NoError(err)

	startedEvent := s.RequireHistoryEvent(historyResp.History.Events, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	attrs := startedEvent.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attrs)
	s.Nil(attrs.Memo, "Memo should be nil when all values are nil")

	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}

func (s *NilSearchAttributeSuite) TestDescribeWorkflow_NilMemoNotVisible() {
	env := testcore.NewEnv(s.T())
	workflowID := "nil-memo-filter-describe-" + uuid.NewString()
	workflowType := &commonpb.WorkflowType{Name: "nil-memo-filter-workflow-type"}
	taskQueue := &taskqueuepb.TaskQueue{Name: env.Tv().TaskQueue().Name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	nilPayload, err := payload.Encode(nil)
	s.NoError(err)
	validPayload := sadefs.MustEncodeValue("valid-value", enumspb.INDEXED_VALUE_TYPE_KEYWORD)

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"ValidKey": validPayload,
			"NilKey":   nilPayload,
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(10 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            "test-identity",
		Memo:                memo,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	s.NotNil(we.GetRunId())

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: we.GetRunId()},
	})
	s.NoError(err)
	s.NotNil(descResp)

	if descResp.WorkflowExecutionInfo.Memo != nil {
		s.NotNil(descResp.WorkflowExecutionInfo.Memo.Fields["ValidKey"])
		_, hasNilKey := descResp.WorkflowExecutionInfo.Memo.Fields["NilKey"]
		s.False(hasNilKey, "nil memo key should not be visible in mutable state / describe")
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID}, Reason: "test cleanup",
	})
}
