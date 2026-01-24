package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkflowVisibilityTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowVisibilityTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowVisibilityTestSuite))
}

func (s *WorkflowVisibilityTestSuite) TestVisibility() {
	startTime := time.Now().UTC()

	// Start 2 workflow executions
	id1 := "functional-visibility-test1"
	id2 := "functional-visibility-test2"
	wt := "functional-visibility-test-type"
	tl := "functional-visibility-test-taskqueue"
	identity := "worker1"

	startRequest := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id1,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	startResponse, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startRequest)
	s.NoError(err0)

	// Now complete one of the executions
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: commandpb.CompleteWorkflowExecutionCommandAttributes_builder{
				Result: payloads.EncodeString("Done"),
			}.Build(),
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err1 := poller.PollAndProcessWorkflowTask()
	s.NoError(err1)

	// wait until the start workflow is done
	var nextToken []byte
	historyEventFilterType := enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	for {
		historyResponse, historyErr := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), workflowservice.GetWorkflowExecutionHistoryRequest_builder{
			Namespace: startRequest.GetNamespace(),
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: startRequest.GetWorkflowId(),
				RunId:      startResponse.GetRunId(),
			}.Build(),
			WaitNewEvent:           true,
			HistoryEventFilterType: historyEventFilterType,
			NextPageToken:          nextToken,
		}.Build())
		s.Nil(historyErr)
		if len(historyResponse.GetNextPageToken()) == 0 {
			break
		}

		nextToken = historyResponse.GetNextPageToken()
	}

	startRequest = workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id2,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}.Build()

	_, err2 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), startRequest)
	s.NoError(err2)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.SetEarliestTime(timestamppb.New(startTime))
	startFilter.SetLatestTime(timestamppb.New(time.Now().UTC()))

	closedCount := 0
	openCount := 0

	var historyLength int64
	s.Eventually(
		func() bool {
			resp, err3 := s.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), workflowservice.ListClosedWorkflowExecutionsRequest_builder{
				Namespace:       s.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: startFilter,
				TypeFilter: filterpb.WorkflowTypeFilter_builder{
					Name: wt,
				}.Build(),
			}.Build())
			s.NoError(err3)
			closedCount = len(resp.GetExecutions())
			if closedCount == 1 {
				historyLength = resp.GetExecutions()[0].GetHistoryLength()
				s.Nil(resp.GetNextPageToken())
				return true
			}
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(1, closedCount)
	s.Equal(int64(5), historyLength)

	s.Eventually(
		func() bool {
			resp, err4 := s.FrontendClient().ListOpenWorkflowExecutions(testcore.NewContext(), workflowservice.ListOpenWorkflowExecutionsRequest_builder{
				Namespace:       s.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: startFilter,
				TypeFilter: filterpb.WorkflowTypeFilter_builder{
					Name: wt,
				}.Build(),
			}.Build())
			s.NoError(err4)
			openCount = len(resp.GetExecutions())
			if openCount == 1 {
				s.Nil(resp.GetNextPageToken())
				return true
			}
			s.Logger.Info("Open WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(1, openCount)
}
