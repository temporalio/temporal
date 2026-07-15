package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkflowVisibilityTestSuite struct {
	parallelsuite.Suite[*WorkflowVisibilityTestSuite]
}

func TestWorkflowVisibilityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowVisibilityTestSuite{})
}

func (s *WorkflowVisibilityTestSuite) TestVisibility() {
	env, _ := testcore.NewEnv(s.T())
	startTime := time.Now().UTC()

	// Start 2 workflow executions
	id1 := "functional-visibility-test1"
	id2 := "functional-visibility-test2"
	wt := "functional-visibility-test-type"
	tl := "functional-visibility-test-taskqueue"
	identity := "worker1"

	startRequest := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id1,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	startResponse, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), startRequest)
	s.NoError(err0)

	// Now complete one of the executions
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	_, err1 := poller.PollAndProcessWorkflowTask()
	s.NoError(err1)

	// wait until the start workflow is done
	var nextToken []byte
	historyEventFilterType := enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	for {
		historyResponse, historyErr := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: startRequest.Namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: startRequest.WorkflowId,
				RunId:      startResponse.RunId,
			},
			WaitNewEvent:           true,
			HistoryEventFilterType: historyEventFilterType,
			NextPageToken:          nextToken,
		})
		s.NoError(historyErr)
		if len(historyResponse.NextPageToken) == 0 {
			break
		}

		nextToken = historyResponse.NextPageToken
	}

	startRequest = &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id2,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	_, err2 := env.FrontendClient().StartWorkflowExecution(s.Context(), startRequest)
	s.NoError(err2)

	startFilter := &filterpb.StartTimeFilter{}
	startFilter.EarliestTime = timestamppb.New(startTime)
	startFilter.LatestTime = timestamppb.New(time.Now().UTC())

	closedCount := 0
	openCount := 0

	var historyLength int64
	s.Eventually(
		func() bool {
			resp, err3 := env.FrontendClient().ListClosedWorkflowExecutions(s.Context(), &workflowservice.ListClosedWorkflowExecutionsRequest{
				Namespace:       env.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: startFilter,
				Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_TypeFilter{
					TypeFilter: &filterpb.WorkflowTypeFilter{
						Name: wt,
					},
				},
			})
			s.NoError(err3)
			closedCount = len(resp.Executions)
			if closedCount == 1 {
				historyLength = resp.Executions[0].HistoryLength
				return true
			}
			env.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(1, closedCount)
	s.Equal(int64(5), historyLength)

	s.Eventually(
		func() bool {
			resp, err4 := env.FrontendClient().ListOpenWorkflowExecutions(s.Context(), &workflowservice.ListOpenWorkflowExecutionsRequest{
				Namespace:       env.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: startFilter,
				Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{
					TypeFilter: &filterpb.WorkflowTypeFilter{
						Name: wt,
					},
				},
			})
			s.NoError(err4)
			openCount = len(resp.Executions)
			if openCount == 1 {
				return true
			}
			env.Logger.Info("Open WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.Equal(1, openCount)
}
