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
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkflowMemoTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowMemoTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowMemoTestSuite))
}

type RunIdGetter interface {
	GetRunId() string
}
type startFunc func() (RunIdGetter, error)

func (s *WorkflowMemoTestSuite) TestStartWithMemo() {
	id := "functional-start-with-memo-test"
	wt := "functional-start-with-memo-test-type"
	tl := "functional-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := commonpb.Memo_builder{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString("memo-value"),
		},
	}.Build()

	request := workflowservice.StartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
		Memo:                memo,
	}.Build()

	fn := func() (RunIdGetter, error) {
		return s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(), memo, `
  1 WorkflowExecutionStarted {"Memo":{"Fields":{"Info":{"Data":"\"memo-value\""}}}}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`)
}

func (s *WorkflowMemoTestSuite) TestSignalWithStartWithMemo() {
	id := "functional-signal-with-start-with-memo-test"
	wt := "functional-signal-with-start-with-memo-test-type"
	tl := "functional-signal-with-start-with-memo-test-taskqueue"
	identity := "worker1"

	memo := commonpb.Memo_builder{
		Fields: map[string]*commonpb.Payload{
			"Info": payload.EncodeString("memo-value"),
		},
	}.Build()

	signalName := "my signal"
	signalInput := payloads.EncodeString("my signal input")
	request := workflowservice.SignalWithStartWorkflowExecutionRequest_builder{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        commonpb.WorkflowType_builder{Name: wt}.Build(),
		TaskQueue:           taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		SignalName:          signalName,
		SignalInput:         signalInput,
		Identity:            identity,
		Memo:                memo,
	}.Build()

	fn := func() (RunIdGetter, error) {
		return s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
	}
	s.startWithMemoHelper(fn, id, taskqueuepb.TaskQueue_builder{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(), memo, `
  1 WorkflowExecutionStarted {"Memo":{"Fields":{"Info":{"Data":"\"memo-value\""}}}}
  2 WorkflowExecutionSignaled
  3 WorkflowTaskScheduled
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted`)
}

// helper function for TestStartWithMemo and TestSignalWithStartWithMemo to reduce duplicate code
func (s *WorkflowMemoTestSuite) startWithMemoHelper(startFn startFunc, id string, taskQueue *taskqueuepb.TaskQueue, memo *commonpb.Memo, expectedHistory string) {
	identity := "worker1"

	we, err0 := startFn()
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

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
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// verify open visibility
	var openExecutionInfo *workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			resp, err1 := s.FrontendClient().ListOpenWorkflowExecutions(testcore.NewContext(), workflowservice.ListOpenWorkflowExecutionsRequest_builder{
				Namespace:       s.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: filterpb.StartTimeFilter_builder{
					EarliestTime: nil,
					LatestTime:   timestamppb.New(time.Now().UTC()),
				}.Build(),
				ExecutionFilter: filterpb.WorkflowExecutionFilter_builder{
					WorkflowId: id,
				}.Build(),
			}.Build())
			s.NoError(err1)
			if len(resp.GetExecutions()) == 1 {
				openExecutionInfo = resp.GetExecutions()[0]
				return true
			}
			s.Logger.Info("Open WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.NotNil(openExecutionInfo)
	s.ProtoEqual(memo, openExecutionInfo.GetMemo())

	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}.Build()

	// verify DescribeWorkflowExecution result: workflow running
	descRequest := workflowservice.DescribeWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: execution,
	}.Build()
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	s.NoError(err)
	s.ProtoEqual(memo, descResp.GetWorkflowExecutionInfo().GetMemo())

	// make progress of workflow
	_, err = poller.PollAndProcessWorkflowTask()
	s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	// verify history
	historyEvents := s.GetHistory(s.Namespace().String(), execution)
	s.EqualHistoryEvents(expectedHistory, historyEvents)

	// verify DescribeWorkflowExecution result: workflow closed, but close visibility task not completed
	descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	s.NoError(err)
	s.ProtoEqual(memo, descResp.GetWorkflowExecutionInfo().GetMemo())

	// verify closed visibility
	var closedExecutionInfo *workflowpb.WorkflowExecutionInfo
	s.Eventually(
		func() bool {
			resp, err1 := s.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), workflowservice.ListClosedWorkflowExecutionsRequest_builder{
				Namespace:       s.Namespace().String(),
				MaximumPageSize: 100,
				StartTimeFilter: filterpb.StartTimeFilter_builder{
					EarliestTime: nil,
					LatestTime:   timestamppb.New(time.Now().UTC()),
				}.Build(),
				ExecutionFilter: filterpb.WorkflowExecutionFilter_builder{
					WorkflowId: id,
				}.Build(),
			}.Build())
			s.NoError(err1)
			if len(resp.GetExecutions()) == 1 {
				closedExecutionInfo = resp.GetExecutions()[0]
				return true
			}
			s.Logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	s.NotNil(closedExecutionInfo)
	s.ProtoEqual(memo, closedExecutionInfo.GetMemo())

	// verify DescribeWorkflowExecution result: workflow closed and close visibility task completed
	descResp, err = s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	s.NoError(err)
	s.ProtoEqual(memo, descResp.GetWorkflowExecutionInfo().GetMemo())
}
