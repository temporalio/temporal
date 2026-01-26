package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyv1 "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RunIdGetter interface {
	GetRunId() string
}
type startFunc func() (RunIdGetter, error)

func TestWorkflowMemo(t *testing.T) {
	t.Run("StartWithMemo", func(t *testing.T) {
		s := testcore.NewEnv(t)

		id := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tl := s.Tv().TaskQueue().Name
		identity := "worker1"

		memo := &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"Info": payload.EncodeString("memo-value"),
			},
		}

		request := &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: wt},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			Identity:            identity,
			Memo:                memo,
		}

		fn := func() (RunIdGetter, error) {
			return s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
		}
		startWithMemoHelper(t, s, s.Namespace().String(), s.Logger, fn, id, &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}, memo, `
  1 WorkflowExecutionStarted {"Memo":{"Fields":{"Info":{"Data":"\"memo-value\""}}}}
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`)
	})

	t.Run("SignalWithStartWithMemo", func(t *testing.T) {
		s := testcore.NewEnv(t)

		id := s.Tv().WorkflowID()
		wt := s.Tv().WorkflowType().Name
		tl := s.Tv().TaskQueue().Name
		identity := "worker1"

		memo := &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"Info": payload.EncodeString("memo-value"),
			},
		}

		signalName := "my signal"
		signalInput := payloads.EncodeString("my signal input")
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           s.Namespace().String(),
			WorkflowId:          id,
			WorkflowType:        &commonpb.WorkflowType{Name: wt},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:               nil,
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(1 * time.Second),
			SignalName:          signalName,
			SignalInput:         signalInput,
			Identity:            identity,
			Memo:                memo,
		}

		fn := func() (RunIdGetter, error) {
			return s.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), request)
		}
		startWithMemoHelper(t, s, s.Namespace().String(), s.Logger, fn, id, &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}, memo, `
  1 WorkflowExecutionStarted {"Memo":{"Fields":{"Info":{"Data":"\"memo-value\""}}}}
  2 WorkflowExecutionSignaled
  3 WorkflowTaskScheduled
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted`)
	})
}

// helper function for TestStartWithMemo and TestSignalWithStartWithMemo to reduce duplicate code
func startWithMemoHelper[T interface {
	FrontendClient() workflowservice.WorkflowServiceClient
	GetHistory(namespace string, execution *commonpb.WorkflowExecution) []*historyv1.HistoryEvent
	EqualHistoryEvents(expected string, actual []*historyv1.HistoryEvent)
	ProtoEqual(expected, actual proto.Message)
	Eventually(condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...interface{})
}](t *testing.T, env T, namespace string, logger log.Logger, startFn startFunc, id string, taskQueue *taskqueuepb.TaskQueue, memo *commonpb.Memo, expectedHistory string) {
	identity := "worker1"

	we, err0 := startFn()
	require.NoError(t, err0)

	logger.Info("StartWorkflowExecution: response", tag.WorkflowRunID(we.GetRunId()))

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
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              logger,
		T:                   t,
	}

	// verify open visibility
	var openExecutionInfo *workflowpb.WorkflowExecutionInfo
	env.Eventually(
		func() bool {
			resp, err1 := env.FrontendClient().ListOpenWorkflowExecutions(testcore.NewContext(), &workflowservice.ListOpenWorkflowExecutionsRequest{
				Namespace:       namespace,
				MaximumPageSize: 100,
				StartTimeFilter: &filterpb.StartTimeFilter{
					EarliestTime: nil,
					LatestTime:   timestamppb.New(time.Now().UTC()),
				},
				Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
					WorkflowId: id,
				}},
			})
			require.NoError(t, err1)
			if len(resp.Executions) == 1 {
				openExecutionInfo = resp.Executions[0]
				return true
			}
			logger.Info("Open WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	require.NotNil(t, openExecutionInfo)
	env.ProtoEqual(memo, openExecutionInfo.Memo)

	execution := &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}

	// verify DescribeWorkflowExecution result: workflow running
	descRequest := &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: execution,
	}
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	require.NoError(t, err)
	env.ProtoEqual(memo, descResp.WorkflowExecutionInfo.Memo)

	// make progress of workflow
	_, err = poller.PollAndProcessWorkflowTask()
	logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	require.NoError(t, err)

	// verify history
	historyEvents := env.GetHistory(namespace, execution)
	env.EqualHistoryEvents(expectedHistory, historyEvents)

	// verify DescribeWorkflowExecution result: workflow closed, but close visibility task not completed
	descResp, err = env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	require.NoError(t, err)
	env.ProtoEqual(memo, descResp.WorkflowExecutionInfo.Memo)

	// verify closed visibility
	var closedExecutionInfo *workflowpb.WorkflowExecutionInfo
	env.Eventually(
		func() bool {
			resp, err1 := env.FrontendClient().ListClosedWorkflowExecutions(testcore.NewContext(), &workflowservice.ListClosedWorkflowExecutionsRequest{
				Namespace:       namespace,
				MaximumPageSize: 100,
				StartTimeFilter: &filterpb.StartTimeFilter{
					EarliestTime: nil,
					LatestTime:   timestamppb.New(time.Now().UTC()),
				},
				Filters: &workflowservice.ListClosedWorkflowExecutionsRequest_ExecutionFilter{ExecutionFilter: &filterpb.WorkflowExecutionFilter{
					WorkflowId: id,
				}},
			})
			require.NoError(t, err1)
			if len(resp.Executions) == 1 {
				closedExecutionInfo = resp.Executions[0]
				return true
			}
			logger.Info("Closed WorkflowExecution is not yet visible")
			return false
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
	require.NotNil(t, closedExecutionInfo)
	env.ProtoEqual(memo, closedExecutionInfo.Memo)

	// verify DescribeWorkflowExecution result: workflow closed and close visibility task completed
	descResp, err = env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), descRequest)
	require.NoError(t, err)
	env.ProtoEqual(memo, descResp.WorkflowExecutionInfo.Memo)
}
