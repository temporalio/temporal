package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ContinueAsNewTestSuite struct {
	parallelsuite.Suite[*ContinueAsNewTestSuite]
}

func TestContinueAsNewTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ContinueAsNewTestSuite{})
}

func (s *ContinueAsNewTestSuite) TestContinueAsNewWorkflow() {
	env := testcore.NewEnv(s.T())

	id := "functional-continue-as-new-workflow-test"
	wt := "functional-continue-as-new-workflow-test-type"
	tl := "functional-continue-as-new-workflow-test-taskqueue"
	identity := "worker1"
	saName := "CustomKeywordField"
	// Uncomment this line to test with mapper.
	// saName = "AliasForCustomKeywordField"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	header := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("sample payload")},
	}
	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{"memoKey": payload.EncodeString("memoVal")},
	}
	searchAttr := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			saName: sadefs.MustEncodeValue("random", enumspb.INDEXED_VALUE_TYPE_KEYWORD),
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		Header:              header,
		Memo:                memo,
		SearchAttributes:    searchAttr,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(10)
	continueAsNewCounter := int32(0)
	var previousRunID string
	var currentRunID string
	var lastRunStartedEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		currentRunID = task.WorkflowExecution.GetRunId()
		if continueAsNewCounter < continueAsNewCount {
			previousRunID = currentRunID
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               payloads.EncodeBytes(buf.Bytes()),
						Header:              header,
						Memo:                memo,
						SearchAttributes:    searchAttr,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}}, nil
		}

		lastRunStartedEvent = task.History.Events[0]
		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	for i := range 10 {
		_, err := poller.PollAndProcessWorkflowTask()
		env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err, strconv.Itoa(i))
	}

	s.False(workflowComplete)
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	s.NoError(err)
	s.True(workflowComplete)
	s.NotNil(lastRunStartedEvent)
	lastRunStartedEventAttrs := lastRunStartedEvent.GetWorkflowExecutionStartedEventAttributes()
	lastRunStartedEventSearchAttrs := lastRunStartedEventAttrs.GetSearchAttributes()
	s.Equal(previousRunID, lastRunStartedEventAttrs.GetContinuedExecutionRunId())
	// top-level workflow doesn't have parent, and root is itself (nil in history event)
	s.Nil(lastRunStartedEventAttrs.GetParentWorkflowExecution())
	s.Nil(lastRunStartedEventAttrs.GetRootWorkflowExecution())
	s.ProtoEqual(header, lastRunStartedEventAttrs.Header)
	s.ProtoEqual(memo, lastRunStartedEventAttrs.Memo)
	s.Equal(
		searchAttr.GetIndexedFields()[saName].GetData(),
		lastRunStartedEventSearchAttrs.GetIndexedFields()[saName].GetData(),
	)
	s.Equal(
		"Keyword",
		string(lastRunStartedEventSearchAttrs.GetIndexedFields()[saName].GetMetadata()["type"]),
	)
	s.Equal(we.RunId, lastRunStartedEventAttrs.GetFirstExecutionRunId())

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: id,
		},
	})
	s.NoError(err)
	s.Equal(currentRunID, descResp.WorkflowExecutionInfo.Execution.GetRunId())
	s.Equal(we.RunId, descResp.WorkflowExecutionInfo.GetFirstRunId())
}

func (s *ContinueAsNewTestSuite) TestContinueAsNewRunTimeout() {
	env := testcore.NewEnv(s.T())

	id := "functional-continue-as-new-workflow-run-timeout-test"
	wt := "functional-continue-as-new-workflow-run-timeout-test-type"
	tl := "functional-continue-as-new-workflow-run-timeout-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowComplete := false
	continueAsNewCount := int32(1)
	continueAsNewCounter := int32(0)
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if continueAsNewCounter < continueAsNewCount {
			continueAsNewCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               payloads.EncodeBytes(buf.Bytes()),
						WorkflowRunTimeout:  durationpb.New(1 * time.Second), // set timeout to 1
						WorkflowTaskTimeout: durationpb.New(1 * time.Second),
					},
				},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// process the workflow task and continue as new
	_, err := poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)

	s.False(workflowComplete)

	//nolint:forbidigo
	time.Sleep(1 * time.Second) // wait 1 second for timeout

	var historyEvents []*historypb.HistoryEvent
	for range 20 {
		historyEvents = env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: id,
		})
		lastEvent := historyEvents[len(historyEvents)-1]
		if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
			env.Logger.Warn("Execution not timedout yet")
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
			continue
		}

		workflowComplete = true
		break
	}
	s.True(workflowComplete)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionTimedOut`, historyEvents)
}

func (s *ContinueAsNewTestSuite) TestContinueAsNewRunExecutionTimeout() {
	env := testcore.NewEnv(s.T())

	id := "functional-continue-as-new-workflow-execution-timeout-test"
	wt := "functional-continue-as-new-workflow-execution-timeout-test-type"
	tl := "functional-continue-as-new-workflow-execution-timeout-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               id,
		WorkflowType:             workflowType,
		TaskQueue:                taskQueue,
		Input:                    nil,
		WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		Identity:                 identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
				ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           taskQueue,
					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				},
			},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	testCompleted := make(chan struct{})
	go func() {
		for {
			select {
			case <-testCompleted:
				return
			default:
				// process the workflow task and continue as new
				_, err := poller.PollAndProcessWorkflowTask(testcore.WithoutRetries)
				env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))

				// rely on WorkflowIdReuseMinimalInterval to prevent tight loop of continue as new
			}
		}
	}()

	s.Eventually(
		func() bool {
			descResp, err := env.FrontendClient().DescribeWorkflowExecution(
				s.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: id,
					},
				},
			)
			s.NoError(err)
			return descResp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT &&
				descResp.GetWorkflowExecutionInfo().Execution.GetRunId() != we.RunId // validate that workflow did continue as new
		},
		time.Second*10,
		time.Millisecond*50,
	)

	close(testCompleted)
}

func (s *ContinueAsNewTestSuite) TestWorkflowContinueAsNewTaskID() {
	env := testcore.NewEnv(s.T())

	id := "functional-wf-continue-as-new-task-id-test"
	wt := "functional-wf-continue-as-new-task-id-type"
	tl := "functional-wf-continue-as-new-task-id-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	var executions []*commonpb.WorkflowExecution

	continueAsNewed := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		executions = append(executions, task.WorkflowExecution)

		if !continueAsNewed {
			continueAsNewed = true
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						Input:               nil,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(1 * time.Second),
					},
				},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("succeed"),
				},
			},
		}}, nil

	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	minTaskID := int64(0)
	_, err := poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events := env.GetHistory(env.Namespace().String(), executions[0])
	s.NotEmpty(events)
	for _, event := range events {
		s.Greater(event.GetTaskId(), minTaskID)
		minTaskID = event.GetTaskId()
	}

	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
	events = env.GetHistory(env.Namespace().String(), executions[1])
	s.NotEmpty(events)
	for _, event := range events {
		s.Greater(event.GetTaskId(), minTaskID)
		minTaskID = event.GetTaskId()
	}
}

// TestContinueAsNewWithDelayStart verifies that the server honors a "delay start" on a
// continue-as-new command: the first workflow task of the new run is deferred by the requested
// interval instead of being scheduled immediately.
//
// The SDKs do not yet expose a delay-start option on the continue-as-new command, so we set the
// BackoffStartInterval field directly on the raw ContinueAsNewWorkflowExecutionCommandAttributes.
// The task poller sends this command over gRPC verbatim, which is how we simulate an SDK that does
// support the flag.
func (s *ContinueAsNewTestSuite) TestContinueAsNewWithDelayStart() {
	env := testcore.NewEnv(s.T())

	// Disable the minimal continue-as-new interval so the only backoff applied to the new run is the
	// one we explicitly request. Otherwise the server enforces WorkflowIdReuseMinimalInterval and the
	// observed backoff could come from there rather than from our requested delay.
	env.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0))

	id := "functional-continue-as-new-delay-start-test"
	wt := "functional-continue-as-new-delay-start-test-type"
	tl := "functional-continue-as-new-delay-start-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	const delayStart = 1 * time.Second

	continueAsNewed := false
	workflowComplete := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		if !continueAsNewed {
			continueAsNewed = true
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
						ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
							WorkflowType:        workflowType,
							TaskQueue:           taskQueue,
							WorkflowRunTimeout:  durationpb.New(100 * time.Second),
							WorkflowTaskTimeout: durationpb.New(10 * time.Second),
							// "delay start": SDKs don't expose this yet, so we set it on the raw command.
							BackoffStartInterval: durationpb.New(delayStart),
						},
					},
				}},
			}, nil
		}

		workflowComplete = true
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Done"),
					},
				},
			}},
		}, nil
	}

	tv := testvars.New(s.T()).WithTaskQueue(tl)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// Process the first workflow task: it issues a continue-as-new with a delayed start.
	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)
	s.False(workflowComplete)

	// The original run's history must record the requested backoff on the continue-as-new event.
	firstRunEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.RunId})
	continuedAsNewEvent := firstRunEvents[len(firstRunEvents)-1]
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, continuedAsNewEvent.GetEventType())
	canAttrs := continuedAsNewEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(delayStart, canAttrs.GetBackoffStartInterval().AsDuration())
	newRunID := canAttrs.GetNewExecutionRunId()
	s.NotEmpty(newRunID)
	s.NotEqual(we.RunId, newRunID)

	newRunExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: newRunID}

	// Process the new run's first workflow task. The poller long-polls and blocks until the task
	// becomes available, which only happens once the delay-start backoff timer fires (~delayStart
	// later). Long-polling here keeps the test deterministic: we don't have to inspect intermediate
	// history to observe that the task was deferred.
	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.NoError(err)
	s.True(workflowComplete)

	// The new run started with the requested first-workflow-task backoff and ran to completion.
	finalEvents := env.GetHistory(env.Namespace().String(), newRunExecution)
	startedAttrs := finalEvents[0].GetWorkflowExecutionStartedEventAttributes()
	s.Equal(delayStart, startedAttrs.GetFirstWorkflowTaskBackoff().AsDuration())
	s.Equal(we.RunId, startedAttrs.GetContinuedExecutionRunId())
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 WorkflowExecutionCompleted`, finalEvents)

	// The first workflow task of the new run was scheduled only after the delay-start backoff: its
	// scheduled-event timestamp is delayStart after the run's start event.
	startedTime := finalEvents[0].GetEventTime().AsTime()
	scheduledTime := finalEvents[1].GetEventTime().AsTime()
	s.GreaterOrEqual(scheduledTime.Sub(startedTime), delayStart)
}

type (
	ParentWithChildContinueAsNew struct {
		suite *ContinueAsNewTestSuite
		env   *testcore.TestEnv

		parentID           string
		parentType         string
		childID            string
		childType          string
		parentWorkflowType *commonpb.WorkflowType
		childWorkflowType  *commonpb.WorkflowType
		closePolicy        enumspb.ParentClosePolicy

		childComplete         bool
		childExecutionStarted bool
		childData             int32
		continueAsNewCount    int32
		continueAsNewCounter  int32
		startedEvent          *historypb.HistoryEvent
		completedEvent        *historypb.HistoryEvent
		childStartedEvents    []*historypb.HistoryEvent
	}
)

func newParentWithChildContinueAsNew(
	s *ContinueAsNewTestSuite,
	env *testcore.TestEnv,
	parentID, parentType, childID, childType string,
	closePolicy enumspb.ParentClosePolicy,
) *ParentWithChildContinueAsNew {
	workflow := &ParentWithChildContinueAsNew{
		suite:       s,
		env:         env,
		parentID:    parentID,
		parentType:  parentType,
		childID:     childID,
		childType:   childType,
		closePolicy: closePolicy,

		childComplete:         false,
		childExecutionStarted: false,
		childData:             int32(1),
		continueAsNewCount:    int32(10),
		continueAsNewCounter:  int32(0),
	}
	workflow.parentWorkflowType = &commonpb.WorkflowType{}
	workflow.parentWorkflowType.Name = parentType

	workflow.childWorkflowType = &commonpb.WorkflowType{}
	workflow.childWorkflowType.Name = childType

	return workflow
}

func (w *ParentWithChildContinueAsNew) workflow(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
	w.env.Logger.Info(
		"Processing workflow task for WorkflowId:",
		tag.WorkflowID(task.WorkflowExecution.GetWorkflowId()),
	)

	// Child workflow logic
	if task.WorkflowExecution.GetWorkflowId() == w.childID {
		if task.PreviousStartedEventId <= 0 {
			w.childStartedEvents = append(w.childStartedEvents, task.History.Events[0])
		}

		if w.continueAsNewCounter < w.continueAsNewCount {
			w.continueAsNewCounter++
			buf := new(bytes.Buffer)
			w.suite.NoError(binary.Write(buf, binary.LittleEndian, w.continueAsNewCounter))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						Input: payloads.EncodeBytes(buf.Bytes()),
					},
				},
			}}, nil
		}

		w.childComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Child Done"),
				},
			},
		}}, nil
	}

	// Parent workflow logic
	if task.WorkflowExecution.GetWorkflowId() == w.parentID {
		if !w.childExecutionStarted {
			w.env.Logger.Info("Starting child execution")
			w.childExecutionStarted = true
			buf := new(bytes.Buffer)
			w.suite.NoError(binary.Write(buf, binary.LittleEndian, w.childData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:         w.env.Namespace().String(),
						WorkflowId:        w.childID,
						WorkflowType:      w.childWorkflowType,
						Input:             payloads.EncodeBytes(buf.Bytes()),
						ParentClosePolicy: w.closePolicy,
					},
				},
			}}, nil
		} else if task.PreviousStartedEventId > 0 {
			for _, event := range task.History.Events[task.PreviousStartedEventId:] {
				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
					w.startedEvent = event
					return []*commandpb.Command{}, nil
				}

				if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
					w.completedEvent = event
					return []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("Done"),
							},
						},
					}}, nil
				}
			}
		}
	}

	return nil, nil
}

func (s *ContinueAsNewTestSuite) TestChildWorkflowWithContinueAsNew() {
	env := testcore.NewEnv(s.T())

	parentID := "functional-child-workflow-with-continue-as-new-test-parent"
	childID := "functional-child-workflow-with-continue-as-new-test-child"
	wtParent := "functional-child-workflow-with-continue-as-new-test-parent-type"
	wtChild := "functional-child-workflow-with-continue-as-new-test-child-type"
	tl := "functional-child-workflow-with-continue-as-new-test-taskqueue"
	identity := "worker1"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	definition := newParentWithChildContinueAsNew(
		s,
		env,
		parentID,
		wtParent,
		childID,
		wtChild,
		enumspb.PARENT_CLOSE_POLICY_ABANDON,
	)

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        definition.parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: definition.workflow,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Make first command to start child execution
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := range 11 {
		env.Logger.Info("workflow task", tag.Counter(i))
		_, err = poller.PollAndProcessWorkflowTask()
		env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	s.Len(definition.childStartedEvents, 10)
	for _, childStartedEvent := range definition.childStartedEvents {
		s.NotNil(childStartedEvent)
		childStartedEventAttrs := childStartedEvent.GetWorkflowExecutionStartedEventAttributes()
		s.NotNil(childStartedEventAttrs.GetRootWorkflowExecution())
		s.Equal(parentID, childStartedEventAttrs.RootWorkflowExecution.GetWorkflowId())
		s.Equal(we.GetRunId(), childStartedEventAttrs.RootWorkflowExecution.GetRunId())
	}

	s.False(definition.childComplete)
	s.NotNil(definition.startedEvent)

	// Process Child Execution final workflow task to complete it
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childComplete)

	// Process ChildExecution completed event and complete parent execution
	_, err = poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.NotNil(definition.completedEvent)
	completedAttributes := definition.completedEvent.GetChildWorkflowExecutionCompletedEventAttributes()
	s.Equal(env.Namespace().String(), completedAttributes.Namespace)
	s.Equal(env.NamespaceID().String(), completedAttributes.NamespaceId)
	s.NotEmpty(completedAttributes.Namespace)
	s.Equal(childID, completedAttributes.WorkflowExecution.WorkflowId)
	s.NotEqual(
		definition.startedEvent.GetChildWorkflowExecutionStartedEventAttributes().WorkflowExecution.RunId,
		completedAttributes.WorkflowExecution.RunId,
	)
	s.Equal(wtChild, completedAttributes.WorkflowType.Name)
	var childResult string
	s.NoError(payloads.Decode(completedAttributes.GetResult(), &childResult))
	s.Equal("Child Done", childResult)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 StartChildWorkflowExecutionInitiated
  6 ChildWorkflowExecutionStarted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 ChildWorkflowExecutionCompleted
 11 WorkflowTaskScheduled
 12 WorkflowTaskStarted
 13 WorkflowTaskCompleted
 14 WorkflowExecutionCompleted`, env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: parentID,
		RunId:      we.RunId,
	}))
}

func (s *ContinueAsNewTestSuite) TestChildWorkflowWithContinueAsNewParentTerminate() {
	env := testcore.NewEnv(s.T())

	parentID := "functional-child-workflow-with-continue-as-new-parent-terminate-test-parent"
	childID := "functional-child-workflow-with-continue-as-new-parent-terminate-test-child"
	wtParent := "functional-child-workflow-with-continue-as-new-parent-terminate-test-parent-type"
	wtChild := "functional-child-workflow-with-continue-as-new-parent-terminate-test-child-type"
	tl := "functional-child-workflow-with-continue-as-new-parent-terminate-test-taskqueue"
	identity := "worker1"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	definition := newParentWithChildContinueAsNew(
		s,
		env,
		parentID,
		wtParent,
		childID,
		wtChild,
		enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	)

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          parentID,
		WorkflowType:        definition.parentWorkflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err0 := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err0)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: definition.workflow,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Make first command to start child execution
	_, err := poller.PollAndProcessWorkflowTask()
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
	s.NoError(err)
	s.True(definition.childExecutionStarted)

	// Process ChildExecution Started event and all generations of child executions
	for i := range 11 {
		env.Logger.Info("workflow task", tag.Counter(i))
		_, err = poller.PollAndProcessWorkflowTask()
		env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
		s.NoError(err)
	}

	s.False(definition.childComplete)
	s.NotNil(definition.startedEvent)

	// Terminate parent workflow execution which should also trigger terminate of child due to parent close policy
	_, err = env.FrontendClient().TerminateWorkflowExecution(
		s.Context(),
		&workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentID,
			},
		},
	)
	s.NoError(err)

	parentDescribeResp, err := env.FrontendClient().DescribeWorkflowExecution(
		s.Context(),
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: parentID,
			},
		},
	)
	s.NoError(err)
	s.NotNil(parentDescribeResp.WorkflowExecutionInfo.CloseTime)

	env.Logger.Info(fmt.Sprintf("Parent Status: %v", parentDescribeResp.WorkflowExecutionInfo.Status))

	var childDescribeResp *workflowservice.DescribeWorkflowExecutionResponse
	// Retry 10 times to wait for child to be terminated due to transfer task processing to enforce parent close policy
	for range 10 {
		childDescribeResp, err = env.FrontendClient().DescribeWorkflowExecution(
			s.Context(),
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: childID,
				},
			},
		)
		s.NoError(err)

		// Check if child is terminated
		if childDescribeResp.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			break
		}

		// Wait for child to be terminated by background transfer task processing
		time.Sleep(time.Second) //nolint:forbidigo
	}
	s.Equal(
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		childDescribeResp.WorkflowExecutionInfo.Status,
		"expected child to be terminated",
	)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 StartChildWorkflowExecutionInitiated
  6 ChildWorkflowExecutionStarted
  7 WorkflowTaskScheduled
  8 WorkflowTaskStarted
  9 WorkflowTaskCompleted
 10 WorkflowExecutionTerminated`, env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: parentID,
		RunId:      we.RunId,
	}))

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowExecutionTerminated`, env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: childID,
	}))
}

func (s *ContinueAsNewTestSuite) TestContinueAsNewWithInternalTaskQueue_Blocked() {
	env := testcore.NewEnv(s.T())

	id := testcore.RandomizeStr(s.T().Name())
	wt := "test-continue-as-new-internal-taskqueue-type"
	tl := "test-continue-as-new-internal-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
	s.NoError(err)
	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// Workflow logic: try to continue as new on internal task queue
	tv := testvars.New(s.T()).WithTaskQueue(tl)
	continueAsNewAttempted := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		env.Logger.Info("Processing workflow task", tag.WorkflowID(task.WorkflowExecution.WorkflowId))

		if !continueAsNewAttempted {
			env.Logger.Info("Attempting to continue as new on internal task queue")
			continueAsNewAttempted = true

			commands := []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType: workflowType,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: primitives.PerNSWorkerTaskQueue,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}}
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: commands,
			}, nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}

	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// Process workflow task - should fail when trying to continue as new on internal task queue
	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	s.Error(err, "Expected error when continuing as new on internal task queue")
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.Contains(err.Error(), "internal per-namespace task queue")

	// Wait a bit for the workflow task failed event to be written to history
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// Verify workflow task failed
	historyEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})

	var foundTaskFailed bool
	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			foundTaskFailed = true
			attrs := event.GetWorkflowTaskFailedEventAttributes()
			s.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_CONTINUE_AS_NEW_ATTRIBUTES, attrs.GetCause())
			s.Contains(attrs.GetFailure().GetMessage(), "internal per-namespace task queue")
			break
		}
	}
	s.True(foundTaskFailed, "WorkflowTaskFailed event should be recorded")
}

// TestStartAfterContinueAsNew_FirstExecutionRunId verifies that once a workflow has been continued
// as new, subsequent StartWorkflowExecution calls (both the USE_EXISTING response and the FAIL
// WorkflowExecutionAlreadyStarted error) report the original (head-of-chain) run id in
// first_execution_run_id, even though the current run id is the post-CAN run.
func (s *ContinueAsNewTestSuite) TestStartAfterContinueAsNew_FirstExecutionRunId() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0))

	id := "functional-start-after-can-first-run-test"
	wt := "functional-start-after-can-first-run-test-type"
	tl := "functional-start-after-can-first-run-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}

	we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), startReq)
	s.NoError(err)
	s.Equal(we0.RunId, we0.FirstExecutionRunId, "brand-new start should set FirstExecutionRunId to RunId")

	// Drive exactly one CAN. After the WT completes, the second run is RUNNING with no further
	// pollers, so the subsequent StartWorkflowExecution calls below land on a live workflow.
	tv := testvars.New(s.T()).WithTaskQueue(tl)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}},
		}, nil
	})
	s.NoError(err)

	// Confirm the post-CAN run is current and distinct from the original.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)
	currentRunID := descResp.WorkflowExecutionInfo.Execution.GetRunId()
	s.NotEqual(we0.RunId, currentRunID, "post-CAN run id should differ from original")
	s.Equal(we0.RunId, descResp.WorkflowExecutionInfo.GetFirstRunId())

	// FAIL conflict policy: error should expose the original run id as the first execution run id.
	failReq := proto.Clone(startReq).(*workflowservice.StartWorkflowExecutionRequest)
	failReq.RequestId = uuid.NewString()
	failReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	_, err = env.FrontendClient().StartWorkflowExecution(s.Context(), failReq)
	var already *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &already)
	s.Equal(currentRunID, already.RunId, "AlreadyStarted error should reference the current run")
	s.Equal(we0.RunId, already.FirstExecutionRunId, "AlreadyStarted error should expose head-of-chain run id")

	// USE_EXISTING dedup: response carries the original run id as first_execution_run_id.
	useExistingReq := proto.Clone(startReq).(*workflowservice.StartWorkflowExecutionRequest)
	useExistingReq.RequestId = uuid.NewString()
	useExistingReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we2, err := env.FrontendClient().StartWorkflowExecution(s.Context(), useExistingReq)
	s.NoError(err)
	s.False(we2.Started)
	s.Equal(currentRunID, we2.RunId)
	s.Equal(we0.RunId, we2.FirstExecutionRunId)
}

// TestResetOfNonCurrentRunFromDifferentChain_FirstExecutionRunId covers the interesting case where
// the workflow has two distinct chains (chain 1: original run A; chain 2: a fresh start B after A
// terminated), and the caller resets A — a non-current run from a *different* chain than the
// currently-current B. The reset run D inherits its first_execution_run_id from A's chain (A),
// not from B. After the reset, dedup responses against the workflow id should report A as the
// FirstExecutionRunId, even though the workflow's most recent prior chain head was B.
func (s *ContinueAsNewTestSuite) TestResetOfNonCurrentRunFromDifferentChain_FirstExecutionRunId() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0))

	id := "functional-reset-non-current-different-chain-test"
	wt := "functional-reset-non-current-different-chain-test-type"
	tl := "functional-reset-non-current-different-chain-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Chain 1: start run A and complete it.
	chain1Req := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}
	we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), chain1Req)
	s.NoError(err)
	chain1RunID := we0.RunId

	tv := testvars.New(s.T()).WithTaskQueue(tl)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("chain1-done"),
					},
				},
			}},
		}, nil
	})
	s.NoError(err)

	// Chain 2: start run B as a brand-new chain (chain1's run is already completed, so this is a
	// new chain with B as its head).
	chain2Req := proto.Clone(chain1Req).(*workflowservice.StartWorkflowExecutionRequest)
	chain2Req.RequestId = uuid.NewString()
	we1, err := env.FrontendClient().StartWorkflowExecution(s.Context(), chain2Req)
	s.NoError(err)
	chain2RunID := we1.RunId
	s.NotEqual(chain1RunID, chain2RunID)
	s.Equal(chain2RunID, we1.FirstExecutionRunId, "chain 2 run B should be its own chain head")

	// Sanity: workflow's current chain head is B.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)
	s.Equal(chain2RunID, desc.WorkflowExecutionInfo.Execution.GetRunId())
	s.Equal(chain2RunID, desc.WorkflowExecutionInfo.GetFirstRunId())

	// Locate the WorkflowTaskCompleted event in chain1's history to use as reset point.
	chain1Events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: id, RunId: chain1RunID})
	var wtCompletedEventID int64
	for _, ev := range chain1Events {
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wtCompletedEventID = ev.GetEventId()
		}
	}
	s.NotZero(wtCompletedEventID)

	// Reset chain1's non-current run A → produces a new run D that inherits A's chain head.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id, RunId: chain1RunID},
		Reason:                    "reset non-current run from different chain",
		WorkflowTaskFinishEventId: wtCompletedEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRunID := resetResp.RunId
	s.NotEqual(chain1RunID, resetRunID)
	s.NotEqual(chain2RunID, resetRunID)

	// After reset, workflow's chain head switches from chain 2's head (B) back to chain 1's head (A).
	desc, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)
	s.Equal(resetRunID, desc.WorkflowExecutionInfo.Execution.GetRunId())
	s.Equal(chain1RunID, desc.WorkflowExecutionInfo.GetFirstRunId(), "reset run should inherit chain1's head, not chain2's")

	// USE_EXISTING dedup against the reset run: FirstExecutionRunId tracks the reset's chain (A),
	// not the prior chain (B).
	useExistingReq := proto.Clone(chain1Req).(*workflowservice.StartWorkflowExecutionRequest)
	useExistingReq.RequestId = uuid.NewString()
	useExistingReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we2, err := env.FrontendClient().StartWorkflowExecution(s.Context(), useExistingReq)
	s.NoError(err)
	s.False(we2.Started)
	s.Equal(resetRunID, we2.RunId)
	s.Equal(chain1RunID, we2.FirstExecutionRunId)
	s.NotEqual(chain2RunID, we2.FirstExecutionRunId, "chain switched back to chain1 after reset")
}

// TestResetOfNonCurrentRunAfterContinueAsNew_FirstExecutionRunId covers the case where the workflow
// has continued-as-new (so a different run is current) and the caller resets the *original*
// (non-current) run. The resulting reset run becomes current; its first_execution_run_id should
// still report the head of the chain (the original run id), and so should subsequent
// Start/dedup interactions against the resulting workflow.
func (s *ContinueAsNewTestSuite) TestResetOfNonCurrentRunAfterContinueAsNew_FirstExecutionRunId() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0))

	id := "functional-reset-non-current-after-can-test"
	wt := "functional-reset-non-current-after-can-test-type"
	tl := "functional-reset-non-current-after-can-test-taskqueue"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	startReq := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	}
	we0, err := env.FrontendClient().StartWorkflowExecution(s.Context(), startReq)
	s.NoError(err)
	originalRunID := we0.RunId

	// Drive one CAN so the original run becomes non-current.
	tv := testvars.New(s.T()).WithTaskQueue(tl)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType:        workflowType,
						TaskQueue:           taskQueue,
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
					},
				},
			}},
		}, nil
	})
	s.NoError(err)

	// Locate the WorkflowTaskCompleted event in the original (non-current) run to use as reset point.
	originalEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: id, RunId: originalRunID})
	var wtCompletedEventID int64
	for _, ev := range originalEvents {
		if ev.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wtCompletedEventID = ev.GetEventId()
		}
	}
	s.NotZero(wtCompletedEventID)

	// Reset the *non-current* original run.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: id, RunId: originalRunID},
		Reason:                    "reset non-current run after CAN",
		WorkflowTaskFinishEventId: wtCompletedEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRunID := resetResp.RunId
	s.NotEqual(originalRunID, resetRunID)

	// The reset run is now current. The chain head must still be the original run id.
	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)
	s.Equal(resetRunID, desc.WorkflowExecutionInfo.Execution.GetRunId())
	s.Equal(originalRunID, desc.WorkflowExecutionInfo.GetFirstRunId(), "reset of non-current run should preserve chain head")

	// USE_EXISTING: response references the reset run id, FirstExecutionRunId == original head.
	useExistingReq := proto.Clone(startReq).(*workflowservice.StartWorkflowExecutionRequest)
	useExistingReq.RequestId = uuid.NewString()
	useExistingReq.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	we2, err := env.FrontendClient().StartWorkflowExecution(s.Context(), useExistingReq)
	s.NoError(err)
	s.False(we2.Started)
	s.Equal(resetRunID, we2.RunId)
	s.Equal(originalRunID, we2.FirstExecutionRunId)
}
