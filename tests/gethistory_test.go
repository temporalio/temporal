package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type RawHistorySuite struct {
	testcore.FunctionalTestBase
}

type RawHistoryClientSuite struct {
	testcore.FunctionalTestBase
}

type GetHistoryFunctionalSuite struct {
	testcore.FunctionalTestBase
	EnableTransitionHistory bool
}

func TestRawHistorySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(RawHistorySuite))
}

func TestRawHistoryClientSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(RawHistoryClientSuite))
}

func TestGetHistoryFunctionalSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &GetHistoryFunctionalSuite{
				EnableTransitionHistory: tc.enableTransitionHistory,
			}
			suite.Run(t, s)
		})
	}
}

func (s *GetHistoryFunctionalSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.EnableTransitionHistory.Key(): s.EnableTransitionHistory,
		dynamicconfig.ExternalPayloadsEnabled.Key(): true,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *RawHistorySuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.SendRawWorkflowHistory.Key(): true,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *GetHistoryFunctionalSuite) TestGetWorkflowExecutionHistory_All() {
	workflowID := "functional-get-workflow-history-events-long-poll-test-all"
	workflowTypeName := "functional-get-workflow-history-events-long-poll-test-all-type"
	taskqueueName := "functional-get-workflow-history-events-long-poll-test-all-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistory := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*historypb.HistoryEvent, []byte) {
		responseInner, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essentially no relation with number of events.
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: 100,
			WaitNewEvent:    isLongPoll,
			NextPageToken:   token,
		})
		s.NoError(err)

		return responseInner.History.Events, responseInner.NextPageToken
	}

	var allEvents []*historypb.HistoryEvent
	var events []*historypb.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	events, token = getHistory(s.Namespace().String(), workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	events, token = getHistory(s.Namespace().String(), workflowID, token, true)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		events, token = getHistory(s.Namespace().String(), workflowID, token, true)
		allEvents = append(allEvents, events...)
	}
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, allEvents)

	// test non long poll
	allEvents = nil
	token = nil
	for {
		events, token = getHistory(s.Namespace().String(), workflowID, token, false)
		allEvents = append(allEvents, events...)
		if token == nil {
			break
		}
	}
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, allEvents)
}

// Note: not *RawHistorySuite. WHY???
func (s *GetHistoryFunctionalSuite) TestGetWorkflowExecutionHistory_Close() {
	workflowID := "functional-get-workflow-history-events-long-poll-test-close"
	workflowTypeName := "functional-get-workflow-history-events-long-poll-test-close-type"
	taskqueueName := "functional-get-workflow-history-events-long-poll-test-close-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *historypb.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(1),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              taskQueue,
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
				}},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistory := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*historypb.HistoryEvent, []byte) {
		closeEventOnly := enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
		responseInner, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize:        100,
			WaitNewEvent:           isLongPoll,
			NextPageToken:          token,
			HistoryEventFilterType: closeEventOnly,
		})

		s.NoError(err)
		return responseInner.History.Events, responseInner.NextPageToken
	}

	var events []*historypb.HistoryEvent
	var token []byte

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	events, token = getHistory(s.Namespace().String(), workflowID, token, true)
	s.True(time.Now().UTC().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	events, token = getHistory(s.Namespace().String(), workflowID, token, true)
	s.True(time.Now().UTC().After(start.Add(time.Second * 10)))
	// since we are only interested in close event
	s.Empty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		events, token = getHistory(s.Namespace().String(), workflowID, token, true)

		// since we are only interested in close event
		if token == nil {
			s.Equal(1, len(events))
			s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[0].EventType)
		} else {
			s.Empty(events)
		}
	}

	// test non long poll for only closed events
	token = nil
	for {
		events, token = getHistory(s.Namespace().String(), workflowID, token, false)
		if token == nil {
			break
		}
	}
	s.Equal(1, len(events))
	s.Logger.Info("Done TestGetWorkflowExecutionHistory_Close")
}

func (s *RawHistorySuite) TestGetWorkflowExecutionHistory_GetRawHistoryData() {
	workflowID := "functional-poll-for-workflow-raw-history-events-long-poll-test-all"
	workflowTypeName := "functional-poll-for-workflow-raw-history-events-long-poll-test-all-type"
	taskqueueName := "functional-poll-for-workflow-raw-history-events-long-poll-test-all-taskqueue"
	identity := "worker1"
	activityName := "activity_type1"

	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	taskQueue := &taskqueuepb.TaskQueue{Name: taskqueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// Start workflow execution
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.Nil(err0)

	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	// workflow logic
	activityScheduled := false
	activityData := int32(1)
	// var signalEvent *workflow.HistoryEvent
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {

		if !activityScheduled {
			activityScheduled = true
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityData))

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:             "1",
						ActivityType:           &commonpb.ActivityType{Name: activityName},
						TaskQueue:              taskQueue,
						Input:                  payloads.EncodeBytes(buf.Bytes()),
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(25 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(25 * time.Second),
					},
				},
			}}, nil
		}

		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}, nil
	}

	// activity handler
	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {

		return payloads.EncodeString("Activity Result."), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// this function poll events from history side
	getHistoryWithLongPoll := func(namespace string, workflowID string, token []byte, isLongPoll bool) ([]*commonpb.DataBlob, []byte) {
		responseInner, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// since the page size have essential no relation with number of events..
			// so just use a really larger number, to test whether long poll works
			MaximumPageSize: 100,
			WaitNewEvent:    isLongPoll,
			NextPageToken:   token,
		})
		s.Nil(err)
		return responseInner.RawHistory, responseInner.NextPageToken
	}

	getHistory := func(namespace string, workflowID string, token []byte) ([]*commonpb.DataBlob, []byte) {
		responseInner, err := s.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			MaximumPageSize: int32(100),
			NextPageToken:   token,
		})
		s.Nil(err)
		return responseInner.RawHistory, responseInner.NextPageToken
	}

	serializer := serialization.NewSerializer()
	convertBlob := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		events := []*historypb.HistoryEvent{}
		for _, blob := range blobs {
			s.True(blob.GetEncodingType() == enumspb.ENCODING_TYPE_PROTO3)
			blobEvents, err := serializer.DeserializeEvents(&commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var blobs []*commonpb.DataBlob
	var token []byte

	var allEvents []*historypb.HistoryEvent
	var events []*historypb.HistoryEvent

	// here do a long pull (which return immediately with at least the WorkflowExecutionStarted)
	start := time.Now().UTC()
	blobs, token = getHistoryWithLongPoll(s.Namespace().String(), workflowID, token, true)
	events = convertBlob(blobs)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// here do a long pull and check # of events and time elapsed
	// Make first command to schedule activity, this should affect the long poll above
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask1 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask1))
	})
	start = time.Now().UTC()
	blobs, token = getHistoryWithLongPoll(s.Namespace().String(), workflowID, token, true)
	events = convertBlob(blobs)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().After(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// finish the activity and poll all events
	time.AfterFunc(time.Second*5, func() {
		errActivity := poller.PollAndProcessActivityTask(false)
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errActivity))
	})
	time.AfterFunc(time.Second*8, func() {
		_, errWorkflowTask2 := poller.PollAndProcessWorkflowTask()
		s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(errWorkflowTask2))
	})
	for token != nil {
		blobs, token = getHistoryWithLongPoll(s.Namespace().String(), workflowID, token, true)
		events = convertBlob(blobs)
		allEvents = append(allEvents, events...)
	}

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, allEvents)

	// test non long poll
	allEvents = nil
	token = nil
	for {
		blobs, token = getHistory(s.Namespace().String(), workflowID, token)
		events = convertBlob(blobs)
		allEvents = append(allEvents, events...)
		if token == nil {
			break
		}
	}
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled
  6 ActivityTaskStarted
  7 ActivityTaskCompleted
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 WorkflowExecutionCompleted`, allEvents)
}

func (s *RawHistoryClientSuite) TestGetHistoryReverse() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	activityId := "heartbeat_retry"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)
		err1 := f1.Get(ctx1, nil)
		s.NoError(err1)

		return nil
	}

	s.Worker().RegisterActivity(activityFn)
	s.Worker().RegisterWorkflow(workflowFn)

	wfId := "functional-test-gethistoryreverse"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	err = workflowRun.Get(ctx, nil)
	s.NoError(err)

	wfeResponse, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.Nil(err)

	eventDefaultOrder := s.GetHistory(s.Namespace().String(), wfeResponse.WorkflowExecutionInfo.Execution)
	eventDefaultOrder = reverseSlice(eventDefaultOrder)

	events := s.getHistoryReverse(s.Namespace().String(), wfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(s.Namespace().String(), wfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(s.Namespace().String(), wfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Equal(len(eventDefaultOrder), len(events))
	s.Equal(eventDefaultOrder, events)
}

func (s *RawHistoryClientSuite) TestGetHistoryReverse_MultipleBranches() {
	activityFn := func(ctx context.Context) error {
		return nil
	}

	activityId := "functional-test-activity-gethistory-reverse-multiple-branches"
	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})

		var err1, err2 error

		f1 := workflow.ExecuteActivity(ctx1, activityFn)
		err1 = f1.Get(ctx1, nil)
		s.NoError(err1)

		s.NoError(workflow.Sleep(ctx, time.Second*2))

		f2 := workflow.ExecuteActivity(ctx1, activityFn)
		err2 = f2.Get(ctx1, nil)
		s.NoError(err2)

		return nil
	}

	s.Worker().RegisterActivity(activityFn)
	s.Worker().RegisterWorkflow(workflowFn)

	wfId := "functional-test-wf-gethistory-reverse-multiple-branches"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.True(workflowRun.GetRunID() != "")

	// we want to reset workflow in the middle of execution
	time.Sleep(time.Second) //nolint:forbidigo

	wfeResponse, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)

	rweResponse, err := s.SdkClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         wfeResponse.WorkflowExecutionInfo.Execution,
		Reason:                    "TestGetHistoryReverseBranch",
		WorkflowTaskFinishEventId: 10,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	resetRunId := rweResponse.GetRunId()
	resetWorkflowRun := s.SdkClient().GetWorkflow(ctx, wfId, resetRunId)
	err = resetWorkflowRun.Get(ctx, nil)
	s.NoError(err)

	resetWfeResponse, err := s.SdkClient().DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), resetWorkflowRun.GetRunID())
	s.NoError(err)

	eventsDefaultOrder := s.GetHistory(s.Namespace().String(), resetWfeResponse.WorkflowExecutionInfo.Execution)
	eventsDefaultOrder = reverseSlice(eventsDefaultOrder)

	events := s.getHistoryReverse(s.Namespace().String(), resetWfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(s.Namespace().String(), resetWfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(s.Namespace().String(), resetWfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Equal(len(eventsDefaultOrder), len(events))
	s.Equal(eventsDefaultOrder, events)
}

func reverseSlice(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events
}

func (s *RawHistoryClientSuite) getHistoryReverse(namespace string, execution *commonpb.WorkflowExecution, pageSize int32) []*historypb.HistoryEvent {
	historyResponse, err := s.FrontendClient().GetWorkflowExecutionHistoryReverse(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       namespace,
		Execution:       execution,
		NextPageToken:   nil,
		MaximumPageSize: pageSize,
	})
	s.Require().NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = s.FrontendClient().GetWorkflowExecutionHistoryReverse(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
			Namespace:       namespace,
			Execution:       execution,
			NextPageToken:   historyResponse.NextPageToken,
			MaximumPageSize: pageSize,
		})
		s.Require().NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *GetHistoryFunctionalSuite) TestGetWorkflowExecutionHistory_ExternalPayloadStats() {
	tv := testvars.New(s.T())

	workflowExternalPayloadSize := int64(1024)
	workflowInputPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
				{SizeBytes: workflowExternalPayloadSize},
			},
		}},
	}

	activityExternalPayloadSize := int64(2048)
	activityInputPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
				{SizeBytes: activityExternalPayloadSize},
			},
		}},
	}

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               workflowInputPayload,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)

	// Process first workflow task (schedules activity)
	_, err = s.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             "activity1",
							ActivityType:           &commonpb.ActivityType{Name: "TestActivity"},
							TaskQueue:              tv.TaskQueue(),
							Input:                  activityInputPayload,
							ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
							ScheduleToStartTimeout: durationpb.New(100 * time.Second),
							StartToCloseTimeout:    durationpb.New(50 * time.Second),
							HeartbeatTimeout:       durationpb.New(5 * time.Second),
						},
					},
				}},
			}, nil
		})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(int64(2), descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(workflowExternalPayloadSize+activityExternalPayloadSize, descResp.WorkflowExecutionInfo.ExternalPayloadSizeBytes)
}
