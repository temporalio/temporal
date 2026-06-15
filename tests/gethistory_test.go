package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type RawHistorySuite struct {
	parallelsuite.Suite[*RawHistorySuite]
}

func TestRawHistorySuite(t *testing.T) {
	parallelsuite.Run(t, &RawHistorySuite{})
}

type GetHistorySuite struct {
	parallelsuite.Suite[*GetHistorySuite]
}

func TestGetHistorySuite_DisableTransitionHistory(t *testing.T) {
	parallelsuite.Run(t, &GetHistorySuite{}, false)
}

func TestGetHistorySuite_EnableTransitionHistory(t *testing.T) {
	parallelsuite.Run(t, &GetHistorySuite{}, true)
}

func (s *RawHistorySuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.SendRawWorkflowHistory, true),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *GetHistorySuite) newTestEnv(enableTransitionHistory bool, opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableTransitionHistory, enableTransitionHistory),
		testcore.WithDynamicConfig(dynamicconfig.ExternalPayloadsEnabled, true),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *GetHistorySuite) TestGetWorkflowExecutionHistory_All(enableTransitionHistory bool) {
	env := s.newTestEnv(enableTransitionHistory)

	// Start workflow execution.
	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           env.Tv().RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          env.Tv().WorkflowID(),
		WorkflowType:        env.Tv().WorkflowType(),
		TaskQueue:           env.Tv().TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            env.Tv().WorkerIdentity(),
	})
	s.NoError(err)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowPoller := env.TaskPoller().PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: env.Tv().TaskQueue(),
	})
	activityPoller := env.TaskPoller().PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: env.Tv().TaskQueue(),
	})

	var allEvents []*historypb.HistoryEvent
	var events []*historypb.HistoryEvent
	var token []byte

	// Long polling returns immediately with at least WorkflowExecutionStarted.
	start := time.Now().UTC()
	events, token = s.getHistory(env, token, true, enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// Start long polling before scheduling the activity that should unblock it.
	type historyPage struct {
		events []*historypb.HistoryEvent
		token  []byte
		err    error
	}
	historyPageCh := make(chan historyPage, 1)
	go func(token []byte) {
		response, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: env.Tv().WorkflowID(),
			},
			MaximumPageSize:        100,
			WaitNewEvent:           true,
			NextPageToken:          token,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED,
		})
		if err != nil {
			historyPageCh <- historyPage{err: err}
			return
		}
		historyPageCh <- historyPage{events: response.History.Events, token: response.NextPageToken}
	}(token)

	_, scheduleActivityErr := workflowPoller.HandleTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             env.Tv().ActivityID(),
				ActivityType:           env.Tv().ActivityType(),
				TaskQueue:              env.Tv().TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
				ScheduleToStartTimeout: durationpb.New(25 * time.Second),
				StartToCloseTimeout:    durationpb.New(50 * time.Second),
				HeartbeatTimeout:       durationpb.New(25 * time.Second),
			}},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(scheduleActivityErr))
	s.NoError(scheduleActivityErr)

	page := <-historyPageCh
	s.NoError(page.err)
	events, token = page.events, page.token
	allEvents = append(allEvents, events...)
	s.NotEmpty(events)
	s.NotNil(token)

	// Finish the activity and poll all events.
	_, completeActivityErr := activityPoller.HandleTask(env.Tv(), func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{
			Result: payloads.EncodeString("Activity Result"),
		}, nil
	})
	env.Logger.Info("PollAndProcessActivityTask", tag.Error(completeActivityErr))
	s.NoError(completeActivityErr)

	_, completeWorkflowErr := workflowPoller.HandleTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(completeWorkflowErr))
	s.NoError(completeWorkflowErr)

	// Fetch history with long polling.
	for token != nil {
		events, token = s.getHistory(env, token, true, enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED)
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

	// Fetch history without long polling.
	allEvents = nil
	token = nil
	for {
		events, token = s.getHistory(env, token, false, enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED)
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

func (s *GetHistorySuite) TestGetWorkflowExecutionHistory_Close(enableTransitionHistory bool) {
	// Use a small long poll timeout to speed up the test.
	longPollTimeout := 5 * time.Second

	env := s.newTestEnv(enableTransitionHistory,
		testcore.WithDynamicConfig(dynamicconfig.HistoryLongPollExpirationInterval, longPollTimeout),
	)

	// Start workflow execution.
	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           env.Tv().RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          env.Tv().WorkflowID(),
		WorkflowType:        env.Tv().WorkflowType(),
		TaskQueue:           env.Tv().TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            env.Tv().WorkerIdentity(),
	})
	s.NoError(err)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	workflowPoller := env.TaskPoller().PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue: env.Tv().TaskQueue(),
	})
	activityPoller := env.TaskPoller().PollActivityTask(&workflowservice.PollActivityTaskQueueRequest{
		TaskQueue: env.Tv().TaskQueue(),
	})

	var events []*historypb.HistoryEvent
	var token []byte

	// Long polling for close events waits for the timeout while the workflow is still running.
	start := time.Now()
	events, token = s.getHistory(env, token, true, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	s.GreaterOrEqual(time.Since(start), longPollTimeout)
	s.Empty(events, "expected no events while waiting for a close event")
	s.NotNil(token)

	_, scheduleActivityErr := workflowPoller.HandleTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             env.Tv().ActivityID(),
				ActivityType:           env.Tv().ActivityType(),
				TaskQueue:              env.Tv().TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
				ScheduleToStartTimeout: durationpb.New(25 * time.Second),
				StartToCloseTimeout:    durationpb.New(50 * time.Second),
				HeartbeatTimeout:       durationpb.New(25 * time.Second),
			}},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(scheduleActivityErr))
	s.NoError(scheduleActivityErr)

	start = time.Now()
	events, token = s.getHistory(env, token, true, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
	s.GreaterOrEqual(time.Since(start), longPollTimeout)
	s.Empty(events, "expected no close event before workflow completion")
	s.NotNil(token)

	// Finish the activity and poll all events.
	_, completeActivityErr := activityPoller.HandleTask(env.Tv(), func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{
			Result: payloads.EncodeString("Activity Result"),
		}, nil
	})
	env.Logger.Info("PollAndProcessActivityTask", tag.Error(completeActivityErr))
	s.NoError(completeActivityErr)

	_, completeWorkflowErr := workflowPoller.HandleTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(completeWorkflowErr))
	s.NoError(completeWorkflowErr)

	s.Await(func(s *GetHistorySuite) {
		// Fetch close events without long polling.
		token = nil
		for {
			events, token = s.getHistory(env, token, false, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT)
			if token == nil {
				break
			}
		}

		s.Len(events, 1, "expected exactly one close event")
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, events[0].EventType)
	}, 5*time.Second, 100*time.Millisecond)
	env.Logger.Info("Done TestGetWorkflowExecutionHistory_Close")
}

func (s *RawHistorySuite) TestGetWorkflowExecutionHistory_GetRawHistoryData() {
	env := s.newTestEnv()

	// Start workflow execution.
	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           env.Tv().RequestID(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          env.Tv().WorkflowID(),
		WorkflowType:        env.Tv().WorkflowType(),
		TaskQueue:           env.Tv().TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            env.Tv().WorkerIdentity(),
	})
	s.NoError(err)

	env.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))

	poller := env.TaskPoller()
	getRawHistory := func(workflowID string, token []byte, isLongPoll bool) ([]*commonpb.DataBlob, []byte) {
		responseInner, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
			// Page size has no reliable relation to the number of returned events, so use a large value.
			MaximumPageSize: 100,
			WaitNewEvent:    isLongPoll,
			NextPageToken:   token,
		})
		s.NoError(err)
		return responseInner.RawHistory, responseInner.NextPageToken
	}
	deserializeHistoryBlobs := func(blobs []*commonpb.DataBlob) []*historypb.HistoryEvent {
		events := []*historypb.HistoryEvent{}
		for _, blob := range blobs {
			s.Equal(enumspb.ENCODING_TYPE_PROTO3, blob.GetEncodingType())
			blobEvents, err := serialization.DefaultDecoder.DeserializeEvents(&commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         blob.Data,
			})
			s.NoError(err)
			events = append(events, blobEvents...)
		}
		return events
	}

	var allEvents []*historypb.HistoryEvent

	// Long polling returns immediately with at least WorkflowExecutionStarted.
	start := time.Now().UTC()
	blobs, token := getRawHistory(env.Tv().WorkflowID(), nil, true)
	events := deserializeHistoryBlobs(blobs)
	allEvents = append(allEvents, events...)
	s.True(time.Now().UTC().Before(start.Add(time.Second * 5)))
	s.NotEmpty(events)
	s.NotNil(token)

	// Start long polling before scheduling the activity that should unblock it.
	type rawHistoryPage struct {
		blobs []*commonpb.DataBlob
		token []byte
		err   error
	}
	rawHistoryPageCh := make(chan rawHistoryPage, 1)
	go func(token []byte) {
		response, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: env.Tv().WorkflowID(),
			},
			// Page size has no reliable relation to the number of returned events, so use a large value.
			MaximumPageSize: 100,
			WaitNewEvent:    true,
			NextPageToken:   token,
		})
		if err != nil {
			rawHistoryPageCh <- rawHistoryPage{err: err}
			return
		}
		rawHistoryPageCh <- rawHistoryPage{blobs: response.RawHistory, token: response.NextPageToken}
	}(token)

	_, scheduleActivityErr := poller.PollAndHandleWorkflowTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             env.Tv().ActivityID(),
					ActivityType:           env.Tv().ActivityType(),
					TaskQueue:              env.Tv().TaskQueue(),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(25 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(25 * time.Second),
				},
			},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(scheduleActivityErr))
	s.NoError(scheduleActivityErr)

	page := <-rawHistoryPageCh
	s.NoError(page.err)
	blobs, token = page.blobs, page.token
	events = deserializeHistoryBlobs(blobs)
	allEvents = append(allEvents, events...)
	s.NotEmpty(events)
	s.NotNil(token)

	// Finish the activity and poll all events.
	_, completeActivityErr := poller.PollAndHandleActivityTask(env.Tv(), func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return &workflowservice.RespondActivityTaskCompletedRequest{
			Result: payloads.EncodeString("Activity Result."),
		}, nil
	})
	env.Logger.Info("PollAndProcessActivityTask", tag.Error(completeActivityErr))
	s.NoError(completeActivityErr)

	_, completeWorkflowErr := poller.PollAndHandleWorkflowTask(env.Tv(), func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("Done"),
				}},
		}}}, nil
	})
	env.Logger.Info("PollAndProcessWorkflowTask", tag.Error(completeWorkflowErr))
	s.NoError(completeWorkflowErr)
	for token != nil {
		blobs, token = getRawHistory(env.Tv().WorkflowID(), token, true)
		events = deserializeHistoryBlobs(blobs)
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

	// Fetch history without long polling.
	allEvents = nil
	token = nil
	for {
		blobs, token = getRawHistory(env.Tv().WorkflowID(), token, false)
		events = deserializeHistoryBlobs(blobs)
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

func (s *RawHistorySuite) TestGetHistoryReverse() {
	env := testcore.NewEnv(s.T())

	activityFn := func(ctx context.Context) error {
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             env.Tv().ActivityID(),
			ScheduleToStartTimeout: 2 * time.Second,
			StartToCloseTimeout:    2 * time.Second,
			RetryPolicy:            activityRetryPolicy,
		})
		f1 := workflow.ExecuteActivity(ctx1, activityFn)
		err1 := f1.Get(ctx1, nil)
		s.NoError(err1)

		return nil
	}

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	err = workflowRun.Get(s.Context(), nil)
	s.NoError(err)

	wfeResponse, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)

	eventDefaultOrder := env.GetHistory(env.Namespace().String(), wfeResponse.WorkflowExecutionInfo.Execution)
	slices.Reverse(eventDefaultOrder)

	events := s.getHistoryReverse(env, wfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Len(events, len(eventDefaultOrder))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(env, wfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Len(events, len(eventDefaultOrder))
	s.Equal(eventDefaultOrder, events)

	events = s.getHistoryReverse(env, wfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Len(events, len(eventDefaultOrder))
	s.Equal(eventDefaultOrder, events)
}

func (s *RawHistorySuite) TestGetHistoryReverse_MultipleBranches() {
	env := testcore.NewEnv(s.T())

	activityFn := func(ctx context.Context) error {
		return nil
	}

	workflowFn := func(ctx workflow.Context) error {
		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    time.Second * 2,
			BackoffCoefficient: 1,
			MaximumInterval:    time.Second * 2,
			MaximumAttempts:    3,
		}

		ctx1 := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             env.Tv().ActivityID(),
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

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		ID:                 env.Tv().WorkflowID(),
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	s.NotEmpty(workflowRun.GetRunID())

	// Reset workflow in the middle of execution.
	time.Sleep(time.Second) //nolint:forbidigo

	wfeResponse, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)

	rweResponse, err := env.SdkClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfeResponse.WorkflowExecutionInfo.Execution,
		Reason:                    "TestGetHistoryReverseBranch",
		WorkflowTaskFinishEventId: 10,
		RequestId:                 env.Tv().RequestID(),
	})
	s.NoError(err)

	resetRunId := rweResponse.GetRunId()
	resetWorkflowRun := env.SdkClient().GetWorkflow(s.Context(), env.Tv().WorkflowID(), resetRunId)
	err = resetWorkflowRun.Get(s.Context(), nil)
	s.NoError(err)

	resetWfeResponse, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), resetWorkflowRun.GetID(), resetWorkflowRun.GetRunID())
	s.NoError(err)

	eventsDefaultOrder := env.GetHistory(env.Namespace().String(), resetWfeResponse.WorkflowExecutionInfo.Execution)
	slices.Reverse(eventsDefaultOrder)

	events := s.getHistoryReverse(env, resetWfeResponse.WorkflowExecutionInfo.Execution, 100)
	s.Len(events, len(eventsDefaultOrder))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(env, resetWfeResponse.WorkflowExecutionInfo.Execution, 3)
	s.Len(events, len(eventsDefaultOrder))
	s.Equal(eventsDefaultOrder, events)

	events = s.getHistoryReverse(env, resetWfeResponse.WorkflowExecutionInfo.Execution, 1)
	s.Len(events, len(eventsDefaultOrder))
	s.Equal(eventsDefaultOrder, events)
}

func (s *RawHistorySuite) getHistoryReverse(env *testcore.TestEnv, execution *commonpb.WorkflowExecution, pageSize int32) []*historypb.HistoryEvent {
	historyResponse, err := env.FrontendClient().GetWorkflowExecutionHistoryReverse(s.Context(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
		Namespace:       env.Namespace().String(),
		Execution:       execution,
		NextPageToken:   nil,
		MaximumPageSize: pageSize,
	})
	s.NoError(err)

	events := historyResponse.History.Events
	for historyResponse.NextPageToken != nil {
		historyResponse, err = env.FrontendClient().GetWorkflowExecutionHistoryReverse(s.Context(), &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
			Namespace:       env.Namespace().String(),
			Execution:       execution,
			NextPageToken:   historyResponse.NextPageToken,
			MaximumPageSize: pageSize,
		})
		s.NoError(err)
		events = append(events, historyResponse.History.Events...)
	}

	return events
}

func (s *GetHistorySuite) TestGetWorkflowExecutionHistory_ExternalPayloadStats(enableTransitionHistory bool) {
	env := s.newTestEnv(enableTransitionHistory)

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

	we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          env.Tv().WorkflowID(),
		WorkflowType:        env.Tv().WorkflowType(),
		TaskQueue:           env.Tv().TaskQueue(),
		Input:               workflowInputPayload,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            env.Tv().WorkerIdentity(),
	})
	s.NoError(err)

	// Process the first workflow task, which schedules an activity.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(env.Tv(),
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             "activity1",
							ActivityType:           &commonpb.ActivityType{Name: "TestActivity"},
							TaskQueue:              env.Tv().TaskQueue(),
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

	descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: env.Tv().WorkflowID(),
			RunId:      we.GetRunId(),
		},
	})
	s.NoError(err)
	s.Equal(int64(2), descResp.WorkflowExecutionInfo.ExternalPayloadCount)
	s.Equal(workflowExternalPayloadSize+activityExternalPayloadSize, descResp.WorkflowExecutionInfo.ExternalPayloadSizeBytes)
}

func (s *GetHistorySuite) getHistory(
	env *testcore.TestEnv,
	token []byte,
	isLongPoll bool,
	eventFilter enumspb.HistoryEventFilterType,
) ([]*historypb.HistoryEvent, []byte) {
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: env.Tv().WorkflowID(),
		},
		// Page size has no reliable relation to the number of returned events, so use a large value.
		MaximumPageSize:        100,
		WaitNewEvent:           isLongPoll,
		NextPageToken:          token,
		HistoryEventFilterType: eventFilter,
	}
	responseInner, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), req)
	s.NoError(err)

	return responseInner.History.Events, responseInner.NextPageToken
}
