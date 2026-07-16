package tests

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type completionHandler struct {
	requestCh         chan *nexusrpc.CompletionRequest
	requestCompleteCh chan error
}

func (h *completionHandler) CompleteOperation(ctx context.Context, request *nexusrpc.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}

type CallbacksSuite struct {
	parallelsuite.Suite[*CallbacksSuite]
}

func TestCallbacksSuiteHSM(t *testing.T) {
	parallelsuite.Run(t, &CallbacksSuite{}, []testcore.TestOption{})
}

func TestCallbacksSuiteCHASM(t *testing.T) {
	parallelsuite.Run(t, &CallbacksSuite{}, []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
	})
}

func (s *CallbacksSuite) runNexusCompletionHTTPServer(t *testing.T, h *completionHandler) string {
	hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h})
	srv := httptest.NewServer(hh)
	t.Cleanup(func() {
		srv.Close()
	})
	return srv.URL
}

func (s *CallbacksSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	env := testcore.NewEnv(s.T(), opts...)
	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)
	return env
}

func (s *CallbacksSuite) TestScheduledCallbackTokenMigration_LegacyWriteEnvelopeRead(opts []testcore.TestOption) {
	testOpts := append([]testcore.TestOption{}, opts...)
	testOpts = append(
		testOpts,
		scheduleCommonOpts(s.T())...,
	)
	testOpts = append(
		testOpts,
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, false),
	)
	env := newScheduleEnv(s.T(), testOpts...)

	ctx := s.Context()
	sid := testcore.RandomizeStr("sched-token-migration")
	wid := testcore.RandomizeStr("sched-token-migration-wf")
	wt := testcore.RandomizeStr("sched-token-migration-wt")

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Second)}},
		},
		Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	_, err := env.FrontendClient().CreateSchedule(chasmContextFactory(ctx), &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	var startedWFID string
	await.RequireTruef(s.T(), func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(chasmContextFactory(ctx), &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		for _, a := range desc.GetInfo().GetRecentActions() {
			if wfid := a.GetStartWorkflowResult().GetWorkflowId(); wfid != "" {
				startedWFID = wfid
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "scheduler should start a scheduled action")
	s.NotEmpty(startedWFID)

	var token string
	for _, e := range env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: startedWFID}) {
		for _, cb := range e.GetWorkflowExecutionStartedEventAttributes().GetCompletionCallbacks() {
			if cb.GetNexus().GetUrl() == chasm.NexusCompletionHandlerURL {
				token = nexus.Header(cb.GetNexus().GetHeader()).Get(commonnexus.CallbackTokenHeader)
			}
		}
	}
	s.NotEmpty(token, "scheduled workflow must carry an internal completion callback token")
	_, reqID, decErr := chasm.UnpackNexusCallbackToken(token)
	s.NoError(decErr)
	s.Empty(reqID, "gate OFF must write a legacy token with no embedded request ID")

	env.OverrideDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true)

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: startedWFID},
		SignalName:        "continue",
	})
	s.NoError(err)

	await.RequireTruef(s.T(), func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(chasmContextFactory(ctx), &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		for _, a := range desc.GetInfo().GetRecentActions() {
			if a.GetStartWorkflowResult().GetWorkflowId() == startedWFID &&
				a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond,
		"scheduler must observe completion of the legacy-token action after enabling the envelope gate")
}

func (s *CallbacksSuite) TestWorkflowCallbacks_InvalidArgument(opts []testcore.TestOption) {
	workflowType := "test"

	cases := []struct {
		name    string
		urls    []string
		header  map[string]string
		message string
	}{
		{
			name:    "invalid-scheme",
			urls:    []string{"invalid"},
			message: "invalid url: unknown scheme: invalid",
		},
		{
			name:    "url-length-too-long",
			urls:    []string{"http://some-very-very-very-very-very-very-very-long-url"},
			message: "invalid url: url length longer than max length allowed of 50",
		},
		{
			name:    "header-size-too-large",
			urls:    []string{"http://some-ignored-address"},
			header:  map[string]string{"too": "long"},
			message: "invalid header: header size longer than max allowed size of 6",
		},
		{
			name:    "too many callbacks",
			urls:    []string{"http://url-1", "http://url-2", "http://url-3"},
			message: "cannot attach more than 2 callbacks to an execution",
		},
		{
			name:    "url not configured",
			urls:    []string{"http://some-unconfigured-address"},
			message: "invalid url: url does not match any configured callback address: http://some-unconfigured-address",
		},
		{
			name:    "https required",
			urls:    []string{"http://some-secure-address"},
			message: "invalid url: callback address does not allow insecure connections: http://some-secure-address",
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func(s *CallbacksSuite) {
			env := testcore.NewEnv(s.T(), opts...)
			env.OverrideDynamicConfig(dynamicconfig.FrontendCallbackURLMaxLength, 50)
			env.OverrideDynamicConfig(dynamicconfig.FrontendCallbackHeaderMaxSize, 6)
			env.OverrideDynamicConfig(dynamicconfig.MaxCallbacksPerWorkflow, 2)
			env.OverrideDynamicConfig(callback.MaxPerExecution, 2)
			env.OverrideDynamicConfig(
				callback.AllowedAddresses,
				[]any{map[string]any{"Pattern": "some-ignored-address", "AllowInsecure": true}, map[string]any{"Pattern": "some-secure-address", "AllowInsecure": false}},
			)

			taskQueue := testcore.RandomizeStr(s.T().Name())

			cbs := make([]*commonpb.Callback, 0, len(tc.urls))
			for _, url := range tc.urls {
				cbs = append(cbs, &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url:    url,
							Header: tc.header,
						},
					},
				})
			}
			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:           uuid.NewString(),
				Namespace:           env.Namespace().String(),
				WorkflowId:          testcore.RandomizeStr(s.T().Name()),
				WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               nil,
				WorkflowRunTimeout:  durationpb.New(100 * time.Second),
				Identity:            s.T().Name(),
				CompletionCallbacks: cbs,
			}

			_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), request)
			var invalidArgument *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgument)
			s.Equal(tc.message, err.Error())
		})
	}
}

func (s *CallbacksSuite) TestWorkflowNexusCallbacks_CarriedOver(opts []testcore.TestOption) {
	cases := []struct {
		name       string
		wf         func(workflow.Context) (int, error)
		runTimeout time.Duration
	}{
		{
			name: "ContinueAsNew",
			wf: func(ctx workflow.Context) (int, error) {
				if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, workflow.NewContinueAsNewError(ctx, "test")
				}
				return 666, nil
			},
			runTimeout: 100 * time.Second,
		},
		{
			name: "WorkflowRunTimeout",
			wf: func(ctx workflow.Context) (int, error) {
				info := workflow.GetInfo(ctx)
				if info.FirstRunID == info.WorkflowExecution.RunID {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, workflow.Sleep(ctx, 10*time.Second)
				}
				return 666, nil
			},
			runTimeout: 500 * time.Millisecond,
		},
		{
			name: "WorkflowFailureRetry",
			wf: func(ctx workflow.Context) (int, error) {
				info := workflow.GetInfo(ctx)
				if info.FirstRunID == info.WorkflowExecution.RunID {
					workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
					return 0, errors.New("intentional workflow failure")
				}
				return 666, nil
			},
			runTimeout: 100 * time.Second,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func(s *CallbacksSuite) {
			env := s.newTestEnv(opts...)

			ctx := s.Context()
			sdkClient := env.SdkClient()

			workflowType := "test"
			workflowID := env.Tv().WorkflowID()

			ch := &completionHandler{
				requestCh:         make(chan *nexusrpc.CompletionRequest, 2),
				requestCompleteCh: make(chan error, 2),
			}
			defer func() {
				close(ch.requestCh)
				close(ch.requestCompleteCh)
			}()
			callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

			env.SdkWorker().RegisterWorkflowWithOptions(tc.wf, workflow.RegisterOptions{Name: workflowType})

			links := []*commonpb.Link{
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  env.Namespace().String(),
							WorkflowId: "some-caller-wfid-1",
							RunId:      "some-caller-runid-1",
						},
					},
				},
				{
					Variant: &commonpb.Link_WorkflowEvent_{
						WorkflowEvent: &commonpb.Link_WorkflowEvent{
							Namespace:  env.Namespace().String(),
							WorkflowId: "some-caller-wfid-2",
							RunId:      "some-caller-runid-2",
						},
					},
				},
			}

			cbs := []*commonpb.Callback{
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: callbackAddress + "/cb1",
						},
					},
					Links: []*commonpb.Link{links[0]},
				},
				{
					Variant: &commonpb.Callback_Nexus_{
						Nexus: &commonpb.Callback_Nexus{
							Url: callbackAddress + "/cb2",
						},
					},
					Links: []*commonpb.Link{links[1]},
				},
			}

			request := &workflowservice.StartWorkflowExecutionRequest{
				RequestId:          uuid.NewString(),
				Namespace:          env.Namespace().String(),
				WorkflowId:         workflowID,
				WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
				TaskQueue:          &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:              nil,
				WorkflowRunTimeout: durationpb.New(tc.runTimeout),
				Identity:           s.T().Name(),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(1 * time.Second),
					MaximumInterval:    durationpb.New(1 * time.Second),
					BackoffCoefficient: 1,
				},
				CompletionCallbacks: []*commonpb.Callback{cbs[0]},
				Links:               []*commonpb.Link{links[0]},
			}

			response1, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
			s.NoError(err)

			workflowExecution := &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      response1.RunId,
			}

			// Send another request to attach callback
			request2 := proto.Clone(request).(*workflowservice.StartWorkflowExecutionRequest)
			request2.RequestId = uuid.NewString()
			request2.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			request2.OnConflictOptions = &workflowpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			}
			request2.CompletionCallbacks = []*commonpb.Callback{cbs[1]}
			request2.Links = []*commonpb.Link{links[1]}

			response2, err := env.FrontendClient().StartWorkflowExecution(ctx, request2)
			s.NoError(err)
			s.False(response2.Started)
			s.Equal(workflowExecution.RunId, response2.RunId)

			_, err = env.FrontendClient().SignalWorkflowExecution(
				ctx,
				&workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         env.Namespace().String(),
					WorkflowExecution: workflowExecution,
					SignalName:        "continue",
				},
			)
			s.NoError(err)

			// Wait for workflow to complete.
			run := sdkClient.GetWorkflow(ctx, workflowID, "")
			s.NoError(run.Get(ctx, nil))

			numAttempts := 2
			for attempt := 1; attempt <= numAttempts; attempt++ {
				for range cbs {
					completion := <-ch.requestCh
					s.Equal(nexus.OperationStateSucceeded, completion.State)
					var result int
					s.NoError(completion.Result.Consume(&result))
					s.Equal(666, result)
				}

				for range cbs {
					var err error
					if attempt < numAttempts {
						// force retry
						err = nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional error")
					}
					ch.requestCompleteCh <- err
				}

				getHistoryResponse, err := env.FrontendClient().GetWorkflowExecutionHistory(
					ctx,
					&workflowservice.GetWorkflowExecutionHistoryRequest{
						Namespace: env.Namespace().String(),
						Execution: &commonpb.WorkflowExecution{
							WorkflowId: workflowID,
						},
						MaximumPageSize: 1, // only interested in the start event
					},
				)
				s.NoError(err)
				startEvent := s.RequireHistoryEvent(
					getHistoryResponse.History.Events,
					enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				)

				// Start event links is empty since it's deduped.
				s.Empty(startEvent.Links)
				startEventAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
				s.NotNil(startEventAttr)
				// Start event contains all callbacks attached to the first workflow.
				s.ProtoElementsMatch(cbs, startEventAttr.CompletionCallbacks)

				await.Require(s.Context(), s.T(), func(col *await.T) {
					description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
					require.NoError(col, err)
					require.Len(col, description.Callbacks, len(cbs))
					descCbs := make([]*commonpb.Callback, 0, len(description.Callbacks))
					for _, callbackInfo := range description.Callbacks {
						protorequire.ProtoEqual(
							col,
							&workflowpb.CallbackInfo_Trigger{
								Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{
									WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{},
								},
							},
							callbackInfo.Trigger,
						)
						require.Equal(col, int32(attempt), callbackInfo.Attempt)
						// Loose check to see that this is set.
						require.Greater(
							col,
							callbackInfo.LastAttemptCompleteTime.AsTime(),
							time.Now().Add(-time.Hour),
						)
						if attempt < numAttempts {
							require.Equal(col, enumspb.CALLBACK_STATE_BACKING_OFF, callbackInfo.State)
							require.Equal(
								col,
								"handler error (INTERNAL): intentional error",
								callbackInfo.LastAttemptFailure.Message,
							)
						} else {
							require.Equal(col, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
							require.Nil(col, callbackInfo.LastAttemptFailure)
						}
						descCbs = append(descCbs, callbackInfo.Callback)
					}
					protoassert.ProtoElementsMatch(col, cbs, descCbs)
				}, 2*time.Second, 100*time.Millisecond)
			}
		})
	}
}

func (s *CallbacksSuite) TestNexusResetWorkflowWithCallback(opts []testcore.TestOption) {
	env := s.newTestEnv(opts...)

	ctx := s.Context()
	sdkClient := env.SdkClient()

	taskQueue := &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workflowID := env.Tv().WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 2),
		requestCompleteCh: make(chan error, 2),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

	// A workflow that completes once it has been reset.
	longRunningWorkflow := func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool {
			info := workflow.GetInfo(ctx)

			return info.OriginalRunID != info.WorkflowExecution.RunID
		})
	}

	env.SdkWorker().RegisterWorkflowWithOptions(longRunningWorkflow, workflow.RegisterOptions{
		Name: "longRunningWorkflow",
	})

	cbs := []*commonpb.Callback{
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: callbackAddress + "/cb1",
				},
			},
		},
		{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: callbackAddress + "/cb2",
				},
			},
		},
	}

	request1 := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "longRunningWorkflow"},
		TaskQueue:           taskQueue,
		Input:               nil,
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{cbs[0]},
	}

	startResponse1, err := env.FrontendClient().StartWorkflowExecution(ctx, request1)
	s.NoError(err)

	// Get history, iterate to ensure workflow task completed event exists.
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      startResponse1.RunId,
	}
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
  			2 WorkflowTaskScheduled
  			3 WorkflowTaskStarted
  			4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Try starting another workflow, which will have the callback attached to the previous workflow.
	request2 := proto.Clone(request1).(*workflowservice.StartWorkflowExecutionRequest)
	request2.RequestId = uuid.NewString()
	request2.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	request2.OnConflictOptions = &workflowpb.OnConflictOptions{
		AttachRequestId:           true,
		AttachCompletionCallbacks: true,
	}
	request2.CompletionCallbacks = []*commonpb.Callback{cbs[1]}

	startResponse2, err := env.FrontendClient().StartWorkflowExecution(ctx, request2)
	s.NoError(err)
	s.False(startResponse2.Started)
	s.Equal(workflowExecution.RunId, startResponse2.RunId)

	// Get history, iterate to ensure workflow execution options updated event exists.
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
  			2 WorkflowTaskScheduled
  			3 WorkflowTaskStarted
  			4 WorkflowTaskCompleted
     		5 WorkflowExecutionOptionsUpdated`,
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Reset workflow must copy all callbacks even after the reset point.
	resetWfResponse, err := sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),

		WorkflowExecution:         workflowExecution,
		Reason:                    "TestNexusResetWorkflowWithCallback",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	// Get the description of the run that was reset and ensure that its callback is still in STANDBY state.
	description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, startResponse1.RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, description.WorkflowExecutionInfo.Status)

	// Should not be invoked during a reset
	s.Len(description.Callbacks, len(cbs))
	descCbs := make([]*commonpb.Callback, 0, len(description.Callbacks))
	for _, callbackInfo := range description.Callbacks {
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfo.State)
		s.Equal(int32(0), callbackInfo.Attempt)
		descCbs = append(descCbs, callbackInfo.Callback)
	}
	s.ProtoElementsMatch(cbs, descCbs)

	resetWorkflowRun := sdkClient.GetWorkflow(ctx, workflowID, resetWfResponse.RunId)
	err = resetWorkflowRun.Get(ctx, nil)
	s.NoError(err)

	for range cbs {
		select {
		case completion := <-ch.requestCh:
			s.Equal(nexus.OperationStateSucceeded, completion.State)
		case <-time.After(time.Second):
			s.Fail("timeout waiting for callback")
		}
		select {
		case ch.requestCompleteCh <- nil:
		case <-time.After(time.Second):
			s.Fail("timeout writing to completion channel")
		}
	}

	await.Require(s.Context(), s.T(),
		func(t *await.T) {
			// Get the description of the run post-reset and ensure its callbacks are in SUCCEEDED
			// state.
			description, err = sdkClient.DescribeWorkflowExecution(ctx, resetWorkflowRun.GetID(), "")
			require.NoError(t, err)
			require.Equal(
				t,
				enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				description.WorkflowExecutionInfo.Status,
			)

			require.Len(t, description.Callbacks, len(cbs))
			descCbs = make([]*commonpb.Callback, 0, len(description.Callbacks))
			for _, callbackInfo := range description.Callbacks {
				require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
				descCbs = append(descCbs, callbackInfo.Callback)
			}
			protoassert.ProtoElementsMatch(t, cbs, descCbs)
		},
		2*time.Second,
		100*time.Millisecond,
	)
}

func blockingWorkflow(ctx workflow.Context) error {
	return workflow.Await(ctx, func() bool {
		return false
	})
}

func (s *CallbacksSuite) TestNexusResetWorkflowWithCallback_ResetToNotBaseRun(opts []testcore.TestOption) {
	env := s.newTestEnv(opts...)

	/*
	 * 1. Start WF w/ no callbacks and immediately terminate
	 * 2. Start WF second time w/ a callback
	 * 3. Reset WF back to the first run
	 * 4. Verify callback is called
	 */

	ctx := s.Context()

	taskQueue := &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	workflowID := env.Tv().WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(s.T(), ch)

	env.SdkWorker().RegisterWorkflow(blockingWorkflow)

	// 1. Start WF w/ no callbacks and immediately terminate
	request1 := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         workflowID,
		WorkflowType:       &commonpb.WorkflowType{Name: "blockingWorkflow"},
		TaskQueue:          taskQueue,
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(20 * time.Second),
		Identity:           s.T().Name(),
	}

	startResponse1, err := env.FrontendClient().StartWorkflowExecution(ctx, request1)
	s.NoError(err)

	// Validate the workflow started, then terminate it
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      startResponse1.RunId,
	}
	s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: workflowExecution,
		Reason:            s.T().Name(),
		Identity:          env.Tv().WorkerIdentity(),
	})
	s.NoError(err)

	// 2. Start WF second time w/ callbacks (new run)
	cbs := []*commonpb.Callback{
		{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: callbackAddress + "/cb1"}}},
	}

	request2 := proto.Clone(request1).(*workflowservice.StartWorkflowExecutionRequest)
	request2.RequestId = uuid.NewString()
	request2.CompletionCallbacks = cbs

	_, err = env.FrontendClient().StartWorkflowExecution(ctx, request2)
	s.NoError(err)

	// 3. Reset workflow back to the first (terminated) run as base; must copy callbacks
	_, err = env.SdkClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         workflowExecution, // base = first (terminated) run
		Reason:                    s.T().Name(),
		WorkflowTaskFinishEventId: 4,
		RequestId:                 "test_id",
	})
	s.NoError(err)

	// 4. Wait for callback deliveries via the handler channels
	select {
	case completion := <-ch.requestCh:
		s.Equal(nexus.OperationStateFailed, completion.State)
		ch.requestCompleteCh <- nil
	case <-ctx.Done():
		s.FailNow("timed out waiting for callback")
	}

	// Ensure the original workflow runs to completion to avoid leaving dangling runs
	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
		},
		Reason:   s.T().Name(),
		Identity: env.Tv().WorkerIdentity(),
	})
	s.NoError(err)
}
