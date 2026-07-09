// The MIT License (MIT)
//
// Copyright (c) 2024 Temporal Technologies Inc.  ALL RIGHTS RESERVED.
//
// See NOTICE.md for full restrictions.

package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type SignalSuggestCANTestSuite struct {
	parallelsuite.Suite[*SignalSuggestCANTestSuite]
}

func TestSignalSuggestCANTestSuiteLegacy(t *testing.T) {
	parallelsuite.Run(t, &SignalSuggestCANTestSuite{}, []testcore.TestOption{})
}

// maxSignals and threshold below are chosen so the suggest boundary is ceil(5*0.6)=3: a third
// signal flips SuggestContinueAsNew on with the TOO_MANY_SIGNALS reason, while the hard limit
// (5) still rejects the sixth signal. Both knobs are namespace-scoped, so NewEnv applies them as
// a namespace constraint on the shared cluster (no dedicated cluster required).
const (
	signalSuggestMaxSignals = 5
	signalSuggestThreshold  = 0.6
	signalSuggestBoundary   = 3 // ceil(5 * 0.6)
	signalSuggestSignalName = "suggest-can-signal"
	signalSuggestWorkflow   = "signal-suggest-can-type"
	signalSuggestTaskQueue  = "signal-suggest-can-taskqueue"
)

func (s *SignalSuggestCANTestSuite) TestSignalSuggestCAN(opts []testcore.TestOption) {
	env := testcore.NewEnv(s.T(), append(opts,
		testcore.WithDynamicConfig(dynamicconfig.MaximumSignalsPerExecution, signalSuggestMaxSignals),
		testcore.WithDynamicConfig(dynamicconfig.MaximumSignalsPerExecutionSuggestContinueAsNewThreshold, signalSuggestThreshold),
	)...)
	identity := "worker1"
	id := "functional-signal-suggest-can-test-" + uuid.NewString()
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id}
	taskQueue := &taskqueuepb.TaskQueue{Name: signalSuggestTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	// The workflow stays open (completes only on a "finish" signal) so we can send signals and
	// observe successive WorkflowTaskStarted attributes as SignalCount climbs.
	startResp, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: signalSuggestWorkflow},
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)
	workflowExecution.RunId = startResp.RunId

	suggestCANSeen := false
	signalReasonSeen := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		for _, event := range task.History.Events {
			if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
				continue
			}
			attrs := event.GetWorkflowTaskStartedEventAttributes()
			if attrs.GetSuggestContinueAsNew() {
				suggestCANSeen = true
				for _, reason := range attrs.GetSuggestContinueAsNewReasons() {
					if reason == enumspb.SUGGEST_CONTINUE_AS_NEW_REASON_TOO_MANY_SIGNALS {
						signalReasonSeen = true
					}
				}
			}
		}
		// Complete the workflow once the finish signal has been received.
		for _, event := range task.History.Events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED &&
				event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName() == "finish" {
				return []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
					Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("Done"),
					}},
				}}, nil
			}
		}
		return []*commandpb.Command{}, nil
	}
	//nolint:staticcheck // SA1019 TaskPoller replacement needed
	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	// Drain the initial workflow task.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send signals up to the suggest boundary (ceil(max*threshold)=3). Each signal below the hard
	// limit (5) must be accepted and must produce a workflow task whose started event we inspect.
	for i := 0; i < signalSuggestBoundary; i++ {
		_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        signalSuggestSignalName,
			Input:             payloads.EncodeString("signal"),
			Identity:          identity,
		})
		s.NoError(err)
		_, err = poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}
	// After `boundary` signals the suggestion should have fired.
	s.True(suggestCANSeen, "expected SuggestContinueAsNew=true once SignalCount reached the boundary")
	s.True(signalReasonSeen, "expected SUGGEST_CONTINUE_AS_NEW_REASON_TOO_MANY_SIGNALS in reasons")

	// INV-2: the hard limit still rejects beyond maximumSignalsPerExecution. The three signals
	// above are under max (5), so the 4th and 5th are still accepted; the 6th crosses the limit.
	for i := signalSuggestBoundary; i < signalSuggestMaxSignals; i++ {
		_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        signalSuggestSignalName,
			Input:             payloads.EncodeString("signal"),
			Identity:          identity,
		})
		s.NoError(err)
	}
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        signalSuggestSignalName,
		Input:             payloads.EncodeString("signal"),
		Identity:          identity,
	})
	s.ErrorIs(err, consts.ErrSignalsLimitExceeded, "hard limit must still reject beyond maximumSignalsPerExecution")

	// Complete the workflow so the test can tear down cleanly.
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        "finish",
		Input:             payloads.EncodeString("finish"),
		Identity:          identity,
	})
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)
}

func (s *SignalSuggestCANTestSuite) TestSignalSuggestCAN_DisabledByDefault(opts []testcore.TestOption) {
	// Backward compatibility: with the default threshold (0) the signal reason must never appear,
	// even with a tiny hard limit.
	env := testcore.NewEnv(s.T(), append(opts,
		testcore.WithDynamicConfig(dynamicconfig.MaximumSignalsPerExecution, signalSuggestMaxSignals),
	)...)
	identity := "worker1"
	id := "functional-signal-suggest-can-disabled-" + uuid.NewString()
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id}
	taskQueue := &taskqueuepb.TaskQueue{Name: signalSuggestTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	startResp, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: signalSuggestWorkflow},
		TaskQueue:           taskQueue,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)
	workflowExecution.RunId = startResp.RunId

	signalReasonSeen := false
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		for _, event := range task.History.Events {
			if event.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
				continue
			}
			for _, reason := range event.GetWorkflowTaskStartedEventAttributes().GetSuggestContinueAsNewReasons() {
				if reason == enumspb.SUGGEST_CONTINUE_AS_NEW_REASON_TOO_MANY_SIGNALS {
					signalReasonSeen = true
				}
			}
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}
	//nolint:staticcheck // SA1019 TaskPoller replacement needed
	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send one signal (below the hard limit) and process the task.
	_, err = env.FrontendClient().SignalWorkflowExecution(env.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: workflowExecution,
		SignalName:        signalSuggestSignalName,
		Input:             payloads.EncodeString("signal"),
		Identity:          identity,
	})
	s.NoError(err)
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	s.False(signalReasonSeen, "signal reason must not appear when threshold is 0 (disabled default)")
}
