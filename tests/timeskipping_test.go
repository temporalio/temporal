package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// defaultMaxSkipPerSession mirrors the compiled default of
// dynamicconfig.WorkflowTimeSkippingMaxSkipPerSession. The frontend populates a request's
// unset MaxSkipPerSession with this value, so tests that leave it empty expect it back.
const defaultMaxSkipPerSession = 200

type TimeSkippingTestSuite struct {
	parallelsuite.Suite[*TimeSkippingTestSuite]
}

func TestTimeSkippingTestSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingTestSuite{})
}

// TestTimeSkipping_FeatureDisabled verifies that starting a workflow with time skipping
// returns an error when the feature flag is off for the namespace.
func (s *TimeSkippingTestSuite) TestTimeSkipping_FeatureDisabled() {
	env := testcore.NewEnv(s.T())
	// WorkflowTimeSkippingEnabled defaults to false; no override needed.
	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.ErrorAs(err, new(*serviceerror.Unimplemented))
}

// TestTimeSkipping_StartWorkflow_DCEnabled verifies that StartWorkflowExecution with
// TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_StartWorkflow_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	beforeStart := time.Now()

	// Request leaves MaxSkipPerSession empty; the frontend populates it from dynamic config.
	inputConfig := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour), Id: "ff-id"},
	}

	resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  inputConfig,
	})
	s.NoError(err)
	afterStart := time.Now()

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled())
	inputConfig.MaxSessionSkipCount = defaultMaxSkipPerSession // frontend populated this from dynamic config
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	startTime := ms.State.ExecutionState.GetStartTime().AsTime()
	s.False(startTime.Before(beforeStart), "zero-offset start must not move before the admission window")
	s.False(startTime.After(afterStart), "zero-offset start must not move after the admission window")
	s.Equal(startTime, ms.State.ExecutionInfo.GetStartTime().AsTime())
	s.Equal(startTime, ms.State.ExecutionInfo.GetExecutionTime().AsTime())
}

// TestTimeSkipping_SignalWithStart_DCEnabled verifies that SignalWithStartWorkflowExecution
// with TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_SignalWithStart_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Request leaves MaxSkipPerSession empty; the frontend populates it from dynamic config.
	inputConfig := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour), Id: "ff-id"},
	}

	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(s.Context(), &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		SignalName:          tv.SignalName(),
		TimeSkippingConfig:  inputConfig,
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	inputConfig.MaxSessionSkipCount = defaultMaxSkipPerSession // frontend populated this from dynamic config
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_ExecuteMultiOperation_DCEnabled verifies that a StartWorkflow inside
// ExecuteMultiOperation with TimeSkippingConfig persists the config in mutable state
// when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ExecuteMultiOperation_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	maxElapsedDuration := time.Hour

	// Request leaves MaxSkipPerSession empty; the frontend populates it from dynamic config.
	inputConfig := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(maxElapsedDuration), Id: "ff-id"},
	}

	resp, err := env.FrontendClient().ExecuteMultiOperation(s.Context(), &workflowservice.ExecuteMultiOperationRequest{
		Namespace: env.Namespace().String(),
		Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{
						RequestId:           uuid.NewString(),
						Namespace:           env.Namespace().String(),
						WorkflowId:          tv.WorkflowID(),
						WorkflowType:        tv.WorkflowType(),
						TaskQueue:           tv.TaskQueue(),
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
						TimeSkippingConfig:  inputConfig,
					},
				},
			},
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
						Namespace:         env.Namespace().String(),
						WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
						Request: &updatepb.Request{
							Meta:  &updatepb.Meta{UpdateId: uuid.NewString()},
							Input: &updatepb.Input{Name: "my-update"},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	runID := resp.GetResponses()[0].GetStartWorkflow().GetRunId()
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	inputConfig.MaxSessionSkipCount = defaultMaxSkipPerSession // frontend populated this from dynamic config
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_UpdateWorkflowOptions_DCEnabled exercises the full UpdateWorkflowExecutionOptions
// lifecycle for TimeSkippingConfig:
//  1. Start workflow with no time-skipping — assert mutable state has no config.
//  2. First update: enable with max_elapsed_duration — check MS and event 1 attributes.
//  3. Second update: change the max_elapsed_duration value — check MS and event 2 attributes.
//  4. Third update: disable (Enabled=false) — check MS and event 3 attributes.
//  5. Assert exactly 3 WorkflowExecutionOptionsUpdated events appear in history.
func (s *TimeSkippingTestSuite) TestTimeSkipping_UpdateWorkflowOptions_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Start a workflow without any time-skipping config.
	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId

	// collectOptionsEvents returns all WorkflowExecutionOptionsUpdated events in history order.
	collectOptionsEvents := func() []*historypb.HistoryEvent {
		histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		})
		s.NoError(err)
		var events []*historypb.HistoryEvent
		for _, e := range histResp.History.Events {
			if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
				events = append(events, e)
			}
		}
		return events
	}
	updateOptions := func(cfg *commonpb.TimeSkippingConfig) {
		_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(s.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace:                env.Namespace().String(),
			WorkflowExecution:        &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
			WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfg},
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
		})
		s.NoError(err)
	}

	// No time-skipping config before any update.
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	s.Nil(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig())
	initialStateStart := ms.State.ExecutionState.GetStartTime().AsTime()
	initialInfoStart := ms.State.ExecutionInfo.GetStartTime().AsTime()
	initialExecutionTime := ms.State.ExecutionInfo.GetExecutionTime().AsTime()
	initialRunExpiration := ms.State.ExecutionInfo.GetWorkflowRunExpirationTime().AsTime()
	assertAdmissionTimesUnchanged := func() {
		current := s.getMutableState(env, tv.WorkflowID(), runID)
		s.Equal(initialStateStart, current.State.ExecutionState.GetStartTime().AsTime())
		s.Equal(initialInfoStart, current.State.ExecutionInfo.GetStartTime().AsTime())
		s.Equal(initialExecutionTime, current.State.ExecutionInfo.GetExecutionTime().AsTime())
		s.Equal(initialRunExpiration, current.State.ExecutionInfo.GetWorkflowRunExpirationTime().AsTime())
	}

	// First update: enable with a max_elapsed_duration. MaxSkipPerSession is left empty and
	// populated by the frontend from dynamic config; the persisted config and event carry it.
	config1 := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour), Id: "ff-id"},
	}
	updateOptions(config1)
	config1.MaxSessionSkipCount = defaultMaxSkipPerSession

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config1, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	assertAdmissionTimesUnchanged()
	events := collectOptionsEvents()
	s.Len(events, 1)
	s.True(proto.Equal(config1, events[0].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Second update: change the max_elapsed_duration duration.
	config2 := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(2 * time.Hour), Id: "ff-id"},
	}
	updateOptions(config2)
	config2.MaxSessionSkipCount = defaultMaxSkipPerSession

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config2, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	assertAdmissionTimesUnchanged()
	events = collectOptionsEvents()
	s.Len(events, 2)
	s.True(proto.Equal(config2, events[1].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Third update: disable time-skipping. The frontend's default-populate runs even for a
	// disabled config, so MaxSkipPerSession still comes back set.
	config3 := &commonpb.TimeSkippingConfig{Enabled: false}
	updateOptions(config3)
	config3.MaxSessionSkipCount = defaultMaxSkipPerSession

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config3, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	assertAdmissionTimesUnchanged()
	events = collectOptionsEvents()
	s.Len(events, 3)
	s.True(proto.Equal(config3, events[2].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))
}

// TestTimeSkipping_ResetWithUpdateOptions verifies that resetting a workflow with a
// PostResetOperation that sets TimeSkippingConfig persists the config in the new run's
// mutable state and emits a WorkflowExecutionOptionsUpdated history event whose
// attributes carry the full config.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ResetWithUpdateOptions() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Start a workflow and drain the first workflow task to establish a reset point.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// Find the WorkflowTaskCompleted event ID to use as the reset point.
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	var wftCompletedEventID int64
	for _, e := range histResp.History.Events {
		if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = e.EventId
			break
		}
	}
	s.NotZero(wftCompletedEventID)

	// Reset with PostResetOperations that sets TimeSkippingConfig.
	inputConfig := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(time.Hour), Id: "ff-id"}}
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:                    "test-timeskipping-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
		PostResetOperations: []*workflowpb.PostResetOperation{
			{
				Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
					UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
						WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: inputConfig},
						UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
					},
				},
			},
		},
	})
	s.NoError(err)
	newRunID := resetResp.RunId

	// New run's mutable state must have the config (MaxSkipPerSession populated by the frontend).
	ms := s.getMutableState(env, tv.WorkflowID(), newRunID)
	inputConfig.MaxSessionSkipCount = defaultMaxSkipPerSession
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))

	// New run's history must contain a WorkflowExecutionOptionsUpdated event with the config.
	histResp, err = env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: newRunID},
	})
	s.NoError(err)
	var optionsUpdatedEvent *historypb.HistoryEvent
	for _, e := range histResp.History.Events {
		if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedEvent = e
			break
		}
	}
	s.NotNil(optionsUpdatedEvent, "expected WorkflowExecutionOptionsUpdated event in new run history")
	s.True(proto.Equal(inputConfig, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))
}

func (s *TimeSkippingTestSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		workflowID,
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(s.Context(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}

// startWorkflowWithTimeSkipping starts a workflow with time-skipping enabled
// and a caller-specified run timeout. Used by tests that need the run timeout
// to be long enough to fit a virtual-time skip.
func (s *TimeSkippingTestSuite) startWorkflowWithTimeSkipping(env *testcore.TestEnv, tv *testvars.TestVars, runTimeout time.Duration) string {
	resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(runTimeout),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	return resp.GetRunId()
}

// scheduleActivityCmd returns a ScheduleActivityTask command that uses tv for all names /
// queue / timeout values.
func scheduleActivityCmd(tv *testvars.TestVars) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             tv.ActivityID(),
				ActivityType:           tv.ActivityType(),
				TaskQueue:              tv.TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			},
		},
	}
}

// startTimerCmd returns a StartTimer command with the given duration and timer ID.
func startTimerCmd(timerID string, d time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            timerID,
				StartToFireTimeout: durationpb.New(d),
			},
		},
	}
}

// completeWorkflowCmd returns a CompleteWorkflowExecution command.
func completeWorkflowCmd() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

// hasEventType returns true if any event in the slice has the given type.
func hasEventType(events []*historypb.HistoryEvent, t enumspb.EventType) bool {
	for _, e := range events {
		if e.GetEventType() == t {
			return true
		}
	}
	return false
}

func timeSkippingTransitions(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range events {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			out = append(out, e)
		}
	}
	return out
}

// indexOfEventType returns the index of the first event with the given type, or -1.
func indexOfEventType(events []*historypb.HistoryEvent, t enumspb.EventType) int {
	for i, e := range events {
		if e.GetEventType() == t {
			return i
		}
	}
	return -1
}

// TestTimeSkipping_RetentionClearsSkippedWorkflowImmediately checks the retention deadline
// end-to-end by its observable effect rather than by inspecting a task timestamp.
//
// The namespace has retention 0 and archival disabled, so GenerateWorkflowCloseTasks emits a
// DeleteHistoryEventTask at closeTime + 0. That closeTime is *virtual*: the workflow skips a
// 1-day user timer before completing, so MutableState.AddTasks must subtract the accumulated
// skip for the deadline to land at real close time. If it did not, the deadline would sit a day
// out in wall clock and the execution would still be readable when this test gives up.
func (s *TimeSkippingTestSuite) TestTimeSkipping_RetentionClearsSkippedWorkflowImmediately() {
	const skippedTimer = 24 * time.Hour

	env := testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
		testcore.WithDynamicConfig(dynamicconfig.RetentionTimerJitterDuration, time.Duration(0)),
	)

	// NewEnv's namespace has 1 day retention, which cannot be lowered through the frontend.
	ns := namespace.Name(testcore.RandomizeStr("ts-retention"))
	_, err := env.RegisterNamespace(s.Context(), ns, 0, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)

	tv := testvars.New(s.T())
	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns.String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(48 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: startResp.GetRunId()}

	poller := taskpoller.New(s.T(), env.FrontendClient(), ns.String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("timer-1", skippedTimer)},
		}, nil
	})
	s.NoError(err)

	wallStart := time.Now()
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)
	s.Less(time.Since(wallStart), 5*time.Minute, "the 1 day timer must be skipped, not waited out")

	// Both mutable state and history events go away: DeleteHistoryEventTask drives the full
	// execution deletion, so Describe stops resolving and the history read fails too.
	s.AwaitTruef(func() bool {
		_, describeErr := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns.String(),
			Execution: execution,
		})
		if describeErr == nil {
			return false
		}
		var notFound *serviceerror.NotFound
		return errors.As(describeErr, &notFound)
	}, 60*time.Second, 500*time.Millisecond, "mutable state was not deleted at real close time")

	_, err = env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: ns.String(),
		Execution: execution,
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound, "history events must be deleted along with mutable state")
}

// TestTimeSkipping_TimerAndActivity verifies that when a workflow has both a long user
// timer and a pending activity, time-skipping is blocked until the activity completes.
// Once the activity is done and the workflow task is drained, time-skipping fires and
// moves the timer's visibility timestamp to near-now, so the timer fires quickly.
//
// Sequence:
//
//	WT1 → schedule activity + start 1-hour timer
//	AT1 → complete activity
//	WT2 → drain (return no commands; triggers time-skipping on close)
//	WT3 → complete workflow (timer has fired)
func (s *TimeSkippingTestSuite) TestTimeSkipping_TimerAndActivity() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Run timeout must exceed the 1h timer; otherwise skip shifts the run-timeout
	// task into the past and the workflow times out before WT3 can fire.
	runID := s.startWorkflowWithTimeSkipping(env, tv, 2*time.Hour)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT 1: simultaneously schedule an activity and start a 1-hour timer.
	// Time-skipping cannot fire while both are pending.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				scheduleActivityCmd(tv),
				startTimerCmd("timer-1", time.Hour),
			},
		}, nil
	})
	s.NoError(err)

	// Activity: complete it.  After this, only the 1-hour timer is pending.
	_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT 2: drain (return no commands).  closeTransaction fires time-skipping here because
	// the workflow is now idle with a pending timer → regenerates the timer task at near-now.
	_, err = poller.PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// WT 3: timer has fired (due to time-skipping); complete the workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	// Verify history.
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_TIMER_FIRED), "timer must have fired via time-skipping")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED),
		"time-skipping transitioned event expected")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")
}

// TestTimeSkipping_ActivityRetryBackoff verifies that when the only in-flight work
// is an activity waiting out a retry backoff, time-skipping fires: it advances virtual
// time to the activity's next-attempt time and re-stamps the activity retry timer to
// near-now wall, so the retry is dispatched promptly instead of after the full backoff.
//
// Sequence:
//
//	WT1 → schedule an activity with a 1h retry InitialInterval (MaximumAttempts=2)
//	AT1 → fail attempt 1; the server schedules the retry 1h out (virtual). The workflow
//	      is now idle except for the backoff activity, so the close transaction skips ~1h
//	      and re-stamps the ActivityRetryTimerTask to ~now wall.
//	AT2 → retry is dispatchable promptly (well under 1h wall); complete it.
//	WT2 → activity completed → complete the workflow.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ActivityRetryBackoff() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	wallStart := time.Now()
	// Run timeout must exceed the (virtual) backoff so the skipped-forward run-timeout
	// task doesn't fire the workflow before the activity retries.
	runID := s.startWorkflowWithTimeSkipping(env, tv, 4*time.Hour)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT1: schedule an activity that backs off 1h between attempts. ScheduleToClose
	// must exceed the backoff so the retry isn't cut off by the schedule-to-close deadline.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             tv.ActivityID(),
							ActivityType:           tv.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(3 * time.Hour),
							StartToCloseTimeout:    durationpb.New(30 * time.Second),
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    durationpb.New(time.Hour),
								BackoffCoefficient: 1.0,
								MaximumAttempts:    2,
							},
						},
					},
				},
			},
		}, nil
	})
	s.NoError(err)

	// AT1: fail the activity, triggering a 1h retry backoff and (because the workflow
	// is otherwise idle) a time-skipping transition on the close transaction.
	_, err = poller.PollAndHandleActivityTask(tv, func(_ *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
		return nil, errors.New("fail attempt 1")
	})
	s.NoError(err)

	// AT2: the retry is dispatchable promptly thanks to time-skipping; complete it.
	_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT2: activity completed → complete the workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)
	wallElapsed := time.Since(wallStart)

	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED),
		"activity must complete on its retry attempt")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED),
		"time-skipping transitioned event expected (the activity retry backoff was skipped)")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")

	// Wall elapsed must be well under the 1h backoff — the retry was skipped, not waited out.
	s.Less(wallElapsed, 3*time.Second,
		"test wall elapsed = %v; the activity retry should be dispatched promptly after skipping the 1h backoff",
		wallElapsed)
}

func (s *TimeSkippingTestSuite) TestTimeSkipping_PendingSignalExternalBlocksSkip() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Target workflow B. No worker polls B; the SignalExternal RPC will land a
	// WorkflowExecutionSignaled event in B's history directly. Distinct task
	// queue so B's idle first WT can't interfere with the SDK worker.
	tvB := tv.WithWorkflowIDNumber(2).WithTaskQueueNumber(2)
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tvB.WorkflowID(),
		WorkflowType:        tvB.WorkflowType(),
		TaskQueue:           tvB.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(2 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)

	// CoordinatorWorkflow: emits the 1h timer and the SignalExternal command in
	// the same WFT response, then waits for whichever future resolves first.
	coordinatorWorkflow := func(wfCtx workflow.Context, targetWorkflowID string) error {
		timerFuture := workflow.NewTimer(wfCtx, time.Hour)
		signalFuture := workflow.SignalExternalWorkflow(
			wfCtx, targetWorkflowID, "", "test-pending-signal", nil)

		workflow.NewSelector(wfCtx).
			AddFuture(timerFuture, func(_ workflow.Future) {}).
			AddFuture(signalFuture, func(_ workflow.Future) {}).
			Select(wfCtx)
		return nil
	}
	const coordinatorTypeName = "CoordinatorWorkflow"
	env.SdkWorker().RegisterWorkflowWithOptions(coordinatorWorkflow, workflow.RegisterOptions{
		Name: coordinatorTypeName,
	})

	// SDK's StartWorkflowOptions doesn't expose TimeSkippingConfig (as of SDK
	// v1.41), so start workflow A directly through the frontend. Use the SDK
	// worker's task queue so the registered coordinator picks up the WT.
	input, err := converter.GetDefaultDataConverter().ToPayloads(tvB.WorkflowID())
	s.NoError(err)

	tvA := tv.WithWorkflowIDNumber(1)
	wallStart := time.Now()
	aResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tvA.WorkflowID(),
		WorkflowType:        &commonpb.WorkflowType{Name: coordinatorTypeName},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               input,
		WorkflowRunTimeout:  durationpb.New(2 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)

	// Wait for A to finish through the SDK.
	err = env.SdkClient().GetWorkflow(ctx, tvA.WorkflowID(), aResp.RunId).Get(ctx, nil)
	s.NoError(err)
	wallElapsed := time.Since(wallStart)

	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tvA.WorkflowID(),
		RunId:      aResp.RunId,
	})

	// The signal future resolved (its completion event landed in A's history).
	s.True(hasEventType(history, enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED),
		"ExternalWorkflowExecutionSignaled event must appear in A's history")

	// The timer never fired — the signal won the Selector. If the new branch in
	// hasInflightWorkToPreventTimeSkipping were missing, skip could fire at WT1
	// close, shift the timer to near-now, and race the signal — making this
	// assertion flaky.
	s.False(hasEventType(history, enumspb.EVENT_TYPE_TIMER_FIRED),
		"TimerFired must NOT appear — the signal future must resolve before the 1h timer fires")

	// Workflow A closed via the signal branch.
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED),
		"workflow A must complete")

	// Wall elapsed must be well under the 1h timer — the workflow should
	// finish as soon as the signal completes (sub-second on a healthy cluster).
	s.Less(wallElapsed, 5*time.Minute,
		"test wall elapsed = %v; the workflow should complete promptly after the signal succeeds, well before the 1h timer would fire",
		wallElapsed)
}

// TestTimeSkipping_PendingNexusOperationBlocksSkip verifies that a pending nexus operation —
// including one parked in retry backoff — blocks time skipping. This is what makes the
// StateMachineTimerTask (nexus HSM) exclusion in RegenerateTimerTasksForTimeSkipping safe: a skip
// can never happen while such a timer is live, so a nexus HSM timer can never be left stale by one.
//
// Sequence:
//
//	WT1 → schedule nexus operation + start a 1h timer. Close tx: nexus op pending → no skip.
//	Nexus attempt 1 → retryable handler error → operation parks in BACKING_OFF, still pending.
//	Nexus attempt 2 → sync success → NexusOperationCompleted → WT2.
//	WT2 → drain. Close tx: no pending nexus op, only the 1h timer → skip fires.
//	WT3 → timer fired via skip → complete workflow.
//
// The load-bearing assertion is ordering: TIME_SKIPPING_TRANSITIONED must appear *after*
// NEXUS_OPERATION_COMPLETED. That is timing-independent — were the nexus gate missing, the skip
// would fire at WT1 close, well ahead of the operation completing.
func (s *TimeSkippingTestSuite) TestTimeSkipping_PendingNexusOperationBlocksSkip() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
	)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Attempt 1 fails retryably so the operation parks in BACKING_OFF with a live nexus HSM
	// timer; attempt 2 succeeds so the workflow can make progress and the test can observe the
	// skip that follows.
	var attempts atomic.Int32
	handler := nexustest.Handler{
		OnStartOperation: func(
			_ context.Context, service, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			if service != "service" {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, `expected service to equal "service"`)
			}
			if attempts.Add(1) == 1 {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional retryable error")
			}
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		},
	}
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), handler)

	// Run timeout must exceed the 1h timer, or the skip shifts the run-timeout task into the
	// past and the workflow times out before WT3.
	wallStart := time.Now()
	runID := s.startWorkflowWithTimeSkipping(env.TestEnv, tv, 4*time.Hour)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT1: nexus operation + 1h timer. The nexus operation is pending at close, so no skip.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
						ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
							Endpoint:  endpointName,
							Service:   "service",
							Operation: "operation",
							Input:     testcore.MustToPayload(s.T(), "input"),
						},
					},
				},
				startTimerCmd("timer-1", time.Hour),
			},
		}, nil
	})
	s.NoError(err)

	// WT2 arrives once the nexus operation completes (after the retry). Draining it is the
	// first close transaction where the workflow is idle, so the skip fires here.
	_, err = poller.PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// WT3: the 1h timer fired because the skip re-stamped it to near-now wall.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)
	wallElapsed := time.Since(wallStart)

	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      runID,
	})

	// The operation really did fail once and retry, so a nexus HSM timer was live while the
	// workflow was otherwise idle.
	s.GreaterOrEqual(int(attempts.Load()), 2, "nexus operation must have been retried at least once")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED), "nexus operation must complete")

	nexusCompletedIdx := indexOfEventType(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED)
	transitionIdx := indexOfEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED)
	s.NotEqual(-1, transitionIdx, "skip must fire once the nexus operation is no longer pending")
	s.Greater(transitionIdx, nexusCompletedIdx,
		"skip must not happen while the nexus operation is pending: TIME_SKIPPING_TRANSITIONED at index %d must follow NEXUS_OPERATION_COMPLETED at index %d",
		transitionIdx, nexusCompletedIdx)

	s.True(hasEventType(history, enumspb.EVENT_TYPE_TIMER_FIRED), "timer must fire via the post-nexus skip")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")
	s.Less(wallElapsed, 5*time.Minute,
		"wall elapsed = %v; the 1h timer must be skipped once the nexus operation completes", wallElapsed)
}

// TestTimeSkipping_StartWithDelay_NoBound verifies that time-skipping with no
// bound shifts a WorkflowStartDelay backoff into the near-now wall-clock window:
// the first WT becomes available immediately instead of waiting wallStart + 1h.
//
// Sequence:
//
//	Start workflow with WorkflowStartDelay = 1h, TimeSkippingConfig{Enabled: true},
//	no bound. On the close transaction of WorkflowExecutionStarted,
//	calculateTimeSkippingTransition picks the backoff (only candidate;
//	!HadOrHasWorkflowTask && ExecutionTime > StartTime), skips by 1h,
//	accumulated = 1h. RegenerateTimerTasksForTimeSkipping step (4) re-emits the
//	WorkflowBackoffTimerTask with VisibilityTimestamp ≈ wallStart
//	(= virtual_executionTime − accumulated = (wallStart + 1h) − 1h).
//	WT1 → complete workflow.
//
// Assertions:
//
//  1. WT1 polled in < 5min wall (would block 1h without skip, exceeding long-poll).
//  2. At least two WorkflowBackoffTimerTask writes (initial + regenerated).
//  3. All backoff tasks are typed WORKFLOW_BACKOFF_TYPE_DELAY_START
//     (no cron, attempt == 1).
//  4. The latest task's VisibilityTimestamp is ≈ wallStart, not wallStart + 1h.
func (s *TimeSkippingTestSuite) TestTimeSkipping_StartWithDelay() {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithHistoryTaskRecorder(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
	)
	tv := testvars.New(s.T())

	const (
		startDelay = time.Hour
		shiftTol   = 5 * time.Second
	)
	wallStart := time.Now()

	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &commonpb.TimeSkippingConfig{Enabled: true},
		WorkflowStartDelay:  durationpb.New(startDelay),
	})
	s.NoError(err)
	runID := startResp.RunId

	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	elapsed := time.Since(wallStart)
	s.Less(elapsed, shiftTol, "skip should have shifted the 1h start delay into near-now wall-clock; took %v", elapsed)

	recorder := env.GetTestCluster().GetHistoryTaskRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       runID,
	})
	var backoffTasks []*historytasks.WorkflowBackoffTimerTask
	for _, rec := range recorded {
		if t, ok := rec.Task.(*historytasks.WorkflowBackoffTimerTask); ok {
			backoffTasks = append(backoffTasks, t)
		}
	}
	s.GreaterOrEqual(len(backoffTasks), 2, "expected initial + regenerated WorkflowBackoffTimerTask (two writes)")
	for _, t := range backoffTasks {
		s.Equal(enumsspb.WORKFLOW_BACKOFF_TYPE_DELAY_START, t.WorkflowBackoffType,
			"all backoff tasks for start-with-delay must have type DELAY_START")
	}
	if len(backoffTasks) >= 1 {
		latest := backoffTasks[len(backoffTasks)-1]
		s.Less(latest.VisibilityTimestamp.Sub(wallStart), shiftTol,
			"regenerated backoff task VisibilityTime must be ≈ wallStart (= virtual exec − accum), got %v vs wallStart %v",
			latest.VisibilityTimestamp, wallStart)
	}
}

// TestTimeSkipping_CanceledTimerNotUsedAsSkipTarget confirms that a timer
// canceled via command is excluded from the skip-target calculation.
// ApplyTimerCanceledEvent deletes the timer from pendingTimerInfoIDs, so it is
// invisible to calculateTimeSkippingTransition.
//
// If a canceled timer were mistakenly used, the second transition would target
// timer-B instead of timer-C.
//
// Sequence:
//
//	WT1 → start timer-A (1h) + timer-B (5h)
//	Skip to timer-A (nearest), accumulated = 1h, timer-A fires → WT2
//	WT2 → cancel timer-B + start timer-C (2h)
//	Skip to timer-C (2h), accumulated = 1h + 2h = 3h, timer-C fires → WT3
//	WT3 → complete workflow
//	Verify: transition targets are exactly timer-A and timer-C, never timer-B
func (s *TimeSkippingTestSuite) TestTimeSkipping_CanceledTimerNotUsedAsSkipTarget() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	const (
		timerADuration = time.Hour
		timerBDuration = 5 * time.Hour
		timerCDuration = 2 * time.Hour
		runTimeout     = 10 * time.Hour
	)

	runID := s.startWorkflowWithTimeSkipping(env, tv, runTimeout)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT1: start timer-A (1h) + timer-B (5h). No activity → time skipping fires
	// on close-tx and targets timer-A (nearest candidate).
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				startTimerCmd("timer-A", timerADuration),
				startTimerCmd("timer-B", timerBDuration),
			},
		}, nil
	})
	s.NoError(err)

	// WT2: timer-A has fired. Cancel timer-B and start timer-C (2h). After this
	// WFT, only timer-C is a candidate — timer-B is gone from pendingTimerInfoIDs.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_CANCEL_TIMER,
					Attributes: &commandpb.Command_CancelTimerCommandAttributes{
						CancelTimerCommandAttributes: &commandpb.CancelTimerCommandAttributes{
							TimerId: "timer-B",
						},
					},
				},
				startTimerCmd("timer-C", timerCDuration),
			},
		}, nil
	})
	s.NoError(err)

	// WT3: timer-C fired (skip targeted it at 2h, not the 4h remaining on
	// canceled timer-B). Complete workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(), RunId: runID,
	})
	timerExpiry := make(map[string]time.Time)
	var skipTargets []time.Time
	for _, event := range history {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_TIMER_STARTED:
			attrs := event.GetTimerStartedEventAttributes()
			timerExpiry[attrs.GetTimerId()] = event.GetEventTime().AsTime().Add(attrs.GetStartToFireTimeout().AsDuration())
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED:
			skipTargets = append(skipTargets,
				event.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetTargetTime().AsTime())
		default:
			continue
		}
	}
	s.Len(timerExpiry, 3)
	s.Len(skipTargets, 2)
	s.Equal(timerExpiry["timer-A"], skipTargets[0])
	s.Equal(timerExpiry["timer-C"], skipTargets[1])
	s.NotEqual(timerExpiry["timer-B"], skipTargets[1])
	s.True(hasEventType(history, enumspb.EVENT_TYPE_TIMER_CANCELED), "timer-B must be canceled")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
}

// TestWorkflowLifecycle_VirtualTimeContract is an end-to-end regression test
// that exercises the virtual-time contract across a full workflow lifecycle:
// activity execution before and after skip, skip transition, workflow close,
// close-time semantics, run-timeout task regeneration, activity-timeout task
// shifting, and retention task scheduling.
//
// Scenario:
//
//	Start workflow with runTimeout = 4h, time-skipping enabled, namespace
//	retention = 1 day.
//	WT1  → schedule activity-1 + 1h user timer (activity blocks skip).
//	AT1  → complete activity-1 (virtual time barely advances — activity is fast).
//	WT2  → drain → closeTransaction fires skip transition (accumulated ≈ 1h).
//	WT3  → timer-1 has fired; schedule activity-2 with 5min ScheduleToClose
//	        (this is when ActivityTimeoutTask for activity-2 is written, with
//	        virtual ScheduledTime ≈ wallStart + 1h).
//	AT2  → complete activity-2.
//	WT4  → complete workflow.
//
// Assertions:
//
//  1. Activity-1 events: ActivityTaskCompleted for activity-1 appears in history.
//  2. Skip: history has WorkflowExecutionTimeSkippingTransitioned event.
//  3. Workflow close: history has WorkflowExecutionCompleted.
//  4. Describe semantics:
//     4a. StartTime ≈ wallBeforeStart (wall frame, admission anchor).
//     4b. ExecutionTime == StartTime (no backoff configured).
//     4c. CloseTime − StartTime ≈ skip (virtual frame).
//     4d. CloseTime − ExecutionTime ≈ skip (reported duration).
//  5. Run-timeout task regenerated: two WorkflowRunTimeoutTask writes; the
//     second has VisibilityTimestamp ≈ wallStart + runTimeout − skip (earlier
//     by ≈1h than the first).
//  6. Retention task: DeleteHistoryEventTask has VisibilityTimestamp ≈
//     wallClose + retention. The retention fires at real wall time
//     "retention after close," not virtual time "retention after virtual close."
//     Concretely: virtual deleteTime = virtualCloseTime + retention; after
//     toRealTime this is (wallClose + skip) + retention − skip =
//     wallClose + retention.
//  7. Activity-2 events: ActivityTaskScheduled for activity-2 has EventTime in
//     virtual frame (EventTime − workflow StartTime ≥ ~skip). Completed event
//     for activity-2 also appears.
//  8. Activity-2 timeout task: the ActivityTimeoutTask written when activity-2
//     is scheduled has VisibilityTimestamp ≈ wallAtActivity2Schedule +
//     scheduleToClose. Principle 2: even though virtual time is ~1h ahead when
//     the activity is scheduled, the wall-clock VisibilityTimestamp must be
//     anchored to real wall time (test allows ±3min tolerance).
//
// This is the single most comprehensive e2e test for the virtual-time system.
// If any of the four principles regresses, at least one of assertions 4–8 will
// fail.
func (s *TimeSkippingTestSuite) TestWorkflowLifecycle_VirtualTimeContract() {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithHistoryTaskRecorder(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true),
	)
	tv := testvars.New(s.T())

	const (
		runTimeout    = 4 * time.Hour
		timerDuration = 1 * time.Hour // determines the amount of skip
		// Namespace retention is 1 day per testcore.NewEnv (see test_env.go:165).
		// Any future change to that default will require updating this constant.
		namespaceRetention = 24 * time.Hour
		// Margin for timing-related assertions: scheduling jitter between our
		// wall-time samples and the server's, plus any clock drift. Must be
		// less than timerDuration so the shift assertion differentiates the
		// virtual-frame and wall-frame cases.
		assertionMargin = 5 * time.Minute
		// Second activity (scheduled AFTER skip) uses a 5-minute
		// ScheduleToClose timeout. With accumulated skip = timerDuration (~1h),
		// virtualToRealTime at the task-generator boundary should produce an
		// ActivityTimeoutTask VisibilityTimestamp ≈ wallAtActivity2Schedule +
		// activity2ScheduleToClose. Delta within activity2TimerMargin is
		// allowed.
		activity2ScheduleToClose = 5 * time.Minute
		activity2TimerMargin     = 3 * time.Minute
	)

	wallBeforeStart := time.Now()
	runID := s.startWorkflowWithTimeSkipping(env, tv, runTimeout)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT1: schedule activity-1 + 1h user timer. Activity blocks skip.
	tvActivity1 := tv.WithActivityIDNumber(1)
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				scheduleActivityCmd(tvActivity1),
				startTimerCmd("lifecycle-timer", timerDuration),
			},
		}, nil
	})
	s.NoError(err)

	// AT1: complete activity-1. After this the workflow has only the pending
	// user timer → skip becomes eligible.
	_, err = poller.PollAndHandleActivityTask(tvActivity1, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT2: drain → closeTransaction fires the skip transition.
	_, err = poller.PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// WT3: timer-1 has fired via skip. Schedule activity-2 with a 5-minute
	// ScheduleToClose timeout. Record the wall time so we can verify that the
	// ActivityTimeoutTask's VisibilityTimestamp is anchored to wall clock
	// (principle 2) and NOT to virtual time (which would put it ~1h+5min out).
	wallAtActivity2Schedule := time.Now()
	tvActivity2 := tv.WithActivityIDNumber(2)
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             tvActivity2.ActivityID(),
							ActivityType:           tvActivity2.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(activity2ScheduleToClose),
						},
					},
				},
			},
		}, nil
	})
	s.NoError(err)

	// AT2: complete activity-2.
	_, err = poller.PollAndHandleActivityTask(tvActivity2, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT4: complete workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)
	wallAfterClose := time.Now()

	// ── Assertion 1/2/3: the three history events are present. ────────────────
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED), "activity must have completed")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED), "skip transition must have happened")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must have completed")

	// ── Assertion 4: StartTime, ExecutionTime, CloseTime have the right frames. ─
	//
	//   StartTime     = caller-provided admission time → WALL clock,
	//                   ≈ wallBeforeStart (± scheduling jitter).
	//   ExecutionTime = StartTime + FirstWorkflowTaskBackoff.
	//                   With no cron / WorkflowStartDelay / ContinueAsNew-backoff,
	//                   FirstWorkflowTaskBackoff = 0 → ExecutionTime == StartTime.
	//   CloseTime     = VIRTUAL time at WorkflowExecutionCompleted event.
	//                   = wallClose + accumulatedSkip ≈ wallStart + skip.
	//
	// Consequences:
	//   - CloseTime − StartTime ≈ skip (≈ timerDuration).  (this is virtualDuration)
	//   - CloseTime − ExecutionTime ≈ skip.  (public reported duration)
	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	execInfo := desc.GetWorkflowExecutionInfo()
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, execInfo.GetStatus())
	s.NotNil(execInfo.GetStartTime(), "StartTime must be set")
	s.NotNil(execInfo.GetExecutionTime(), "ExecutionTime must be set")
	s.NotNil(execInfo.GetCloseTime(), "CloseTime must be set")

	// 4a. StartTime ≈ wallBeforeStart (wall frame, admission anchor).
	startTime := execInfo.GetStartTime().AsTime()
	s.GreaterOrEqual(startTime, wallBeforeStart.Add(-assertionMargin),
		"StartTime %v is earlier than wallBeforeStart %v − margin; StartTime should be wall admission time",
		startTime, wallBeforeStart)
	s.LessOrEqual(startTime, wallBeforeStart.Add(assertionMargin),
		"StartTime %v is later than wallBeforeStart %v + margin; StartTime should be ≈ wall admission moment, NOT virtual-shifted",
		startTime, wallBeforeStart)

	// 4b. ExecutionTime == StartTime (no backoff configured).
	executionTime := execInfo.GetExecutionTime().AsTime()
	s.Equal(startTime, executionTime,
		"ExecutionTime %v should equal StartTime %v because FirstWorkflowTaskBackoff = 0",
		executionTime, startTime)

	// 4c. CloseTime − StartTime ≈ skip (virtual frame).
	virtualDuration := execInfo.GetCloseTime().AsTime().Sub(startTime)
	s.GreaterOrEqual(
		virtualDuration, timerDuration-assertionMargin,
		"Describe.CloseTime − Describe.StartTime = %v is shorter than the accumulated skip (~%v); CloseTime may not be written in virtual frame",
		virtualDuration, timerDuration,
	)
	// Upper bound: skip + actual wall time spent (very small in this test).
	// assertionMargin (5min) is comfortably above any realistic wall elapsed.
	s.LessOrEqual(
		virtualDuration, timerDuration+assertionMargin,
		"Describe.CloseTime − Describe.StartTime = %v exceeds accumulated skip + test wall time; something is wrong with the close-time computation",
		virtualDuration,
	)

	// 4d. CloseTime − ExecutionTime ≈ skip (the "reported duration").
	reportedDuration := execInfo.GetCloseTime().AsTime().Sub(executionTime)
	s.GreaterOrEqual(
		reportedDuration, timerDuration-assertionMargin,
		"Reported duration (CloseTime − ExecutionTime) = %v is shorter than accumulated skip (~%v)",
		reportedDuration, timerDuration,
	)
	s.LessOrEqual(
		reportedDuration, timerDuration+assertionMargin,
		"Reported duration (CloseTime − ExecutionTime) = %v exceeds accumulated skip + test wall time",
		reportedDuration,
	)

	// ── Assertion 5: WorkflowRunTimeoutTask regenerated with shifted timestamp. ─
	recorder := env.GetTestCluster().GetHistoryTaskRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       runID,
	})

	var runTimeoutTasks []*historytasks.WorkflowRunTimeoutTask
	var deleteHistoryTasks []*historytasks.DeleteHistoryEventTask
	var activityTimeoutTasks []*historytasks.ActivityTimeoutTask
	for _, rec := range recorded {
		switch t := rec.Task.(type) {
		case *historytasks.WorkflowRunTimeoutTask:
			runTimeoutTasks = append(runTimeoutTasks, t)
		case *historytasks.DeleteHistoryEventTask:
			deleteHistoryTasks = append(deleteHistoryTasks, t)
		case *historytasks.ActivityTimeoutTask:
			activityTimeoutTasks = append(activityTimeoutTasks, t)
		default:
			// other task types are not relevant to this assertion
		}
	}

	s.GreaterOrEqual(
		len(runTimeoutTasks), 2,
		"expected initial + regenerated WorkflowRunTimeoutTask (two writes)",
	)

	// Initial task at workflow start: VisibilityTimestamp ≈ wallStart + runTimeout.
	firstRTT := runTimeoutTasks[0]
	initialExpected := wallBeforeStart.Add(runTimeout)
	s.GreaterOrEqual(firstRTT.VisibilityTimestamp, initialExpected.Add(-assertionMargin),
		"initial WorkflowRunTimeoutTask %v is earlier than expected %v",
		firstRTT.VisibilityTimestamp, initialExpected)
	s.LessOrEqual(firstRTT.VisibilityTimestamp, initialExpected.Add(assertionMargin),
		"initial WorkflowRunTimeoutTask %v is later than expected %v",
		firstRTT.VisibilityTimestamp, initialExpected)

	// Regenerated task after skip: VisibilityTimestamp ≈ wallStart + runTimeout − skip.
	latestRTT := runTimeoutTasks[len(runTimeoutTasks)-1]
	shiftedExpected := wallBeforeStart.Add(runTimeout).Add(-timerDuration)
	s.GreaterOrEqual(latestRTT.VisibilityTimestamp, shiftedExpected.Add(-assertionMargin),
		"regenerated WorkflowRunTimeoutTask %v is earlier than expected %v",
		latestRTT.VisibilityTimestamp, shiftedExpected)
	s.LessOrEqual(latestRTT.VisibilityTimestamp, shiftedExpected.Add(assertionMargin),
		"regenerated WorkflowRunTimeoutTask %v is later than expected %v (principle 3: skip must shift outstanding tasks earlier by accumulated duration)",
		latestRTT.VisibilityTimestamp, shiftedExpected)

	// ── Assertion 6: DeleteHistoryEventTask fires at wallClose + retention. ───
	//
	// virtual deleteTime = virtualCloseTime + retention + tiny-jitter
	//                    = (wallClose + skip) + retention + tiny-jitter
	// VisibilityTimestamp = toRealTime(deleteTime)
	//                     = deleteTime − skip
	//                     = wallClose + retention + tiny-jitter
	//
	// RetentionTimerJitterDuration is overridden to 1s in functional tests
	// (testcore/dynamic_config_overrides.go:54), so the jitter is negligible.
	s.GreaterOrEqual(len(deleteHistoryTasks), 1, "expected a DeleteHistoryEventTask for retention")
	deleteTask := deleteHistoryTasks[len(deleteHistoryTasks)-1]
	retentionExpected := wallAfterClose.Add(namespaceRetention)
	s.GreaterOrEqual(deleteTask.VisibilityTimestamp, retentionExpected.Add(-assertionMargin),
		"DeleteHistoryEventTask VisibilityTimestamp %v is earlier than expected ~wallClose+retention %v (principle 2: retention should anchor on wall close, not virtual close)",
		deleteTask.VisibilityTimestamp, retentionExpected)
	s.LessOrEqual(deleteTask.VisibilityTimestamp, retentionExpected.Add(assertionMargin),
		"DeleteHistoryEventTask VisibilityTimestamp %v is later than expected ~wallClose+retention %v",
		deleteTask.VisibilityTimestamp, retentionExpected)

	// ── Assertion 7: activity-2 events are stamped with virtual time. ─────────
	//
	// activity-2 was scheduled AFTER the skip fired, so by the time WT3's
	// closeTransaction runs, ms.timeSource is ~1h ahead of wall clock. The
	// ActivityTaskScheduled event's EventTime is stamped from hBuilder which
	// uses ms.timeSource, so it must be in virtual frame — specifically at
	// least (workflow StartTime + timerDuration − margin) into the "virtual
	// future." A wall-frame write would produce EventTime ≈ wallStart +
	// small_delta, which would be ~1h earlier than the expected virtual
	// timestamp.
	var activity2ScheduledEvent, activity2CompletedEvent *historypb.HistoryEvent
	for _, e := range history {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityId() == tvActivity2.ActivityID() {
				activity2ScheduledEvent = e
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			// Completion event doesn't carry ActivityId directly; match by
			// ScheduledEventId pointing at activity-2's Scheduled event.
			if activity2ScheduledEvent != nil &&
				e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == activity2ScheduledEvent.GetEventId() {
				activity2CompletedEvent = e
			}
		default:
			// other event types are not relevant to this assertion
		}
	}
	s.NotNil(activity2ScheduledEvent, "ActivityTaskScheduled event for activity-2 must exist")
	s.NotNil(activity2CompletedEvent, "ActivityTaskCompleted event for activity-2 must exist")

	virtualScheduledDelta := activity2ScheduledEvent.GetEventTime().AsTime().Sub(execInfo.GetStartTime().AsTime())
	s.GreaterOrEqual(
		virtualScheduledDelta, timerDuration-assertionMargin,
		"activity-2 ActivityTaskScheduled EventTime − StartTime = %v is shorter than accumulated skip (~%v); event may not be stamped with virtual time",
		virtualScheduledDelta, timerDuration,
	)

	// ── Assertion 8: activity-2 timeout task is anchored to wall clock. ──────
	//
	// When activity-2 is scheduled (WT3 close), timer_sequence.CreateNextActivityTimer
	// writes an ActivityTimeoutTask with:
	//   VisibilityTimestamp = toRealTime(virtualScheduledTime + activity2ScheduleToClose)
	//                       = (wallAtActivity2Schedule + skip + timeout) − skip
	//                       = wallAtActivity2Schedule + activity2ScheduleToClose
	//
	// Principle 2: regardless of virtual time's offset, the wall-clock
	// VisibilityTimestamp must be ≈ wallAtActivity2Schedule + 5min. The test
	// tolerates ±3min drift (covers scheduling jitter between our wall-time
	// sample and the server's write, plus any internal processing delay).
	s.GreaterOrEqual(len(activityTimeoutTasks), 1, "expected at least one ActivityTimeoutTask written for activity-2")
	// Scan all recorded ActivityTimeoutTasks; find the one referencing activity-2's
	// ScheduledEventId. Each activity may generate multiple timeout tasks
	// (ScheduleToStart, ScheduleToClose, etc.), but in this test only
	// ScheduleToClose was configured.
	var activity2TimeoutTask *historytasks.ActivityTimeoutTask
	for _, t := range activityTimeoutTasks {
		if t.EventID == activity2ScheduledEvent.GetEventId() {
			activity2TimeoutTask = t
			break
		}
	}
	s.NotNil(activity2TimeoutTask, "expected an ActivityTimeoutTask for activity-2 (EventID=%d)", activity2ScheduledEvent.GetEventId())

	activity2TimerExpected := wallAtActivity2Schedule.Add(activity2ScheduleToClose)
	s.False(
		activity2TimeoutTask.VisibilityTimestamp.Before(activity2TimerExpected.Add(-activity2TimerMargin)),
		"activity-2 ActivityTimeoutTask VisibilityTimestamp %v is earlier than expected ~wallAtSchedule+5min %v (principle 2: task must anchor to wall clock, not virtual)",
		activity2TimeoutTask.VisibilityTimestamp, activity2TimerExpected,
	)
	s.False(
		activity2TimeoutTask.VisibilityTimestamp.After(activity2TimerExpected.Add(activity2TimerMargin)),
		"activity-2 ActivityTimeoutTask VisibilityTimestamp %v is later than expected ~wallAtSchedule+5min %v; this would happen if the virtual ScheduledTime leaked into VisibilityTimestamp (would show ≈ wallAtSchedule + 1h + 5min)",
		activity2TimeoutTask.VisibilityTimestamp, activity2TimerExpected,
	)
}

func (s *TimeSkippingFastForwardFunctionalSuite) TestTimeSkipping_ExecutionTimeoutTimesOutIdleWorkflow() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	ctx := s.Context()

	const executionTimeout = 5 * time.Minute

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Await(ctx, func() bool { return false })
	}, workflow.RegisterOptions{Name: "blockingConditionWorkflow"})

	startWall := time.Now()
	workflowID := uuid.NewString()
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: "blockingConditionWorkflow"},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue()},
		WorkflowExecutionTimeout: durationpb.New(executionTimeout),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
		TimeSkippingConfig:       &commonpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	runID := startResp.RunId

	// The workflow is idle (blocked on Await) with the execution timeout as its only skip
	// target, so it skips straight to the timeout and times out — without waiting out 5 min
	// of real time.
	run := env.SdkClient().GetWorkflow(ctx, workflowID, runID)
	err = run.Get(ctx, nil)
	var timeoutErr *sdktemporal.TimeoutError
	s.ErrorAs(err, &timeoutErr, "expected TimeoutError, got: %v", err)

	s.Less(time.Since(startWall), executionTimeout,
		"time skipping must reach the execution timeout in far less than the 5 min real timeout")

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 1, "exactly one transition: skip to the execution timeout")
	s.False(transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward(),
		"timing out at the execution timeout is not a fast-forward disable")
	s.True(hasEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT))
}

func (s *TimeSkippingTestSuite) TestTimeSkippingTransitionEventOrdersAfterOptionsUpdated() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Start WITHOUT time skipping.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId

	// WT1: schedule a user timer. Time skipping is off, so the workflow just goes idle with a
	// pending timer (no skip yet).
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", time.Hour)},
		}, nil
	})
	s.NoError(err)

	// Enable time skipping. The workflow is idle with a pending timer, so this update's close-tx
	// emits a transition skipping to the timer — in the same transaction as OPTIONS_UPDATED.
	_, err = env.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			TimeSkippingConfig: &commonpb.TimeSkippingConfig{Enabled: true},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
	})
	s.NoError(err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})

	// The transition and the OPTIONS_UPDATED that triggered it must both be present, and the
	// OPTIONS_UPDATED must come first (the transition is the last event of the transaction).
	optionsUpdatedIdx, transitionIdx := -1, -1
	for i, e := range hist {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
			optionsUpdatedIdx = i
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED:
			transitionIdx = i
		default:
			// other event types are irrelevant to this ordering check
		}
	}
	s.NotEqual(-1, optionsUpdatedIdx, "expected an OPTIONS_UPDATED event")
	s.NotEqual(-1, transitionIdx, "expected a TIME_SKIPPING_TRANSITIONED event")
	s.Less(optionsUpdatedIdx, transitionIdx,
		"OPTIONS_UPDATED (event %d) must precede the transition it triggered (event %d)",
		hist[optionsUpdatedIdx].GetEventId(), hist[transitionIdx].GetEventId())
	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

// TestTimeSkipping_Paused verifies time-skipping behavior related to pause and unpause:
//  1. we allow setting and storing the time skipping config when paused;
//  2. time doesn't skip when paused, and skipping resumes when unpaused;
//  3. if a user sets a time point to disable time skipping in the options (the fast-forward)
//     and also pauses the execution, that timer can still fire and turn off time skipping.
//
// Sequence:
//
//	WT1       → start timer t1 (5min). Time skipping is off, so nothing skips.
//	Pause     → status PAUSED.
//	Update #1 → enable skipping with a 2s fast-forward. The close-tx cannot skip, but the
//	            fast-forward timer task is scheduled at wall now+2s.
//	(wait)    → once real time passes 2s the task fires through pause: HasReached=true,
//	            Config.Enabled=false, and no time was skipped (t1 is untouched).
//	          → the workflow is still paused; turning time skipping off does not resume it.
//	Update #2 → enable skipping again with no fast-forward config (FastForwardInfo cleared,
//	            session skip count reset). Still paused → still no skip 2s later.
//	Unpause   → the unpause transaction schedules a WFT, which blocks skipping; draining it
//	            leaves the workflow idle on t1, so the close-tx skips ~5min and t1 fires.
//	WT        → complete the workflow.
//
// Final history has exactly two transitions: the fast-forward disable (while paused) and
// the skip to t1 (after unpause).
func (s *TimeSkippingTestSuite) TestTimeSkipping_Paused() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	env.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const (
		timerDuration = 5 * time.Minute
		fastForward   = 2 * time.Second
		// The fast-forward target is only 2s out, so its timer task must fire almost
		// immediately; anything beyond this means it did not fire through pause at all.
		fastForwardWait = 3 * time.Second
		// Real time to let a skip happen, if one were going to, before asserting it did not.
		settleWindow = 2 * time.Second
		// Whole-test budget. fastForward + settleWindow are spent waiting on real time; the
		// rest is RPCs. The 5min timer is skipped, never waited out.
		maxTestDuration = 10 * time.Second
	)

	testStart := time.Now()

	// Start without a time-skipping config: skipping must stay off until the first update,
	// otherwise WT1's close transaction would skip t1 before we can pause.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId
	execution := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID}

	timeSkippingInfo := func() *persistencespb.TimeSkippingInfo {
		return s.getMutableState(env, tv.WorkflowID(), runID).State.ExecutionInfo.GetTimeSkippingInfo()
	}
	workflowStatus := func() enumspb.WorkflowExecutionStatus {
		desc, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: execution,
		})
		s.NoError(descErr)
		return desc.GetWorkflowExecutionInfo().GetStatus()
	}
	updateTimeSkipping := func(cfg *commonpb.TimeSkippingConfig) {
		_, updateErr := env.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace:                env.Namespace().String(),
			WorkflowExecution:        execution,
			WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfg},
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
		})
		s.NoError(updateErr)
	}
	// (1) WT1: start the 5-minute timer and go idle.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", timerDuration)},
		}, nil
	})
	s.NoError(err)
	s.Nil(timeSkippingInfo(), "no time-skipping info before the workflow opts in")

	// (2) Pause.
	_, err = env.FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: tv.WorkflowID(),
		RunId:      runID,
		Identity:   "test",
		Reason:     "paused workflows must not skip time",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, workflowStatus())

	// (3) Enable time skipping with a 2s fast-forward while paused. The close transaction of
	// the options update cannot skip, but the fast-forward timer task is still scheduled.
	updateTimeSkipping(&commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"},
	})
	tsi := timeSkippingInfo()
	s.True(tsi.GetConfig().GetEnabled())
	s.False(tsi.GetFastForwardInfo().GetHasReached(), "the fast-forward starts out pending")
	s.False(hasEventType(env.GetHistory(env.Namespace().String(), execution), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED),
		"enabling time skipping on a paused workflow must not skip")

	// (4) Pause does not hold off the fast-forward. Its timer task is wall-clock-anchored, and
	// executeTimeSkippingTimerTask gates only on IsWorkflowExecutionRunning (a paused workflow
	// is still RUNNING), so once 2s of real time pass it fires through pause and turns time
	// skipping off. What it must NOT do is advance the clock of a paused workflow.
	s.AwaitTruef(func() bool {
		return timeSkippingInfo().GetFastForwardInfo().GetHasReached()
	}, fastForwardWait, 500*time.Millisecond, "the fast-forward timer task must fire through pause once its wall-clock target passes")

	tsi = timeSkippingInfo()
	s.False(tsi.GetConfig().GetEnabled(), "the fast-forward must turn time skipping off even though the workflow is paused")
	s.Zero(tsi.GetAccumulatedSkippedDuration().AsDuration(),
		"the fast-forward disable must not advance the clock of a paused workflow")

	histPaused := env.GetHistory(env.Namespace().String(), execution)
	s.False(hasEventType(histPaused, enumspb.EVENT_TYPE_TIMER_FIRED), "t1 must not fire while paused")
	transitions := timeSkippingTransitions(histPaused)
	s.Len(transitions, 1, "the only transition so far is the fast-forward disable")
	s.True(transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward(),
		"the transition written while paused must be the fast-forward disable, not a skip")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, workflowStatus())

	// (5) Still paused: re-enable time skipping, this time with no fast-forward config.
	updateTimeSkipping(&commonpb.TimeSkippingConfig{Enabled: true})
	tsi = timeSkippingInfo()
	s.True(tsi.GetConfig().GetEnabled(), "time skipping is enabled again")
	s.Nil(tsi.GetConfig().GetFastForwardConfig(), "the update dropped the fast-forward config")

	// (6) Nothing skips while the workflow stays paused, even with skipping enabled and no
	// fast-forward to bound it.
	time.Sleep(settleWindow) //nolint:forbidigo // negative assertion: give a skip time to happen
	tsi = timeSkippingInfo()
	s.True(tsi.GetConfig().GetEnabled(), "time skipping must stay enabled — nothing disabled it")
	s.Zero(tsi.GetAccumulatedSkippedDuration().AsDuration(), "a paused workflow must not skip time")
	histStillPaused := env.GetHistory(env.Namespace().String(), execution)
	s.False(hasEventType(histStillPaused, enumspb.EVENT_TYPE_TIMER_FIRED), "t1 must not fire while paused")
	s.Len(timeSkippingTransitions(histStillPaused), 1, "no new transition while paused")

	// (7) Unpause: the workflow can finally skip to t1 and run to completion.
	unpauseWall := time.Now()
	_, err = env.FrontendClient().UnpauseWorkflowExecution(ctx, &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: tv.WorkflowID(),
		RunId:      runID,
		Identity:   "test",
		Reason:     "let the workflow skip",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// poll the wf task created by unpasue
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)
	// poll the wf task created by user timer
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.True(firedTimers(task)["t1"], "the workflow task after unpause must carry TimerFired for t1")
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	sinceUnpause := time.Since(unpauseWall)
	s.Less(sinceUnpause, time.Minute,
		"the workflow should skip to t1 and finish promptly after unpause; took %v of the 5min timer", sinceUnpause)

	tsi = timeSkippingInfo()
	s.InDelta(float64(timerDuration), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(time.Minute),
		"the post-unpause skip covers what was left of the 5min timer")
	s.True(tsi.GetConfig().GetEnabled(), "one skip with no fast-forward and a fresh session count leaves skipping enabled")

	histFinal := env.GetHistory(env.Namespace().String(), execution)
	s.True(hasEventType(histFinal, enumspb.EVENT_TYPE_TIMER_FIRED), "t1 must fire once the workflow is unpaused")
	s.True(hasEventType(histFinal, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED))
	s.True(hasEventType(histFinal, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED))
	s.True(hasEventType(histFinal, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED))
	transitions = timeSkippingTransitions(histFinal)
	s.Len(transitions, 2, "exactly two transitions: the fast-forward disable while paused, the skip after unpause")
	s.True(transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward())
	s.False(transitions[1].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward(),
		"the post-unpause transition is a plain skip, not a fast-forward disable")

	elapsed := time.Since(testStart)
	s.Less(elapsed, maxTestDuration,
		"the whole test took %v: %v of that is the fast-forward and settle waits, so the rest is RPC time — the 5min timer must be skipped, not waited out",
		elapsed, fastForward+settleWindow)
}
