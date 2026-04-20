package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

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
	// TimeSkippingEnabled defaults to false; no override needed.
	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.Error(err, "expected error when time skipping is disabled for namespace")
}

// TestTimeSkipping_StartWorkflow_DCEnabled verifies that StartWorkflowExecution with
// TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_StartWorkflow_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
		MaxSkippedDuration: durationpb.New(time.Hour),
	}

	resp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true, DisablePropagation: false, Bound: inputBound},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled())
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: false,
		Bound:              inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_SignalWithStart_DCEnabled verifies that SignalWithStartWorkflowExecution
// with TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_SignalWithStart_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxElapsedDuration{
		MaxElapsedDuration: durationpb.New(time.Hour),
	}

	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		SignalName:          tv.SignalName(),
		TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
			Enabled:            true,
			DisablePropagation: true,
			Bound:              inputBound,
		},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound:              inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_ExecuteMultiOperation_DCEnabled verifies that a StartWorkflow inside
// ExecuteMultiOperation with TimeSkippingConfig persists the config in mutable state
// when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ExecuteMultiOperation_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	maxSkippedDuration := time.Hour

	inputConfig := &workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
			MaxSkippedDuration: durationpb.New(maxSkippedDuration),
		},
	}

	resp, err := env.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), &workflowservice.ExecuteMultiOperationRequest{
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
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_UpdateWorkflowOptions_DCEnabled exercises the full UpdateWorkflowExecutionOptions
// lifecycle for TimeSkippingConfig:
//  1. Start workflow with no time-skipping — assert mutable state has no config.
//  2. First update: enable with MaxSkippedDuration bound — check MS and event 1 attributes.
//  3. Second update: change bound to MaxElapsedDuration, add DisablePropagation — check MS and event 2 attributes.
//  4. Third update: disable (Enabled=false) — check MS and event 3 attributes.
//  5. Assert exactly 3 WorkflowExecutionOptionsUpdated events appear in history.
func (s *TimeSkippingTestSuite) TestTimeSkipping_UpdateWorkflowOptions_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Start a workflow without any time-skipping config.
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
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
		histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
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
	updateOptions := func(cfg *workflowpb.TimeSkippingConfig) {
		_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
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

	// First update: enable with a bound.
	config1 := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(time.Hour)},
	}
	updateOptions(config1)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config1, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	events := collectOptionsEvents()
	s.Len(events, 1)
	s.True(proto.Equal(config1, events[0].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Second update: change bound type, add DisablePropagation.
	config2 := &workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound:              &workflowpb.TimeSkippingConfig_MaxElapsedDuration{MaxElapsedDuration: durationpb.New(2 * time.Hour)},
	}
	updateOptions(config2)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config2, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	events = collectOptionsEvents()
	s.Len(events, 2)
	s.True(proto.Equal(config2, events[1].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Third update: disable time-skipping.
	config3 := &workflowpb.TimeSkippingConfig{Enabled: false}
	updateOptions(config3)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config3, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
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
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

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
	inputConfig := &workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound:              &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(time.Hour)},
	}
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

	// New run's mutable state must have the config.
	ms := s.getMutableState(env, tv.WorkflowID(), newRunID)
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
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(testcore.NewContext(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}
