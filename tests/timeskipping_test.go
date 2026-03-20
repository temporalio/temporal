package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type TimeSkippingTestSuite struct {
	testcore.FunctionalTestBase
}

func TestTimeSkippingTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TimeSkippingTestSuite))
}

func (s *TimeSkippingTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

func (s *TimeSkippingTestSuite) updateTimeSkipping(
	workflowExecution *commonpb.WorkflowExecution,
	identity string,
	enabled bool,
) *workflowservice.UpdateWorkflowExecutionOptionsResponse {
	resp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: workflowExecution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: enabled},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
		Identity:   identity,
	})
	s.NoError(err)
	return resp
}

// TestTimeSkipping_EnabledToDisabled starts a workflow with time skipping enabled,
// then disables it via UpdateWorkflowExecutionOptions. Verifies a
// WorkflowExecutionOptionsUpdated event is written with Enabled=false.
func (s *TimeSkippingTestSuite) TestTimeSkipping_EnabledToDisabled() {
	s.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)

	id := "functional-timeskipping-enabled-to-disabled"
	tl := "functional-timeskipping-enabled-to-disabled-tq"
	tv := testvars.New(s.T()).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// Disable time skipping — a real change that must produce an options-updated event.
	updateResp := s.updateTimeSkipping(workflowExecution, tv.WorkerIdentity(), false)
	s.False(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// todo: @feiyang, will change with a new event type after data plane is added
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)

	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			s.False(event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig().GetEnabled())
		}
	}
}

// TestTimeSkipping_DisabledToEnabled starts a workflow without time skipping,
// then enables it via UpdateWorkflowExecutionOptions. Verifies a
// WorkflowExecutionOptionsUpdated event is written with Enabled=true.
func (s *TimeSkippingTestSuite) TestTimeSkipping_DisabledToEnabled() {
	s.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)

	id := "functional-timeskipping-disabled-to-enabled"
	tl := "functional-timeskipping-disabled-to-enabled-tq"
	tv := testvars.New(s.T()).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// Enable time skipping — a real change that must produce an options-updated event.
	updateResp := s.updateTimeSkipping(workflowExecution, tv.WorkerIdentity(), true)
	s.True(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// todo: @feiyang, will change with a new event type after data plane is added
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)

	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			s.True(event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig().GetEnabled())
		}
	}
}

// TestTimeSkipping_DisabledToDisabled starts a workflow with time skipping enabled,
// disables it, then attempts to disable it again. The second update is a no-op
// and must not produce a second WorkflowExecutionOptionsUpdated event.
func (s *TimeSkippingTestSuite) TestTimeSkipping_DisabledToDisabled() {
	s.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)

	id := "functional-timeskipping-disabled-to-disabled"
	tl := "functional-timeskipping-disabled-to-disabled-tq"
	tv := testvars.New(s.T()).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// First update: enabled → disabled. This is a real change that produces an event.
	s.updateTimeSkipping(workflowExecution, tv.WorkerIdentity(), false)

	// Second update: disabled → disabled again. Must be a no-op with no additional event.
	updateResp := s.updateTimeSkipping(workflowExecution, tv.WorkerIdentity(), false)
	s.False(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// Only one WorkflowExecutionOptionsUpdated event (from the first update).
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)
}

// TestTimeSkipping_FeatureDisabled verifies that starting a workflow with time skipping
// returns an error when the feature flag is off for the namespace.
func (s *TimeSkippingTestSuite) TestTimeSkipping_FeatureDisabled() {
	// TimeSkippingEnabled defaults to false; no override needed.
	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.Error(err, "expected error when time skipping is disabled for namespace")
}
