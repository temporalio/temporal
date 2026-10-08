package localserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// BenchmarkAgentLoop runs workflows that each run 20 activities in sequence, advancing the clock
// before every call as the host does. The cost per workflow should not depend on how many
// workflows the server has already run.
func BenchmarkAgentLoop(b *testing.B) {
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	runIDs := 0
	s, err := New(now, func() string { runIDs++; return fmt.Sprintf("run-%d", runIDs) })
	require.NoError(b, err)
	for i := range b.N {
		runAgentLoop(b, s, fmt.Sprintf("wf-%d", i), 20)
	}
}

func runAgentLoop(b testing.TB, s *Server, workflowID string, steps int) {
	ctx := context.Background()
	advance := func() { require.NoError(b, s.AdvanceTime(ctx, s.engine.now)) }
	advance()
	_, err := s.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    testNamespace,
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: "AgentLoop"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	require.NoError(b, err)
	for step := range steps + 1 {
		advance()
		wft := pollWorkflowTask(b, s)
		advance()
		if step == steps {
			completeWorkflowTask(b, s, wft, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			})
			return
		}
		completeWorkflowTask(b, s, wft, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          fmt.Sprint(step),
				ActivityType:        &commonpb.ActivityType{Name: "Step"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				StartToCloseTimeout: durationpb.New(10 * time.Second),
			}},
		})
		advance()
		act, err := s.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: testNamespace,
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		})
		require.NoError(b, err)
		advance()
		_, err = s.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: testNamespace,
			TaskToken: act.GetTaskToken(),
		})
		require.NoError(b, err)
	}
}
