package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// heartbeatTimerTaskTypeID is the CHASM registered task type ID for the "heartbeatTimer" pure
// task defined in chasm/lib/activity/library.go. It's derived the same way the CHASM registry
// derives it internally, so it identifies HeartbeatTimeoutTask entries in a persisted
// ChasmComponentAttributes.PureTasks list.
var heartbeatTimerTaskTypeID = chasm.GenerateTypeID(chasm.FullyQualifiedName("activity", "heartbeatTimer"))

type ActivityHeartbeatTimerTaskParitySuite struct {
	parallelsuite.Suite[*ActivityHeartbeatTimerTaskParitySuite]
}

func TestActivityHeartbeatTimerTaskParitySuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityHeartbeatTimerTaskParitySuite{})
}

// getMutableState loads the persisted mutable state for a workflow/activity execution directly
// from the persistence layer, bypassing caches, so tests can assert on internal timer task
// bookkeeping (ActivityInfo.TimerTaskStatus for legacy workflow activities, or
// ChasmComponentAttributes.PureTasks for CHASM standalone activities).
func (s *ActivityHeartbeatTimerTaskParitySuite) getMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID, runID string,
	archetypeID chasm.ArchetypeID,
) *persistencespb.WorkflowMutableState {
	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		workflowID,
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	resp, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: archetypeID,
	})
	s.NoError(err)
	return resp.State
}

// TestWorkflowActivityHeartbeat_TimerTaskDoesNotAccumulate is the legacy WFA (workflow activity)
// baseline: repeated heartbeats must coalesce onto a single outstanding heartbeat timeout timer,
// tracked via the TimerTaskStatusCreatedHeartbeat bit on ActivityInfo, rather than creating a new
// persisted timer task per heartbeat.
func (s *ActivityHeartbeatTimerTaskParitySuite) TestWorkflowActivityHeartbeat_TimerTaskDoesNotAccumulate() {
	env := testcore.NewEnv(s.T())
	ctx := s.Context()
	id := "heartbeat-timer-parity-wfa"
	tl := "heartbeat-timer-parity-wfa-tq"
	identity := "test-worker"
	activityName := "activity_timer"

	we, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: "heartbeat-timer-parity-wfa-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(5 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             "1",
				ActivityType:           &commonpb.ActivityType{Name: activityName},
				TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:                  payloads.EncodeString("activity-input"),
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(30 * time.Second),
				HeartbeatTimeout:       durationpb.New(20 * time.Second),
			}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	for i := 0; i < 10; i++ {
		_, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   payloads.EncodeString("details"),
		})
		s.NoError(err)
	}

	await.Require(ctx, s.T(), func(c *await.T) {
		mutableState := s.getMutableState(ctx, env, id, we.RunId, chasm.WorkflowArchetypeID)
		require.Len(c, mutableState.ActivityInfos, 1)
		for _, ai := range mutableState.ActivityInfos {
			require.NotZero(c, ai.TimerTaskStatus&workflow.TimerTaskStatusCreatedHeartbeat,
				"expected a single outstanding heartbeat timer to have been created")
		}
	}, 5*time.Second, 100*time.Millisecond)
}

// TestStandaloneActivityHeartbeat_TimerTaskDoesNotAccumulate is the SAA (standalone activity,
// chasm/lib/activity) counterpart to the legacy WFA case above: even though RecordHeartbeat
// unconditionally calls ctx.AddTask on every heartbeat, the CHASM framework's per-transaction
// task validation (chasm.Node.closeTransactionCleanupInvalidTasks) removes the previous
// HeartbeatTimeoutTask before the new one is added, so at most one outstanding heartbeat
// timeout task exists per activity attempt at any time, mirroring the legacy WFA behavior.
func (s *ActivityHeartbeatTimerTaskParitySuite) TestStandaloneActivityHeartbeat_TimerTaskDoesNotAccumulate() {
	env := testcore.NewEnv(s.T())
	ctx := s.Context()
	activityID := "heartbeat-timer-parity-saa"
	taskQueue := "heartbeat-timer-parity-saa-tq"
	identity := "test-worker"

	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, nsValues(true))

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "activity_timer"},
		Identity:            identity,
		Input:               payloads.EncodeString("activity-input"),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(30 * time.Second),
		HeartbeatTimeout:    durationpb.New(20 * time.Second),
		RequestId:           uuid.NewString(),
	})
	s.NoError(err)
	s.True(startResp.Started)

	pollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  identity,
	})
	s.NoError(err)
	s.NotEmpty(pollResp.TaskToken)

	for i := 0; i < 10; i++ {
		_, err := env.FrontendClient().RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Details:   payloads.EncodeString("details"),
		})
		s.NoError(err)
	}

	await.Require(ctx, s.T(), func(c *await.T) {
		mutableState := s.getMutableState(ctx, env, activityID, startResp.RunId, activity.ArchetypeID)
		rootNode, ok := mutableState.ChasmNodes[""]
		require.True(c, ok, "expected a root CHASM node for the standalone activity")
		pureTasks := rootNode.GetMetadata().GetComponentAttributes().GetPureTasks()
		heartbeatTasks := 0
		for _, task := range pureTasks {
			if task.TypeId == heartbeatTimerTaskTypeID {
				heartbeatTasks++
			}
		}
		require.Equal(c, 1, heartbeatTasks,
			"expected a single outstanding heartbeat timeout task, got %d persisted pure tasks total, %d of them heartbeat timers",
			len(pureTasks), heartbeatTasks)
	}, 5*time.Second, 100*time.Millisecond)
}
