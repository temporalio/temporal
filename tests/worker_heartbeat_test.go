package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	chasmworker "go.temporal.io/server/chasm/lib/worker"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type workerHeartbeatTestSuite struct {
	testcore.FunctionalTestBase

	chasmEngine chasm.Engine
}

func TestWorkerHeartbeatTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(workerHeartbeatTestSuite))
}

func (s *workerHeartbeatTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key():               true,
			dynamicconfig.EnableWorkerStateTracking.Key(): true,
		}),
	)

	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *workerHeartbeatTestSuite) TestRecordHeartbeat() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	tv := testvars.New(t)
	workerInstanceKey := tv.Any().String()

	resp, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify the worker entity was created in CHASM
	engineCtx := chasm.NewEngineContext(ctx, s.chasmEngine)
	worker, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)

	require.NoError(t, err)
	require.NotNil(t, worker)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker.Status)
	require.Equal(t, workerInstanceKey, worker.GetWorkerHeartbeat().GetWorkerInstanceKey())
	require.NotNil(t, worker.GetLeaseExpirationTime())
}

func (s *workerHeartbeatTestSuite) TestRecordMultipleHeartbeats() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	tv := testvars.New(t)
	workerInstanceKey := tv.Any().String()

	req := &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-identity",
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: workerInstanceKey,
			},
		},
	}

	// Send first heartbeat
	resp1, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp1)

	// Read worker state after first heartbeat
	engineCtx := chasm.NewEngineContext(ctx, s.chasmEngine)
	worker1, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, worker1)
	firstLeaseExpiration := worker1.GetLeaseExpirationTime().AsTime()

	// Wait a bit to ensure lease expiration time will be different
	time.Sleep(100 * time.Millisecond)

	// Send second heartbeat for the same worker
	resp2, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp2)

	// Verify that the second heartbeat updated the existing worker entity
	worker2, err := chasm.ReadComponent(
		engineCtx,
		chasm.NewComponentRef[*chasmworker.Worker](
			chasm.ExecutionKey{
				NamespaceID: s.NamespaceID().String(),
				BusinessID:  workerInstanceKey,
			},
		),
		func(w *chasmworker.Worker, _ chasm.Context, _ any) (*chasmworker.Worker, error) {
			return w, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, worker2)
	require.Equal(t, workerstatepb.WORKER_STATUS_ACTIVE, worker2.Status)
	require.Equal(t, workerInstanceKey, worker2.GetWorkerHeartbeat().GetWorkerInstanceKey())

	// Verify lease expiration time was updated (extended)
	secondLeaseExpiration := worker2.GetLeaseExpirationTime().AsTime()
	require.True(t, secondLeaseExpiration.After(firstLeaseExpiration),
		"Second heartbeat should extend the lease expiration time")
}

func (s *workerHeartbeatTestSuite) TestForceRescheduleActivity_StartsActivity() {
	// This test verifies that ForceRescheduleActivity correctly reschedules
	// an activity that is in STARTED state (i.e., running on a worker).
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	tv := testvars.New(t)
	taskQueue := tv.TaskQueue().Name

	// Step 1: Start a workflow that schedules an activity
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
		Identity:     "test-worker",
		WorkflowExecutionTimeout: &durationpb.Duration{
			Seconds: 300,
		},
	})
	require.NoError(t, err)
	runID := startResp.RunId

	// Step 2: Poll for workflow task and schedule an activity
	pollWTResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  "test-worker",
	})
	require.NoError(t, err)
	require.NotNil(t, pollWTResp)

	// Complete workflow task with an activity command
	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollWTResp.TaskToken,
		Identity:  "test-worker",
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:   "test-activity-1",
						ActivityType: tv.ActivityType(),
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ScheduleToCloseTimeout: &durationpb.Duration{Seconds: 300},
						StartToCloseTimeout:    &durationpb.Duration{Seconds: 300},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Step 3: Poll for activity task - this puts the activity in STARTED state
	pollATResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test-worker",
	})
	require.NoError(t, err)
	require.NotNil(t, pollATResp)
	require.Equal(t, "test-activity-1", pollATResp.ActivityId)

	// Step 4: Verify activity is in STARTED state
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      runID,
		},
	})
	require.NoError(t, err)
	require.Len(t, descResp.PendingActivities, 1)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, descResp.PendingActivities[0].State)
	originalIdentity := descResp.PendingActivities[0].LastWorkerIdentity

	// Step 5: Call ForceRescheduleActivity
	historyClient := s.GetTestCluster().HistoryClient()
	_, err = historyClient.ForceRescheduleActivity(ctx, &historyservice.ForceRescheduleActivityRequest{
		NamespaceId: s.NamespaceID().String(),
		WorkflowId:  tv.WorkflowID(),
		RunId:       runID,
		ActivityId:  "test-activity-1",
	})
	require.NoError(t, err)

	// Step 6: Verify activity is back in SCHEDULED state
	descResp2, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      runID,
		},
	})
	require.NoError(t, err)
	require.Len(t, descResp2.PendingActivities, 1)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp2.PendingActivities[0].State,
		"Activity should be back in SCHEDULED state after ForceRescheduleActivity")
	require.Empty(t, descResp2.PendingActivities[0].LastWorkerIdentity,
		"LastWorkerIdentity should be cleared after reschedule")

	t.Logf("Successfully rescheduled activity from STARTED (identity=%s) to SCHEDULED", originalIdentity)

	// Step 7: Verify the activity can be picked up again
	pollATResp2, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "new-worker",
	})
	require.NoError(t, err)
	require.NotNil(t, pollATResp2)
	require.Equal(t, "test-activity-1", pollATResp2.ActivityId)

	t.Log("Activity was successfully picked up by a new worker after reschedule")
}
