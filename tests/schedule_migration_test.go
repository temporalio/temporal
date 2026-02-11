package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestScheduleMigrationV2AlreadyExists(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-v2-exists")
	wid := testcore.RandomizeStr("sched-migrate-v2-exists-wf")
	wt := testcore.RandomizeStr("sched-migrate-v2-exists-wt")
	tq := testcore.RandomizeStr("tq")

	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()
	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create CHASM Schedule directly
	_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(
		ctx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Schedule:   sched,
				Identity:   "test",
				RequestId:  uuid.NewString(),
			},
		},
	)
	require.NoError(t, err)

	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)

	// Create the V1 (workflow-backed) scheduler directly
	startArgs := &schedulespb.StartScheduleArgs{
		Schedule: sched,
		State: &schedulespb.InternalState{
			Namespace:     nsName,
			NamespaceId:   nsID,
			ScheduleId:    sid,
			ConflictToken: scheduler.InitialConflictToken,
		},
	}
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(startArgs)
	require.NoError(t, err)
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                nsName,
		WorkflowId:               v1WorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 "test",
		RequestId:                uuid.NewString(),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}
	_, err = env.GetTestCluster().HistoryClient().StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(nsID, startReq, nil, nil, time.Now().UTC()),
	)
	require.NoError(t, err)

	_, err = env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
		ctx,
		&historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: nsID,
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: nsName,
				Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
			},
		},
	)
	require.NoError(t, err)

	// Issue migration. The CHASM handler will return AlreadyStarted,
	// and the V1 activity treats that as success (logs warning, returns nil).
	// The V1 workflow terminates, but the pre-existing V2 schedule retains
	// its original state -- the V1 state is not applied.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: nsID,
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, 500*time.Millisecond)

	// The V2 schedule should still exist and be describable after migration.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)
}

func TestScheduleMigrationV1ToV2(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-v1-to-v2")
	wid := testcore.RandomizeStr("sched-migrate-v1-to-v2-wf")
	wt := testcore.RandomizeStr("sched-migrate-v1-to-v2-wt")
	tq := testcore.RandomizeStr("tq")

	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()
	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create the V1 (workflow-backed) scheduler directly.
	startArgs := &schedulespb.StartScheduleArgs{
		Schedule: sched,
		State: &schedulespb.InternalState{
			Namespace:     nsName,
			NamespaceId:   nsID,
			ScheduleId:    sid,
			ConflictToken: scheduler.InitialConflictToken,
		},
	}
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(startArgs)
	require.NoError(t, err)
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                nsName,
		WorkflowId:               v1WorkflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 "test",
		RequestId:                uuid.NewString(),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}
	_, err = env.GetTestCluster().HistoryClient().StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(nsID, startReq, nil, nil, time.Now().UTC()),
	)
	require.NoError(t, err)

	// Wait for the per-namespace worker to pick up the V1 workflow.
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: nsID,
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetHistoryLength() > 3
	}, 10*time.Second, 500*time.Millisecond)

	// Issue migration from V1 to V2.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for V1 workflow to complete.
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: nsID,
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, 500*time.Millisecond)

	// V2 schedule should now exist.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)
}
