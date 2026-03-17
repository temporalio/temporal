package tests

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
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
				RequestId:  testcore.RandomizeStr("request-id"),
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

	// Directly calling CreateFromMigrationState when a CHASM schedule already
	// exists should return AlreadyExists, matching CreateSchedule's behavior.
	_, err = env.GetTestCluster().SchedulerClient().CreateFromMigrationState(
		ctx,
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: nsID,
			State: &schedulerpb.SchedulerMigrationState{
				SchedulerState: &schedulerpb.SchedulerState{
					Namespace:   nsName,
					NamespaceId: nsID,
					ScheduleId:  sid,
					Schedule:    sched,
				},
				GeneratorState: &schedulerpb.GeneratorState{},
				InvokerState:   &schedulerpb.InvokerState{},
			},
		},
	)
	var alreadyExists *serviceerror.AlreadyExists
	require.ErrorAs(t, err, &alreadyExists)
	require.Contains(t, alreadyExists.Error(), sid)

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
		RequestId:                testcore.RandomizeStr("request-id"),
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
		RequestId:  testcore.RandomizeStr("request-id"),
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

func TestScheduleMigrationDynamicConfig(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-dc")
	wid := testcore.RandomizeStr("sched-migrate-dc-wf")
	wt := testcore.RandomizeStr("sched-migrate-dc-wt")
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
		RequestId:                testcore.RandomizeStr("request-id"),
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

	// V1 workflow should automatically migrate due to dynamic config and complete.
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
	}, 30*time.Second, 500*time.Millisecond)

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
		RequestId:                testcore.RandomizeStr("request-id"),
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
		RequestId:  testcore.RandomizeStr("request-id"),
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

func TestScheduleMigrationV2ToV1(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		// Explicitly disable CHASM scheduler creation so the frontend's
		// DescribeSchedule routes directly to V1 (workflow-backed) without
		// attempting the CHASM path first. This lets us verify the V1 schedule
		// is fully functional after migration.
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-v2-to-v1")
	wid := testcore.RandomizeStr("sched-migrate-v2-to-v1-wf")
	wt := testcore.RandomizeStr("sched-migrate-v2-to-v1-wt")
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
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			CatchupWindow: durationpb.New(time.Minute),
		},
		State: &schedulepb.ScheduleState{
			Notes: "original notes",
		},
	}

	// Create CHASM schedule directly.
	_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(
		ctx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Schedule:   sched,
				Identity:   "test",
				RequestId:  testcore.RandomizeStr("request-id"),
			},
		},
	)
	require.NoError(t, err)

	// Describe the CHASM schedule before migration to capture its state.
	v2Desc, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)
	v2Schedule := v2Desc.GetFrontendResponse().GetSchedule()
	v2ConflictToken := v2Desc.GetFrontendResponse().GetConflictToken()

	// Migrate from V2 (CHASM) to V1 (workflow).
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Wait for V1 workflow to start and become describable via the frontend.
	// Because EnableCHASMSchedulerCreation is false, the frontend routes
	// DescribeSchedule directly to the V1 (workflow-backed) path.
	var v1Desc *workflowservice.DescribeScheduleResponse
	require.Eventually(t, func() bool {
		v1Desc, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	v1Schedule := v1Desc.GetSchedule()

	// Validate the schedule spec is preserved across migration.
	require.Len(t, v1Schedule.GetSpec().GetInterval(), len(v2Schedule.GetSpec().GetInterval()))
	require.Equal(t,
		v2Schedule.GetSpec().GetInterval()[0].GetInterval().AsDuration(),
		v1Schedule.GetSpec().GetInterval()[0].GetInterval().AsDuration(),
	)

	// Validate the action is preserved.
	v2Action := v2Schedule.GetAction().GetStartWorkflow()
	v1Action := v1Schedule.GetAction().GetStartWorkflow()
	require.Equal(t, v2Action.GetWorkflowId(), v1Action.GetWorkflowId())
	require.Equal(t, v2Action.GetWorkflowType().GetName(), v1Action.GetWorkflowType().GetName())
	require.Equal(t, v2Action.GetTaskQueue().GetName(), v1Action.GetTaskQueue().GetName())

	// Validate policies are preserved.
	require.Equal(t,
		v2Schedule.GetPolicies().GetOverlapPolicy(),
		v1Schedule.GetPolicies().GetOverlapPolicy(),
	)
	require.Equal(t,
		v2Schedule.GetPolicies().GetCatchupWindow().AsDuration(),
		v1Schedule.GetPolicies().GetCatchupWindow().AsDuration(),
	)

	// Validate the paused state is correctly restored (not the migration-imposed pause).
	require.Equal(t, v2Schedule.GetState().GetPaused(), v1Schedule.GetState().GetPaused())
	require.Equal(t, v2Schedule.GetState().GetNotes(), v1Schedule.GetState().GetNotes())

	// Validate the conflict token value is preserved across migration.
	// V2 (CHASM) serializes as LittleEndian, V1 (workflow) as BigEndian, so decode both to int64.
	require.Len(t, v2ConflictToken, 8)
	v2Token := int64(binary.LittleEndian.Uint64(v2ConflictToken))
	v1ConflictToken := v1Desc.GetConflictToken()
	require.Len(t, v1ConflictToken, 8)
	v1Token := int64(binary.BigEndian.Uint64(v1ConflictToken))
	require.Equal(t, v2Token, v1Token)

	// Verify the CHASM scheduler is closed after migration.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	// Closed CHASM schedulers return ErrClosed (FailedPrecondition).
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)
}

func TestScheduleMigrationV2ToV1Idempotent(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-v2-to-v1-idem")
	wid := testcore.RandomizeStr("sched-migrate-v2-to-v1-idem-wf")
	wt := testcore.RandomizeStr("sched-migrate-v2-to-v1-idem-wt")
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

	// Create CHASM schedule.
	_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(
		ctx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Schedule:   sched,
				Identity:   "test",
				RequestId:  testcore.RandomizeStr("request-id"),
			},
		},
	)
	require.NoError(t, err)

	// First migration call.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Second migration call should also succeed (idempotent).
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)
}
