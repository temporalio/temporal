package tests

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
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
	"google.golang.org/protobuf/types/known/timestamppb"
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
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, false),
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

	// Wait for the CHASM scheduler to be closed after migration.
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.Eventually(t, func() bool {
		_, chasmErr := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
			},
		)
		return errors.As(chasmErr, &failedPreconditionErr)
	}, 10*time.Second, 500*time.Millisecond)

	// Wait for the V1 system scheduler workflow to be running.
	sysWorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		_, descErr := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: nsID,
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: sysWorkflowID},
				},
			},
		)
		return descErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Describe the V1 schedule via the frontend. With routing disabled, this
	// goes directly to the V1 path. The per-namespace worker must pick up
	// the workflow and register query handlers before this succeeds.
	var v1Desc *workflowservice.DescribeScheduleResponse
	require.Eventually(t, func() bool {
		v1Desc, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return err == nil
	}, 30*time.Second, 500*time.Millisecond)

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

	// Validate ListSchedules returns exactly one entry once the V1 workflow
	// has written its visibility records (no duplicates from V1+V2).
	var listResp *workflowservice.ListSchedulesResponse
	require.Eventually(t, func() bool {
		listResp, err = env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       nsName,
			MaximumPageSize: 10,
		})
		return err == nil && len(listResp.GetSchedules()) == 1
	}, 30*time.Second, 500*time.Millisecond)
	require.Equal(t, sid, listResp.GetSchedules()[0].GetScheduleId())
}

func TestScheduleMigrationV2ToV1Idempotent(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, false),
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

func TestCHASMScheduleDescribeAfterDisablingCreationAndMigration(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, true),
	)

	ctx := testcore.NewContext()
	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()
	sid := testcore.RandomizeStr("sched-routing-after-disable")
	wid := testcore.RandomizeStr("sched-routing-after-disable-wf")
	wt := testcore.RandomizeStr("sched-routing-after-disable-wt")
	tq := testcore.RandomizeStr("tq")

	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
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

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	firstDescribe, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotNil(t, firstDescribe.GetSchedule())
	require.Eventually(t, func() bool {
		listResp, listErr := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: nsName})
		if listErr != nil {
			return false
		}
		for _, schedule := range listResp.GetSchedules() {
			if schedule.GetScheduleId() == sid {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond)

	// Verify the schedule exists in CHASM by describing it directly through the
	// scheduler client (history-only path that only goes to CHASM).
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)

	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, false)

	require.Eventually(t, func() bool {
		describeResp, describeErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		if describeErr != nil {
			return false
		}
		if describeResp.GetSchedule() == nil {
			return false
		}
		listResp, listErr := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{Namespace: nsName})
		if listErr != nil {
			return false
		}
		for _, schedule := range listResp.GetSchedules() {
			if schedule.GetScheduleId() == sid {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond)
}

// TestScheduleMigrationV2ToV1RoutingFallback verifies that after migrating a
// CHASM schedule to V1, frontend operations with CHASM routing enabled fall
// through to the V1 workflow stack when the CHASM scheduler returns ErrClosed.
func TestScheduleMigrationV2ToV1RoutingFallback(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-v2-to-v1-routing")
	wid := testcore.RandomizeStr("sched-v2-to-v1-routing-wf")
	wt := testcore.RandomizeStr("sched-v2-to-v1-routing-wt")
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

	// Migrate from V2 (CHASM) to V1 (workflow).
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Wait for the CHASM scheduler to be closed after migration.
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.Eventually(t, func() bool {
		_, chasmErr := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
			},
		)
		return errors.As(chasmErr, &failedPreconditionErr)
	}, 10*time.Second, 500*time.Millisecond)

	// Wait for the V1 workflow to be running and query handlers registered.
	require.Eventually(t, func() bool {
		_, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return descErr == nil
	}, 30*time.Second, 500*time.Millisecond)

	// With CHASM routing still enabled, DescribeSchedule through the frontend
	// should succeed by falling through from the closed CHASM schedule to V1.
	descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotNil(t, descResp.GetSchedule())
	require.Equal(t, wt, descResp.GetSchedule().GetAction().GetStartWorkflow().GetWorkflowType().GetName())
	// The schedule was created unpaused; migration should preserve that state
	// (not the temporary migration-imposed pause).
	require.False(t, descResp.GetSchedule().GetState().GetPaused())

	// ListScheduleMatchingTimes should also fall through to V1.
	now := time.Now().UTC()
	matchResp, err := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		StartTime:  timestamppb.New(now),
		EndTime:    timestamppb.New(now.Add(5 * time.Hour)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, matchResp.GetStartTime())

	// PatchSchedule (pause) should also fall through to V1.
	_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "pausing via routing fallback test",
		},
		Identity: "test",
	})
	require.NoError(t, err)

	// Verify the pause took effect on V1. The patch is delivered as a signal,
	// so the workflow needs time to process it.
	require.Eventually(t, func() bool {
		descResp, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return err == nil && descResp.GetSchedule().GetState().GetPaused()
	}, 10*time.Second, 500*time.Millisecond)

	// DeleteSchedule should also fall through to V1.
	_, err = env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Identity:   "test",
	})
	require.NoError(t, err)
}

func TestScheduleUpdateAfterDelete(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-update-after-delete")
	wid := testcore.RandomizeStr("sched-update-after-delete-wf")
	wt := testcore.RandomizeStr("sched-update-after-delete-wt")
	tq := testcore.RandomizeStr("tq")

	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()

	schedule := &schedulepb.Schedule{
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
				Schedule:   schedule,
				Identity:   "test",
				RequestId:  testcore.RandomizeStr("request-id"),
			},
		},
	)
	require.NoError(t, err)

	// Delete via scheduler client.
	_, err = env.GetTestCluster().SchedulerClient().DeleteSchedule(
		ctx,
		&schedulerpb.DeleteScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Identity:   "test",
			},
		},
	)
	require.NoError(t, err)

	// Update via scheduler client should fail on the closed schedule.
	_, err = env.GetTestCluster().SchedulerClient().UpdateSchedule(
		ctx,
		&schedulerpb.UpdateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.UpdateScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Schedule:   schedule,
				Identity:   "test",
			},
		},
	)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPreconditionErr)

	// Patch via scheduler client should also fail on the closed schedule.
	_, err = env.GetTestCluster().SchedulerClient().PatchSchedule(
		ctx,
		&schedulerpb.PatchScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Patch:      &schedulepb.SchedulePatch{Pause: "test"},
				Identity:   "test",
			},
		},
	)
	require.ErrorAs(t, err, &failedPreconditionErr)

	// Delete again is idempotent in CHASM — sets Closed=true again.
	_, err = env.GetTestCluster().SchedulerClient().DeleteSchedule(
		ctx,
		&schedulerpb.DeleteScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Identity:   "test",
			},
		},
	)
	require.NoError(t, err)
}

func TestScheduleMigrationV1ToV2WithClosedV2(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-v1-v2-closed")
	wid := testcore.RandomizeStr("sched-migrate-v1-v2-closed-wf")
	wt := testcore.RandomizeStr("sched-migrate-v1-v2-closed-wt")
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

	// Create a CHASM schedule and then delete it.
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

	_, err = env.GetTestCluster().SchedulerClient().DeleteSchedule(
		ctx,
		&schedulerpb.DeleteScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.DeleteScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Identity:   "test",
			},
		},
	)
	require.NoError(t, err)

	// Create a V1 (workflow-backed) scheduler with the same ID.
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

	// Issue migration from V1 to V2. The previously deleted CHASM execution
	// does not block creation of a new one -- StartExecution succeeds because
	// closed executions allow reuse of the business ID.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Wait for the V1 workflow to complete (migration activity ran).
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

	// The new V2 schedule should be describable.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)
}

// TestScheduleMigrationV1ToV2NoDuplicateRecentActions verifies that migrating
// a V1 schedule with a running workflow to V2 does not produce duplicate entries
// in RecentActions. In V1, recordAction puts the same workflow in both
// RunningWorkflows and RecentActions. The migration must deduplicate these.
func TestScheduleMigrationV1ToV2NoDuplicateRecentActions(t *testing.T) {
	// Create the env without EnableChasm so that CreateSchedule does not write
	// a CHASM sentinel (which would block the migration activity).
	env := testcore.NewEnv(
		t,
		testcore.WithSdkWorker(),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-no-dup")
	wid := testcore.RandomizeStr("sched-migrate-no-dup-wf")
	wt := testcore.RandomizeStr("sched-migrate-no-dup-wt")

	nsName := env.Namespace().String()

	// Register a workflow that blocks until signaled, so it stays running
	// during migration.
	resumeSignal := "resume"
	workflowFn := func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, resumeSignal)
		ch.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// Create a V1 schedule with an immediate trigger.
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the V1 scheduler to start the workflow and record it as running.
	var runningWfID string
	require.Eventually(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		a := descResp.Info.RecentActions[0]
		if a.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return false
		}
		runningWfID = a.GetStartWorkflowResult().GetWorkflowId()
		return true
	}, 15*time.Second, 500*time.Millisecond)

	// Enable CHASM now so the migration activity can create the V2 schedule.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)

	// Migrate from V1 to V2 while the workflow is still running.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Wait for the V1 scheduler workflow to complete (migration done).
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
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

	// Describe the V2 schedule and verify no duplicate RunIds in RecentActions.
	v2Desc, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     env.NamespaceID().String(),
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)

	recentActions := v2Desc.GetFrontendResponse().GetInfo().GetRecentActions()
	assertRecentActionsNoDuplicateRunIDs(t, recentActions)

	// The running workflow should appear exactly once.
	var count int
	for _, action := range recentActions {
		if strings.HasPrefix(action.GetStartWorkflowResult().GetWorkflowId(), wid) {
			count++
		}
	}
	require.Equal(t, 1, count, "running workflow should appear exactly once in RecentActions, got %d", count)

	// Clean up: signal the running workflow to complete.
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         nsName,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
		SignalName:        resumeSignal,
	})
	require.NoError(t, err)
}
