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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ScheduleMigrationTestSuite struct {
	parallelsuite.Suite[*ScheduleMigrationTestSuite]
}

func TestScheduleMigrationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleMigrationTestSuite{})
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV2AlreadyExists() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	s.NoError(err)

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
	s.ErrorAs(err, &alreadyExists)
	s.Contains(alreadyExists.Error(), sid)

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
	s.NoError(err)
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
	s.NoError(err)

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
	s.NoError(err)

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
	s.NoError(err)

	s.Eventually(func() bool {
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
	s.NoError(err)
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationDynamicConfig() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)
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
	s.NoError(err)

	// Wait for the per-namespace worker to pick up the V1 workflow.
	s.Eventually(func() bool {
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
	s.Eventually(func() bool {
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
	s.NoError(err)
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV1ToV2() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)
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
	s.NoError(err)

	// Wait for the per-namespace worker to pick up the V1 workflow.
	s.Eventually(func() bool {
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
	s.NoError(err)

	// Wait for V1 workflow to complete.
	s.Eventually(func() bool {
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
	s.NoError(err)
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV2ToV1() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

	// Describe the CHASM schedule before migration to capture its state.
	v2Desc, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	s.NoError(err)
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
	s.NoError(err)

	// Wait for the CHASM scheduler to be closed after migration.
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.Eventually(func() bool {
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
	s.Eventually(func() bool {
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
	s.Eventually(func() bool {
		v1Desc, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return err == nil
	}, 30*time.Second, 500*time.Millisecond)

	v1Schedule := v1Desc.GetSchedule()

	// Validate the schedule spec is preserved across migration.
	s.Len(v1Schedule.GetSpec().GetInterval(), len(v2Schedule.GetSpec().GetInterval()))
	s.Equal(
		v2Schedule.GetSpec().GetInterval()[0].GetInterval().AsDuration(),
		v1Schedule.GetSpec().GetInterval()[0].GetInterval().AsDuration(),
	)

	// Validate the action is preserved.
	v2Action := v2Schedule.GetAction().GetStartWorkflow()
	v1Action := v1Schedule.GetAction().GetStartWorkflow()
	s.Equal(v2Action.GetWorkflowId(), v1Action.GetWorkflowId())
	s.Equal(v2Action.GetWorkflowType().GetName(), v1Action.GetWorkflowType().GetName())
	s.Equal(v2Action.GetTaskQueue().GetName(), v1Action.GetTaskQueue().GetName())

	// Validate policies are preserved.
	s.Equal(
		v2Schedule.GetPolicies().GetOverlapPolicy(),
		v1Schedule.GetPolicies().GetOverlapPolicy(),
	)
	s.Equal(
		v2Schedule.GetPolicies().GetCatchupWindow().AsDuration(),
		v1Schedule.GetPolicies().GetCatchupWindow().AsDuration(),
	)

	// Validate the paused state is correctly restored (not the migration-imposed pause).
	s.Equal(v2Schedule.GetState().GetPaused(), v1Schedule.GetState().GetPaused())
	s.Equal(v2Schedule.GetState().GetNotes(), v1Schedule.GetState().GetNotes())

	// Validate the conflict token value is preserved across migration.
	// V2 (CHASM) serializes as LittleEndian, V1 (workflow) as BigEndian, so decode both to int64.
	s.Len(v2ConflictToken, 8)
	v2Token := int64(binary.LittleEndian.Uint64(v2ConflictToken))
	v1ConflictToken := v1Desc.GetConflictToken()
	s.Len(v1ConflictToken, 8)
	v1Token := int64(binary.BigEndian.Uint64(v1ConflictToken))
	s.Equal(v2Token, v1Token)

	// Validate ListSchedules returns exactly one entry once the V1 workflow
	// has written its visibility records (no duplicates from V1+V2).
	var listResp *workflowservice.ListSchedulesResponse
	s.Eventually(func() bool {
		listResp, err = env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       nsName,
			MaximumPageSize: 10,
		})
		return err == nil && len(listResp.GetSchedules()) == 1
	}, 30*time.Second, 500*time.Millisecond)
	s.Equal(sid, listResp.GetSchedules()[0].GetScheduleId())
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV2ToV1Idempotent() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

	// First migration call.
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// Second migration call should also succeed (idempotent).
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)
}

func (s *ScheduleMigrationTestSuite) TestCHASMScheduleDescribeAfterDisablingCreationAndMigration() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

	firstDescribe, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
	})
	s.NoError(err)
	s.NotNil(firstDescribe.GetSchedule())
	s.Eventually(func() bool {
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
	s.NoError(err)

	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, false)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, false)

	s.Eventually(func() bool {
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
func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV2ToV1RoutingFallback() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

	// Migrate from V2 (CHASM) to V1 (workflow).
	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_WORKFLOW,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// Wait for the CHASM scheduler to be closed after migration.
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.Eventually(func() bool {
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
	s.Eventually(func() bool {
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
	s.NoError(err)
	s.NotNil(descResp.GetSchedule())
	s.Equal(wt, descResp.GetSchedule().GetAction().GetStartWorkflow().GetWorkflowType().GetName())
	// The schedule was created unpaused; migration should preserve that state
	// (not the temporary migration-imposed pause).
	s.False(descResp.GetSchedule().GetState().GetPaused())

	// ListScheduleMatchingTimes should also fall through to V1.
	now := time.Now().UTC()
	matchResp, err := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		StartTime:  timestamppb.New(now),
		EndTime:    timestamppb.New(now.Add(5 * time.Hour)),
	})
	s.NoError(err)
	s.NotEmpty(matchResp.GetStartTime())

	// PatchSchedule (pause) should also fall through to V1.
	_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "pausing via routing fallback test",
		},
		Identity: "test",
	})
	s.NoError(err)

	// Verify the pause took effect on V1. The patch is delivered as a signal,
	// so the workflow needs time to process it.
	s.Eventually(func() bool {
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
	s.NoError(err)
}

func (s *ScheduleMigrationTestSuite) TestScheduleUpdateAfterDelete() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

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
	s.NoError(err)

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
	s.ErrorAs(err, &failedPreconditionErr)

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
	s.ErrorAs(err, &failedPreconditionErr)

	// Delete on an already-closed CHASM schedule returns ErrClosed.
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
	s.ErrorAs(err, &failedPreconditionErr)
}

func (s *ScheduleMigrationTestSuite) TestScheduleMigrationV1ToV2WithClosedV2() {
	env := testcore.NewEnv(
		s.T(),
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
	s.NoError(err)

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
	s.NoError(err)

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
	s.NoError(err)
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
	s.NoError(err)

	// Wait for the per-namespace worker to pick up the V1 workflow.
	s.Eventually(func() bool {
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
	s.NoError(err)

	// Wait for the V1 workflow to complete (migration activity ran).
	s.Eventually(func() bool {
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
	s.NoError(err)
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

	// Disable CHASM to create V1 schedule.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

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

// TestDeleteScheduleContextMetadata verifies that DeleteSchedule propagates the
// correct context metadata (workflow-type, workflow-task-queue) for every
// combination of CHASM and V1 state. This metadata is read by saas-temporal's
// metering interceptor for action attribution.
//
// We assert by reading gRPC response trailers: the frontend's
// ContextMetadataInterceptor is decorated to setTrailer=true for this test,
// so any context metadata set during the handler is emitted as trailers that
// the client can read directly.
func (s *ScheduleMigrationTestSuite) TestDeleteScheduleContextMetadata() {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, true),
		testcore.WithFxOptions(primitives.FrontendService,
			fx.Decorate(func(logger log.Logger) *interceptor.ContextMetadataInterceptor {
				return interceptor.NewContextMetadataInterceptor(true, logger)
			}),
		),
	)

	newSched := func() (sid, wt, tq string, sched *schedulepb.Schedule) {
		sid = testcore.RandomizeStr("sid")
		wt = testcore.RandomizeStr("wt")
		tq = testcore.RandomizeStr("tq")
		sched = &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   testcore.RandomizeStr("wid"),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		return
	}

	createCHASMSchedule := func(t *testing.T, sid string, sched *schedulepb.Schedule) {
		_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(
			testcore.NewContext(),
			&schedulerpb.CreateScheduleRequest{
				NamespaceId: env.NamespaceID().String(),
				FrontendRequest: &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: sid,
					Schedule:   sched,
					Identity:   "test",
					RequestId:  testcore.RandomizeStr("req"),
				},
			},
		)
		require.NoError(t, err)
	}

	createCHASMSentinel := func(t *testing.T, sid string) {
		_, err := env.GetTestCluster().SchedulerClient().CreateSentinel(
			testcore.NewContext(),
			&schedulerpb.CreateSentinelRequest{
				NamespaceId: env.NamespaceID().String(),
				Namespace:   env.Namespace().String(),
				ScheduleId:  sid,
			},
		)
		require.NoError(t, err)
	}

	createV1Scheduler := func(t *testing.T, sid string, sched *schedulepb.Schedule) {
		startArgs := &schedulespb.StartScheduleArgs{
			Schedule: sched,
			State: &schedulespb.InternalState{
				Namespace:     env.Namespace().String(),
				NamespaceId:   env.NamespaceID().String(),
				ScheduleId:    sid,
				ConflictToken: scheduler.InitialConflictToken,
			},
		}
		inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(startArgs)
		require.NoError(t, err)
		_, err = env.GetTestCluster().HistoryClient().StartWorkflowExecution(
			testcore.NewContext(),
			common.CreateHistoryStartWorkflowRequest(
				env.NamespaceID().String(),
				&workflowservice.StartWorkflowExecutionRequest{
					Namespace:                env.Namespace().String(),
					WorkflowId:               scheduler.WorkflowIDPrefix + sid,
					WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
					Input:                    inputPayloads,
					Identity:                 "test",
					RequestId:                testcore.RandomizeStr("req"),
					WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
					WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				},
				nil, nil, time.Now().UTC(),
			),
		)
		require.NoError(t, err)
	}

	createV1DummySentinel := func(t *testing.T, sid string) {
		_, err := env.GetTestCluster().HistoryClient().StartWorkflowExecution(
			testcore.NewContext(),
			common.CreateHistoryStartWorkflowRequest(
				env.NamespaceID().String(),
				&workflowservice.StartWorkflowExecutionRequest{
					Namespace:                env.Namespace().String(),
					WorkflowId:               scheduler.WorkflowIDPrefix + sid,
					WorkflowType:             &commonpb.WorkflowType{Name: dummy.DummyWFTypeName},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
					Identity:                 "test",
					RequestId:                testcore.RandomizeStr("req"),
					WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
					WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				},
				nil, nil, time.Now().UTC(),
			),
		)
		require.NoError(t, err)
	}

	deleteAndAssertMetadata := func(t *testing.T, sid, expectedWfType, expectedTQ string) {
		var trailer metadata.MD
		_, err := env.FrontendClient().DeleteSchedule(
			testcore.NewContext(),
			&workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Identity:   "test",
			},
			grpc.Trailer(&trailer),
		)
		require.NoError(t, err)
		require.Equal(t, []string{expectedWfType}, trailer.Get("workflow-type"),
			"workflow-type should match the owning stack's metadata")
		require.Equal(t, []string{expectedTQ}, trailer.Get("workflow-task-queue"),
			"workflow-task-queue should match the owning stack's metadata")
	}

	// Subtest: Both stacks have real entries. CHASM metadata wins.
	s.Run("BothStacks", func(s *ScheduleMigrationTestSuite) {
		sid, wt, tq, sched := newSched()
		createCHASMSchedule(s.T(), sid, sched)
		createV1Scheduler(s.T(), sid, sched)
		deleteAndAssertMetadata(s.T(), sid, wt, tq)
	})

	// Subtest: CHASM has real schedule, V1 has dummy sentinel. CHASM metadata wins.
	s.Run("CHASMOnly_V1Sentinel", func(s *ScheduleMigrationTestSuite) {
		sid, wt, tq, sched := newSched()
		createCHASMSchedule(s.T(), sid, sched)
		createV1DummySentinel(s.T(), sid)
		deleteAndAssertMetadata(s.T(), sid, wt, tq)
	})

	// Subtest: CHASM has sentinel, V1 has real scheduler. V1 metadata wins.
	s.Run("CHASMSentinel_V1Real", func(s *ScheduleMigrationTestSuite) {
		sid, _, _, sched := newSched()
		createCHASMSentinel(s.T(), sid)
		createV1Scheduler(s.T(), sid, sched)
		deleteAndAssertMetadata(s.T(), sid, scheduler.WorkflowType, primitives.PerNSWorkerTaskQueue)
	})

	// Subtest: No CHASM entry, V1 has real scheduler. V1 metadata wins.
	s.Run("V1Only_NoCHASM", func(s *ScheduleMigrationTestSuite) {
		sid, _, _, sched := newSched()
		createV1Scheduler(s.T(), sid, sched)
		deleteAndAssertMetadata(s.T(), sid, scheduler.WorkflowType, primitives.PerNSWorkerTaskQueue)
	})

	// Subtest: CHASM has sentinel, V1 has nothing. Delete returns error.
	// Metering skips error responses so metadata content is irrelevant.
	s.Run("CHASMSentinel_V1Gone", func(s *ScheduleMigrationTestSuite) {
		sid := testcore.RandomizeStr("sid")
		createCHASMSentinel(s.T(), sid)
		_, err := env.FrontendClient().DeleteSchedule(
			testcore.NewContext(),
			&workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Identity:   "test",
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.NotContains(notFoundErr.Message, "sentinel",
			"sentinel error should not leak to the client")
	})

	// Subtest: Neither stack has the schedule. Delete returns error.
	s.Run("NeitherStack", func(s *ScheduleMigrationTestSuite) {
		sid := testcore.RandomizeStr("nonexistent")
		_, err := env.FrontendClient().DeleteSchedule(
			testcore.NewContext(),
			&workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Identity:   "test",
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.NotContains(notFoundErr.Message, "sentinel",
			"sentinel error should not leak to the client")
	})
}

// TestPatchScheduleContextMetadata verifies that PatchSchedule propagates the
// correct context metadata for CHASM and V1 schedules.
func (s *ScheduleMigrationTestSuite) TestPatchScheduleContextMetadata() {
	env := testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerRouting, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, true),
		testcore.WithFxOptions(primitives.FrontendService,
			fx.Decorate(func(logger log.Logger) *interceptor.ContextMetadataInterceptor {
				return interceptor.NewContextMetadataInterceptor(true, logger)
			}),
		),
	)

	newSched := func() (sid, wt, tq string, sched *schedulepb.Schedule) {
		sid = testcore.RandomizeStr("sid")
		wt = testcore.RandomizeStr("wt")
		tq = testcore.RandomizeStr("tq")
		sched = &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   testcore.RandomizeStr("wid"),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		return
	}

	createCHASMSchedule := func(t *testing.T, sid string, sched *schedulepb.Schedule) {
		_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(
			testcore.NewContext(),
			&schedulerpb.CreateScheduleRequest{
				NamespaceId: env.NamespaceID().String(),
				FrontendRequest: &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: sid,
					Schedule:   sched,
					Identity:   "test",
					RequestId:  testcore.RandomizeStr("req"),
				},
			},
		)
		require.NoError(t, err)
	}

	createV1Scheduler := func(t *testing.T, sid string, sched *schedulepb.Schedule) {
		startArgs := &schedulespb.StartScheduleArgs{
			Schedule: sched,
			State: &schedulespb.InternalState{
				Namespace:     env.Namespace().String(),
				NamespaceId:   env.NamespaceID().String(),
				ScheduleId:    sid,
				ConflictToken: scheduler.InitialConflictToken,
			},
		}
		inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(startArgs)
		require.NoError(t, err)
		_, err = env.GetTestCluster().HistoryClient().StartWorkflowExecution(
			testcore.NewContext(),
			common.CreateHistoryStartWorkflowRequest(
				env.NamespaceID().String(),
				&workflowservice.StartWorkflowExecutionRequest{
					Namespace:                env.Namespace().String(),
					WorkflowId:               scheduler.WorkflowIDPrefix + sid,
					WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
					Input:                    inputPayloads,
					Identity:                 "test",
					RequestId:                testcore.RandomizeStr("req"),
					WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
					WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				},
				nil, nil, time.Now().UTC(),
			),
		)
		require.NoError(t, err)
	}

	patchAndAssertMetadata := func(t *testing.T, sid, expectedWfType, expectedTQ string) {
		var trailer metadata.MD
		_, err := env.FrontendClient().PatchSchedule(
			testcore.NewContext(),
			&workflowservice.PatchScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Patch:      &schedulepb.SchedulePatch{Pause: "test pause"},
				Identity:   "test",
				RequestId:  uuid.NewString(),
			},
			grpc.Trailer(&trailer),
		)
		require.NoError(t, err)
		require.Equal(t, []string{expectedWfType}, trailer.Get("workflow-type"),
			"workflow-type should match the owning stack's metadata")
		require.Equal(t, []string{expectedTQ}, trailer.Get("workflow-task-queue"),
			"workflow-task-queue should match the owning stack's metadata")
	}

	// CHASM schedule: metadata should reflect the schedule's action target.
	s.Run("CHASMSchedule", func(s *ScheduleMigrationTestSuite) {
		sid, wt, tq, sched := newSched()
		createCHASMSchedule(s.T(), sid, sched)
		patchAndAssertMetadata(s.T(), sid, wt, tq)
	})

	// V1 schedule: metadata should reflect the V1 scheduler workflow.
	s.Run("V1Schedule", func(s *ScheduleMigrationTestSuite) {
		sid, _, _, sched := newSched()
		createV1Scheduler(s.T(), sid, sched)
		patchAndAssertMetadata(s.T(), sid, scheduler.WorkflowType, primitives.PerNSWorkerTaskQueue)
	})

	// CHASM sentinel with no V1 workflow: patch should fail.
	s.Run("CHASMSentinel_V1Gone", func(s *ScheduleMigrationTestSuite) {
		sid := testcore.RandomizeStr("sid")
		_, err := env.GetTestCluster().SchedulerClient().CreateSentinel(
			testcore.NewContext(),
			&schedulerpb.CreateSentinelRequest{
				NamespaceId: env.NamespaceID().String(),
				Namespace:   env.Namespace().String(),
				ScheduleId:  sid,
			},
		)
		s.NoError(err)

		_, err = env.FrontendClient().PatchSchedule(
			testcore.NewContext(),
			&workflowservice.PatchScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Patch:      &schedulepb.SchedulePatch{Pause: "test"},
				Identity:   "test",
				RequestId:  uuid.NewString(),
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.NotContains(notFoundErr.Message, "sentinel",
			"sentinel error should not leak to the client")
	})
}

// TestScheduleMigration_StaleRunningDoesNotSkipPending guards the race fix in
// CreateSchedulerFromMigration. Without the fix, a "running" BufferedStart
// migrated from V1 (RunId set, Completed=nil, HasCallback=false) is treated as
// live by InvokerProcessBufferTask if it fires before SchedulerCallbacksTask
// has a chance to refresh it. With SCHEDULE_OVERLAP_POLICY_SKIP that causes a
// concurrently-pending start to be silently dropped (Info.OverlapSkipped++).
//
// The test sends a tailored CreateFromMigrationStateRequest directly to the
// CHASM scheduler API, bypassing V1 and the migration activity:
//   - One pending BufferedStart for a recent nominal time.
//   - One stale "running" BufferedStart whose underlying workflow has already
//     completed by the time the request is sent.
//
// SchedulerCallbacksTask must run first, observe the workflow as completed,
// stamp Completed, and only then fire ProcessBuffer (which then sees
// isRunning=false and does not apply SKIP).
func TestScheduleMigration_StaleRunningDoesNotSkipPending(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-stale-running")
	pendingWid := testcore.RandomizeStr("sched-stale-running-pending-wf")
	runningWid := testcore.RandomizeStr("sched-stale-running-running-wf")
	wt := testcore.RandomizeStr("sched-stale-running-wt")

	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()

	workflowFn := func(workflow.Context) error { return nil }
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// Start the workflow that will appear as a stale "running" entry in the
	// migration state, and wait for it to complete.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    nsName,
		WorkflowId:   runningWid,
		WorkflowType: &commonpb.WorkflowType{Name: wt},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:     "test",
		RequestId:    uuid.NewString(),
	})
	require.NoError(t, err)
	runningRunID := startResp.GetRunId()
	require.Eventually(t, func() bool {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: nsName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: runningWid, RunId: runningRunID},
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 10*time.Second, 200*time.Millisecond)

	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   pendingWid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: durationpb.New(time.Hour),
		},
	}

	// Two BufferedStarts:
	//   - Pending (Attempt=0, no RunId): SKIP-eligible if isRunning=true.
	//   - Running (Attempt=1, RunId set, Completed=nil, HasCallback=false):
	//     mirrors convertRunningWorkflowsToBufferedStarts, but the underlying
	//     workflow has already finished -- the stale state SchedulerCallbacksTask
	//     must refresh before ProcessBuffer runs.
	now := time.Now().UTC()
	pendingNominal := now.Add(-30 * time.Second)
	state := &schedulerpb.SchedulerMigrationState{
		SchedulerState: &schedulerpb.SchedulerState{
			Namespace:     nsName,
			NamespaceId:   nsID,
			ScheduleId:    sid,
			Schedule:      sched,
			Info:          &schedulepb.ScheduleInfo{},
			ConflictToken: scheduler.InitialConflictToken,
		},
		GeneratorState: &schedulerpb.GeneratorState{
			LastProcessedTime: timestamppb.New(now),
		},
		InvokerState: &schedulerpb.InvokerState{
			LastProcessedTime: timestamppb.New(now),
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime:   timestamppb.New(pendingNominal),
					ActualTime:    timestamppb.New(pendingNominal),
					RequestId:     "sched-migrated-pending-" + uuid.NewString(),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					WorkflowId:    pendingWid + "-" + pendingNominal.Format(time.RFC3339Nano),
					Attempt:       0,
				},
				{
					NominalTime:   timestamppb.New(now),
					ActualTime:    timestamppb.New(now),
					StartTime:     timestamppb.New(now),
					RequestId:     "sched-migrated-running-" + runningRunID,
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
					WorkflowId:    runningWid,
					RunId:         runningRunID,
					Attempt:       1,
					HasCallback:   false,
				},
			},
		},
	}

	_, err = env.GetTestCluster().SchedulerClient().CreateFromMigrationState(
		ctx,
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: nsID,
			State:       state,
		},
	)
	require.NoError(t, err)

	// Wait for both actions to surface in RecentActions:
	//   - The original running workflow, refreshed to Completed by
	//     SchedulerCallbacksTask.
	//   - The pending start, kicked off by ProcessBuffer once isRunning
	//     resolves to false, then run to completion by the SDK worker.
	var lastDesc *schedulerpb.DescribeScheduleResponse
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
			},
		)
		if err != nil {
			return false
		}
		lastDesc = desc
		return len(desc.GetFrontendResponse().GetInfo().GetRecentActions()) >= 2
	}, 30*time.Second, 500*time.Millisecond, "expected both running and pending starts to surface in RecentActions")

	// Load-bearing assertion: the pending start must NOT have been dropped
	// under SKIP overlap policy.
	require.Equal(t, int64(0), lastDesc.GetFrontendResponse().GetInfo().GetOverlapSkipped(),
		"stale running entry must not cause the pending start to be dropped under SKIP overlap policy")
}
