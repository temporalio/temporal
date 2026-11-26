package tests

import (
	"context"
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
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestCreateScheduleCrossStackSameRequestID runs CreateSchedule against a real
// history service. A repeated request ID on one stack must not be accepted as a
// new schedule on the other stack.
func TestCreateScheduleCrossStackSameRequestID(t *testing.T) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	t.Run("V1ThenCHASM", func(t *testing.T) {
		ctx := testcontext.For(t)
		scheduleID := testcore.RandomizeStr("v1-then-chasm")
		requestID := testcore.RandomizeStr("request-id")

		_, err := env.FrontendClient().CreateSchedule(v1ContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.NoError(t, err)
		runID := scheduleWorkflowRunID(ctx, t, env, scheduleID)
		require.Equal(t, scheduler.WorkflowType, scheduleWorkflowType(ctx, t, env, scheduleID))

		_, err = env.FrontendClient().CreateSchedule(chasmContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.ErrorAs(t, err, new(*serviceerror.WorkflowExecutionAlreadyStarted))
		require.Equal(t, runID, scheduleWorkflowRunID(ctx, t, env, scheduleID))
		require.Equal(t, scheduler.WorkflowType, scheduleWorkflowType(ctx, t, env, scheduleID))

		_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
			chasmContextFactory(ctx),
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId: env.NamespaceID().String(),
				FrontendRequest: &workflowservice.DescribeScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
				},
			},
		)
		require.ErrorAs(t, err, new(*serviceerror.NotFound))
	})

	t.Run("CHASMThenV1", func(t *testing.T) {
		ctx := testcontext.For(t)
		scheduleID := testcore.RandomizeStr("chasm-then-v1")
		requestID := testcore.RandomizeStr("request-id")

		_, err := env.FrontendClient().CreateSchedule(chasmContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.NoError(t, err)
		runID := scheduleWorkflowRunID(ctx, t, env, scheduleID)
		require.Equal(t, dummy.DummyWFTypeName, scheduleWorkflowType(ctx, t, env, scheduleID))

		_, err = env.FrontendClient().CreateSchedule(v1ContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.ErrorAs(t, err, new(*serviceerror.WorkflowExecutionAlreadyStarted))
		require.Equal(t, runID, scheduleWorkflowRunID(ctx, t, env, scheduleID))
		require.Equal(t, dummy.DummyWFTypeName, scheduleWorkflowType(ctx, t, env, scheduleID))

		_, err = env.FrontendClient().DescribeSchedule(chasmContextFactory(ctx), &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		require.NoError(t, err)
	})

	t.Run("V1Retry", func(t *testing.T) {
		ctx := testcontext.For(t)
		scheduleID := testcore.RandomizeStr("v1-retry")
		requestID := testcore.RandomizeStr("request-id")

		_, err := env.FrontendClient().CreateSchedule(v1ContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.NoError(t, err)
		runID := scheduleWorkflowRunID(ctx, t, env, scheduleID)

		_, err = env.FrontendClient().CreateSchedule(v1ContextFactory(ctx), crossStackScheduleRequest(env, scheduleID, requestID))
		require.NoError(t, err)
		require.Equal(t, runID, scheduleWorkflowRunID(ctx, t, env, scheduleID))
		require.Equal(t, scheduler.WorkflowType, scheduleWorkflowType(ctx, t, env, scheduleID))
	})

	t.Run("DedupedSentinel", func(t *testing.T) {
		ctx := testcontext.For(t)
		scheduleID := testcore.RandomizeStr("deduped-sentinel")
		requestID := testcore.RandomizeStr("request-id")
		startV1Sentinel(ctx, t, env, scheduleID, requestID)
		runID := scheduleWorkflowRunID(ctx, t, env, scheduleID)

		_, err := env.FrontendClient().CreateSchedule(
			v1ContextFactory(ctx),
			crossStackScheduleRequest(env, scheduleID, requestID),
		)
		require.ErrorAs(t, err, new(*serviceerror.WorkflowExecutionAlreadyStarted))
		require.Equal(t, runID, scheduleWorkflowRunID(ctx, t, env, scheduleID))
		require.Equal(t, dummy.DummyWFTypeName, scheduleWorkflowType(ctx, t, env, scheduleID))
	})
}

func crossStackScheduleRequest(env *testcore.TestEnv, scheduleID, requestID string) *workflowservice.CreateScheduleRequest {
	return &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: scheduleID,
		RequestId:  requestID,
		Identity:   "test",
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   scheduleID + "-workflow",
						WorkflowType: &commonpb.WorkflowType{Name: "workflow-type"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			State: &schedulepb.ScheduleState{Paused: true},
		},
	}
}

func scheduleWorkflowExecution(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) *workflowpb.WorkflowExecutionInfo {
	t.Helper()
	resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + scheduleID},
	})
	require.NoError(t, err)
	return resp.GetWorkflowExecutionInfo()
}

func scheduleWorkflowType(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) string {
	t.Helper()
	return scheduleWorkflowExecution(ctx, t, env, scheduleID).GetType().GetName()
}

func scheduleWorkflowRunID(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) string {
	t.Helper()
	return scheduleWorkflowExecution(ctx, t, env, scheduleID).GetExecution().GetRunId()
}

func startV1Sentinel(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID, requestID string) {
	t.Helper()
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                env.Namespace().String(),
		WorkflowId:               scheduler.WorkflowIDPrefix + scheduleID,
		WorkflowType:             &commonpb.WorkflowType{Name: dummy.DummyWFTypeName},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Identity:                 "test",
		RequestId:                requestID,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: payload.EncodeString("TemporalSchedulerSentinel"),
			},
		},
	}
	_, err := env.GetTestCluster().HistoryClient().StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(env.NamespaceID().String(), startReq, nil, nil, time.Now().UTC()),
	)
	require.NoError(t, err)
}
