package tests

import (
	"context"
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
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestCreateSchedule_CrossPathRetry reproduces the Mixed brain failure on
// #13253. During a rolling upgrade, gRPC retries of one CreateSchedule (same
// RequestId) can alternate between a frontend on the V2 (CHASM) create path
// and one on the V1 (workflow) path. Each row plants the state an earlier
// attempt left behind when it died between phases, then retries through the
// other path. A retry must be idempotent: succeed, with exactly one real
// scheduler across both key spaces.
func TestCreateSchedule_CrossPathRetry(t *testing.T) {
	env := newScheduleEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true))
	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()

	newSchedule := func() *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
			State: &schedulepb.ScheduleState{Paused: true},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   testcore.RandomizeStr("wid"),
						WorkflowType: &commonpb.WorkflowType{Name: "wt"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: "tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	// Mirrors writeSchedulerCHASMSentinel (V1 path, phase 1).
	plantCHASMSentinel := func(ctx context.Context, t *testing.T, sid, reqID string) {
		_, err := env.GetTestCluster().SchedulerClient().CreateSentinel(ctx, &schedulerpb.CreateSentinelRequest{
			NamespaceId: nsID,
			Namespace:   nsName,
			ScheduleId:  sid,
			RequestId:   reqID,
		})
		require.NoError(t, err)
	}

	// Mirrors writeSchedulerWorkflowSentinel (V2 path, phase 1).
	plantV1KeySentinel := func(ctx context.Context, t *testing.T, sid, reqID string) {
		_, err := env.GetTestCluster().HistoryClient().StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(
			nsID,
			&workflowservice.StartWorkflowExecutionRequest{
				Namespace:                nsName,
				WorkflowId:               scheduler.WorkflowIDPrefix + sid,
				WorkflowType:             &commonpb.WorkflowType{Name: dummy.DummyWFTypeName},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				Identity:                 "test",
				RequestId:                chasmscheduler.SentinelRequestID(reqID),
				WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
				SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
					sadefs.TemporalNamespaceDivision: payload.EncodeString("TemporalSchedulerSentinel"),
				}},
			},
			nil, nil, time.Now().UTC(),
		))
		require.NoError(t, err)
	}

	// Mirrors createScheduleWorkflow's real start (V1 path, phase 2).
	plantV1Real := func(ctx context.Context, t *testing.T, sid, reqID string) {
		input, err := sdk.PreferProtoDataConverter.ToPayloads(&schedulespb.StartScheduleArgs{
			Schedule: newSchedule(),
			State: &schedulespb.InternalState{
				Namespace:     nsName,
				NamespaceId:   nsID,
				ScheduleId:    sid,
				ConflictToken: scheduler.InitialConflictToken,
			},
		})
		require.NoError(t, err)
		_, err = env.GetTestCluster().HistoryClient().StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(
			nsID,
			&workflowservice.StartWorkflowExecutionRequest{
				Namespace:                nsName,
				WorkflowId:               scheduler.WorkflowIDPrefix + sid,
				WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
				TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
				Input:                    input,
				Identity:                 "test",
				RequestId:                reqID,
				WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			},
			nil, nil, time.Now().UTC(),
		))
		require.NoError(t, err)
	}

	// Mirrors createScheduleCHASM's real create (V2 path, phase 2).
	plantV2Real := func(ctx context.Context, t *testing.T, sid, reqID string) {
		_, err := env.GetTestCluster().SchedulerClient().CreateSchedule(ctx, &schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
				Schedule:   newSchedule(),
				Identity:   "test",
				RequestId:  reqID,
			},
		})
		require.NoError(t, err)
	}

	isV1Real := func(ctx context.Context, t *testing.T, sid string) bool {
		resp, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: nsID,
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: nsName,
				Execution: &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid},
			},
		})
		if _, ok := err.(*serviceerror.NotFound); ok {
			return false
		}
		require.NoError(t, err)
		info := resp.GetWorkflowExecutionInfo()
		return info.GetType().GetName() == scheduler.WorkflowType &&
			info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}

	isV2Real := func(ctx context.Context, t *testing.T, sid string) bool {
		resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: nsName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: sid},
			Archetype: string(chasm.SchedulerArchetype),
		})
		if _, ok := err.(*serviceerror.NotFound); ok {
			return false
		}
		require.NoError(t, err)
		node := resp.GetDatabaseMutableState().GetChasmNodes()[""]
		require.NotNil(t, node)
		var state schedulerpb.SchedulerState
		require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &state))
		return !state.GetSentinel()
	}

	type plantFn func(ctx context.Context, t *testing.T, sid, reqID string)
	// Planted under a different request's ID: a genuinely concurrent creator.
	foreign := func(plant plantFn) plantFn {
		return func(ctx context.Context, t *testing.T, sid, _ string) { plant(ctx, t, sid, uuid.NewString()) }
	}

	type attempt struct {
		viaV2 bool
		// The V1 path defers with Unavailable when this request's CHASM-path
		// attempt owns the V1 key; the SDK retries it.
		wantUnavailable bool
	}
	v1Attempt, v2Attempt := attempt{viaV2: false}, attempt{viaV2: true}
	v1Deferred := attempt{viaV2: false, wantUnavailable: true}

	tests := []struct {
		name     string
		prior    []plantFn // state left by earlier attempts, in write order
		attempts []attempt
		// A different request holds the ID: the create must fail as a conflict
		// and must not displace the other request's reservation.
		wantConflict bool
	}{
		// Same-path retries.
		{
			name:     "V2 succeeded, response lost; retry V2",
			prior:    []plantFn{plantV1KeySentinel, plantV2Real},
			attempts: []attempt{v2Attempt},
		},
		{
			name:     "V1 succeeded, response lost; retry V1",
			prior:    []plantFn{plantCHASMSentinel, plantV1Real},
			attempts: []attempt{v1Attempt},
		},

		// Cross-path retries.
		{
			// The #13253 Mixed brain failure.
			name:     "V1 died after phase 1; retry V2",
			prior:    []plantFn{plantCHASMSentinel},
			attempts: []attempt{v2Attempt},
		},
		{
			name:     "V1 succeeded, response lost; retry V2",
			prior:    []plantFn{plantCHASMSentinel, plantV1Real},
			attempts: []attempt{v2Attempt},
		},
		{
			// Previously reported success with no real scheduler anywhere.
			name:     "V2 died after phase 1; retry V1 then V2",
			prior:    []plantFn{plantV1KeySentinel},
			attempts: []attempt{v1Deferred, v2Attempt},
		},
		{
			name:     "V2 succeeded, response lost; retry V1",
			prior:    []plantFn{plantV1KeySentinel, plantV2Real},
			attempts: []attempt{v1Attempt},
		},

		// Both earlier attempts died after phase 1. The V1 key arbitrates: it
		// holds the CHASM path's sentinel, so the CHASM path wins.
		{
			name:     "both died after phase 1; retry V2",
			prior:    []plantFn{plantV1KeySentinel, plantCHASMSentinel},
			attempts: []attempt{v2Attempt},
		},
		{
			name:     "both died after phase 1; retry V1 then V2",
			prior:    []plantFn{plantV1KeySentinel, plantCHASMSentinel},
			attempts: []attempt{v1Deferred, v2Attempt},
		},

		// Guards: a different request's reservations are still conflicts.
		{
			name:         "foreign CHASM sentinel; create V2",
			prior:        []plantFn{foreign(plantCHASMSentinel)},
			attempts:     []attempt{v2Attempt},
			wantConflict: true,
		},
		{
			name:         "foreign V1-key sentinel; create V1",
			prior:        []plantFn{foreign(plantV1KeySentinel)},
			attempts:     []attempt{v1Attempt},
			wantConflict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testcore.NewContext()
			sid := testcore.RandomizeStr("sched-cross-path-retry")
			reqID := uuid.NewString()

			for _, plant := range tt.prior {
				plant(ctx, t, sid, reqID)
			}

			for i, a := range tt.attempts {
				attemptCtx := ctx
				if a.viaV2 {
					attemptCtx = chasmContextFactory(ctx)
				}
				_, err := env.FrontendClient().CreateSchedule(attemptCtx, &workflowservice.CreateScheduleRequest{
					Namespace:  nsName,
					ScheduleId: sid,
					Schedule:   newSchedule(),
					Identity:   "test",
					RequestId:  reqID,
				})
				switch {
				case a.wantUnavailable:
					var unavailable *serviceerror.Unavailable
					require.ErrorAs(t, err, &unavailable, "attempt %d", i)
				case tt.wantConflict:
					var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
					require.ErrorAs(t, err, &alreadyStarted, "attempt %d", i)
				default:
					require.NoError(t, err, "attempt %d: retry of the same CreateSchedule request must be idempotent", i)
				}
			}

			v1, v2 := isV1Real(ctx, t, sid), isV2Real(ctx, t, sid)
			if tt.wantConflict {
				require.False(t, v1 || v2, "conflicting create must not create a scheduler; V1 real=%v, V2 real=%v", v1, v2)
				return
			}
			require.NotEqual(t, v1, v2,
				"expected exactly one real scheduler after a successful create; V1 real=%v, V2 real=%v", v1, v2)
		})
	}
}
