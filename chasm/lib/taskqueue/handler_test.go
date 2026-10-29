package taskqueue

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	chasmtaskqueuepb "go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/proto"
)

func newTestHandler(t *testing.T, engineOptions ...chasmtest.EngineOption) (*handler, context.Context, *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
	t.Helper()
	h := newHandler(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	require.NoError(t, registry.Register(newLibrary(h)))
	ctx := chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry, engineOptions...))
	return h, ctx, &chasmtaskqueuepb.UpsertTaskQueueUserDataRequest{
		NamespaceId: "namespace-id",
		TaskQueue:   "task-queue",
		Clock:       &clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 2},
		PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
			int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
				DeploymentData: &persistencespb.DeploymentData{
					DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
						"deployment": {
							RoutingConfig: &deploymentpb.RoutingConfig{
								CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"},
								RevisionNumber:           10,
							},
							Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
								"build": {RevisionNumber: 5},
							},
						},
					},
				},
				Config: &taskqueuepb.TaskQueueConfig{
					QueueRateLimit:          &taskqueuepb.RateLimitConfig{RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: 100}},
					FairnessWeightOverrides: map[string]float32{"priority": 2},
				},
				FairnessState: enumsspb.FAIRNESS_STATE_V1,
			},
			int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY): {
				Config: &taskqueuepb.TaskQueueConfig{
					FairnessKeysRateLimitDefault: &taskqueuepb.RateLimitConfig{RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: 50}},
				},
				FairnessState: enumsspb.FAIRNESS_STATE_V2,
			},
		},
	}
}

func readTestTaskQueueUserData(ctx context.Context, t *testing.T, h *handler, req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) *chasmtaskqueuepb.GetTaskQueueUserDataResponse {
	t.Helper()
	response, err := h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.NoError(t, err)
	return response
}

func readTestTaskQueueExecutionKey(ctx context.Context, t *testing.T, req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) chasm.ExecutionKey {
	t.Helper()
	key, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(_ *TaskQueue, chasmContext chasm.Context, _ struct{}) (chasm.ExecutionKey, error) {
			return chasmContext.ExecutionKey(), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	return key
}

func TestTaskQueueUserDataLogsErrorsExceptNotFound(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		prepare  func(*testing.T, *handler, context.Context, *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest)
		wantLogs int
	}{
		{name: "invalid request", prepare: func(_ *testing.T, _ *handler, _ context.Context, req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
			req.TaskQueue = ""
		}, wantLogs: 1},
		{name: "not found"},
		{name: "success", prepare: func(t *testing.T, h *handler, ctx context.Context, req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, req := newTestHandler(t)
			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			h.logger = logger
			if testCase.prepare != nil {
				testCase.prepare(t, h, ctx, req)
			}
			capture := logger.StartCapture()
			_, err := h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
				NamespaceId: req.NamespaceId,
				TaskQueue:   req.TaskQueue,
			})
			switch testCase.name {
			case "invalid request":
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			case "not found":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				require.NoError(t, err)
			}
			require.Len(t, capture.Snapshot(), testCase.wantLogs)
			if testCase.wantLogs > 0 {
				capture.RequireContains(t, testlogger.CapturedLogPattern{
					Level:   testlogger.Error,
					Message: "Failed to read task queue user data from CHASM",
					Tags: map[string]any{
						"error":              err.Error(),
						"wf-namespace-id":    req.NamespaceId,
						"wf-task-queue-name": req.TaskQueue,
					},
				})
			}
		})
	}
}

func TestTaskQueueUserDataZeroExpectedVersionCreates(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	req.ExpectedVersion = 0
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, response)
	stored := readTestTaskQueueUserData(ctx, t, h, req)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	}, stored)
}

func TestTaskQueueUserDataVersionIncrementsOnCAS(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, response)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	}, readTestTaskQueueUserData(ctx, t, h, req))

	for version := int64(1); version < 3; version++ {
		req.ExpectedVersion = version
		response, err = h.UpsertTaskQueueUserData(ctx, req)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: version + 1}, response)
		protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
			Version: version + 1,
			PerType: req.PerType,
			Clock:   req.Clock,
		}, readTestTaskQueueUserData(ctx, t, h, req))
	}
}

func TestTaskQueueUserDataCASConflictPreservesVersionAndData(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		condition func(*chasmtaskqueuepb.UpsertTaskQueueUserDataRequest)
	}{
		{"create existing", func(req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
			req.ExpectedVersion = 0
		}},
		{"stale version", func(req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
			req.ExpectedVersion = 1
		}},
		{"future version", func(req *chasmtaskqueuepb.UpsertTaskQueueUserDataRequest) {
			req.ExpectedVersion = 3
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, req := newTestHandler(t)
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			req.ExpectedVersion = 1
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			expected := common.CloneProto(&chasmtaskqueuepb.GetTaskQueueUserDataResponse{
				Version: 2,
				PerType: req.PerType,
				Clock:   req.Clock,
			})
			req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
			req.Clock.WallClock = 20
			testCase.condition(req)
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			response := readTestTaskQueueUserData(ctx, t, h, req)
			protorequire.ProtoEqual(t, expected, response)
		})
	}
}

func TestTaskQueueUserDataRoundTrip(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	expected := common.CloneProto(&chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	})
	encoded, err := proto.Marshal(req)
	require.NoError(t, err)
	decoded := &chasmtaskqueuepb.UpsertTaskQueueUserDataRequest{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))
	_, err = h.UpsertTaskQueueUserData(ctx, decoded)
	require.NoError(t, err)

	stored := readTestTaskQueueUserData(ctx, t, h, req)
	protorequire.ProtoEqual(t, expected, stored)
	stored.Clock.WallClock = 20
	stored.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"].RoutingConfig.RevisionNumber = 20
	protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))

	req.ExpectedVersion = 1
	req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
	delete(req.PerType, int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	req.PerType[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)] = &persistencespb.TaskQueueTypeUserData{FairnessState: enumsspb.FAIRNESS_STATE_V1}
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 2}, response)
	expected.Version = 2
	expected.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)] = common.CloneProto(req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)])
	expected.PerType[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)] = common.CloneProto(req.PerType[int32(enumspb.TASK_QUEUE_TYPE_NEXUS)])
	protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
}

func TestTaskQueueUserDataPartialUpdatePreservesNodes(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name            string
		includeActivity bool
		activityData    *persistencespb.TaskQueueTypeUserData
		serialize       bool
	}{
		{name: "omitted type"},
		{name: "empty type", includeActivity: true, activityData: &persistencespb.TaskQueueTypeUserData{}},
		{name: "nil type", includeActivity: true},
		{name: "nil type over protobuf", includeActivity: true, serialize: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var snapshots []chasm.NodesSnapshot
			h, ctx, req := newTestHandler(t, chasmtest.WithInvariantCheck(
				func(_ *testing.T, node *chasm.Node, _ chasm.RootComponent) {
					snapshot := node.Snapshot(nil)
					for path, value := range snapshot.Nodes {
						snapshot.Nodes[path] = common.CloneProto(value)
					}
					snapshots = append(snapshots, snapshot)
				},
			))
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			expected := readTestTaskQueueUserData(ctx, t, h, req)
			require.Len(t, snapshots, 1)
			require.Contains(t, snapshots[0].Nodes, "UserData")
			require.Contains(t, snapshots[0].Nodes, "UserData$PerType#1")
			require.Contains(t, snapshots[0].Nodes, "UserData$PerType#2")

			workflowData := common.CloneProto(req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)])
			workflowData.FairnessState = enumsspb.FAIRNESS_STATE_V2
			workflowData.Config = nil
			req.ExpectedVersion = 1
			req.PerType = map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): workflowData,
				int32(enumspb.TASK_QUEUE_TYPE_NEXUS):    testCase.activityData,
			}
			if testCase.includeActivity {
				req.PerType[int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY)] = testCase.activityData
			}
			if testCase.serialize {
				encoded, err := proto.Marshal(req)
				require.NoError(t, err)
				req = &chasmtaskqueuepb.UpsertTaskQueueUserDataRequest{}
				require.NoError(t, proto.Unmarshal(encoded, req))
			}
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)

			require.Len(t, snapshots, 2)
			require.Contains(t, snapshots[1].Nodes, "UserData$PerType#1")
			require.Contains(t, snapshots[1].Nodes, "UserData$PerType#2")
			require.NotContains(t, snapshots[1].Nodes, "UserData$PerType#3")
			protorequire.ProtoEqual(t, snapshots[0].Nodes["UserData$PerType#2"], snapshots[1].Nodes["UserData$PerType#2"])
			protorequire.ProtoEqual(t,
				snapshots[0].Nodes["UserData$PerType#1"].GetMetadata().GetInitialVersionedTransition(),
				snapshots[1].Nodes["UserData$PerType#1"].GetMetadata().GetInitialVersionedTransition(),
			)
			expected.Version = 2
			expected.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)] = common.CloneProto(workflowData)
			protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
			workflowData.FairnessState = enumsspb.FAIRNESS_STATE_V1
			protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
		})
	}
}

func TestTaskQueueUserDataEmptyPerTypeMapRejected(t *testing.T) {
	t.Parallel()
	for _, existing := range []bool{false, true} {
		for _, testCase := range []struct {
			name string
			data map[int32]*persistencespb.TaskQueueTypeUserData
		}{
			{name: "nil map"},
			{name: "empty map", data: map[int32]*persistencespb.TaskQueueTypeUserData{}},
		} {
			t.Run(fmt.Sprintf("%s/existing=%t", testCase.name, existing), func(t *testing.T) {
				t.Parallel()
				h, ctx, req := newTestHandler(t)
				var expected *chasmtaskqueuepb.GetTaskQueueUserDataResponse
				if existing {
					_, err := h.UpsertTaskQueueUserData(ctx, req)
					require.NoError(t, err)
					expected = readTestTaskQueueUserData(ctx, t, h, req)
					req.ExpectedVersion = 1
				}
				req.PerType = testCase.data
				req.Clock.WallClock = 20
				response, err := h.UpsertTaskQueueUserData(ctx, req)
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
				require.Nil(t, response)
				if existing {
					protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
				} else {
					_, err = h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
						NamespaceId: req.NamespaceId,
						TaskQueue:   req.TaskQueue,
					})
					require.ErrorAs(t, err, new(*serviceerror.NotFound))
					requireTestTaskQueueAbsent(ctx, t, req)
				}
			})
		}
	}
}

func TestTaskQueueUserDataNegativeExpectedVersion(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	req.ExpectedVersion = -1
	_, err = h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	}, readTestTaskQueueUserData(ctx, t, h, req))
}

func TestTaskQueueUserDataPositiveExpectedVersionDoesNotCreate(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	req.ExpectedVersion = 1
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	_, err = h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	requireTestTaskQueueAbsent(ctx, t, req)
}

func TestTaskQueueUserDataTerminationClosesTaskQueue(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)

	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(taskQueue *TaskQueue, mutableContext chasm.MutableContext, request chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
			return taskQueue.Terminate(mutableContext, request)
		},
		chasm.TerminateComponentRequest{},
	)
	require.NoError(t, err)
	lifecycleState, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(taskQueue *TaskQueue, chasmContext chasm.Context, _ struct{}) (chasm.LifecycleState, error) {
			return taskQueue.LifecycleState(chasmContext), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	require.Equal(t, chasm.LifecycleStateCompleted, lifecycleState)

	stored, err := h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.Nil(t, stored)
	req.ExpectedVersion = 1
	req.Clock.WallClock = 30
	_, err = h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	stored, err = h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.Nil(t, stored)

	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(taskQueue *TaskQueue, mutableContext chasm.MutableContext, request chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
			return taskQueue.Terminate(mutableContext, request)
		},
		chasm.TerminateComponentRequest{},
	)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))

	req.ExpectedVersion = 0
	req.Clock.WallClock = 20
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, response)
	stored = readTestTaskQueueUserData(ctx, t, h, req)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	}, stored)

	req.ExpectedVersion = 1
	req.Clock.WallClock = 30
	response, err = h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 2}, response)
}

func TestTaskQueueUserDataVersionCASPreservesIncomingClock(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name  string
		clock *clockspb.HybridLogicalClock
	}{
		{name: "nil"},
		{name: "equal", clock: &clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 2}},
		{name: "older", clock: &clockspb.HybridLogicalClock{WallClock: 5, Version: 3, ClusterId: 4}},
		{name: "newer", clock: &clockspb.HybridLogicalClock{WallClock: 20, Version: 6, ClusterId: 7}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			h, ctx, req := newTestHandler(t)
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			req.ExpectedVersion = 1
			req.Clock = testCase.clock
			response, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 2}, response)
			stored := readTestTaskQueueUserData(ctx, t, h, req)
			protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
				Version: 2,
				PerType: req.PerType,
				Clock:   req.Clock,
			}, stored)
		})
	}
}

func TestTaskQueueUserDataUsesTaskQueueNameForExecutionKey(t *testing.T) {
	t.Parallel()
	h, ctx, original := newTestHandler(t)
	req := &chasmtaskqueuepb.UpsertTaskQueueUserDataRequest{
		NamespaceId:     original.NamespaceId,
		TaskQueue:       original.TaskQueue,
		PerType:         original.PerType,
		Clock:           original.Clock,
		ExpectedVersion: original.ExpectedVersion,
	}
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)

	stored, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(taskQueue *TaskQueue, chasmContext chasm.Context, _ struct{}) (*chasmtaskqueuepb.GetTaskQueueUserDataResponse, error) {
			return taskQueue.UserData.Get(chasmContext).getTaskQueueUserData(chasmContext), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{Version: 1, PerType: req.PerType, Clock: req.Clock}, stored)

	response, err := h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
		Version: 1,
		PerType: req.PerType,
		Clock:   req.Clock,
	}, response)
}

func TestTaskQueueUserDataPersistedHierarchyRoundTrip(t *testing.T) {
	t.Parallel()
	var persisted *persistencespb.WorkflowMutableState
	h, ctx, req := newTestHandler(t, chasmtest.WithInvariantCheck(
		func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			require.IsType(t, &TaskQueue{}, root)
			chasmContext := chasm.NewContext(context.Background(), node)
			key := chasmContext.ExecutionKey()
			serializedRef, err := chasmContext.Ref(root)
			require.NoError(t, err)
			ref := &persistencespb.ChasmComponentRef{}
			require.NoError(t, proto.Unmarshal(serializedRef, ref))
			persisted = &persistencespb.WorkflowMutableState{
				ChasmNodes: node.Snapshot(nil).Nodes,
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
					NamespaceId:          key.NamespaceID,
					WorkflowId:           key.BusinessID,
					StateTransitionCount: ref.GetExecutionVersionedTransition().GetTransitionCount(),
					TransitionHistory:    []*persistencespb.VersionedTransition{ref.GetExecutionVersionedTransition()},
				},
				ExecutionState: &persistencespb.WorkflowExecutionState{
					RunId:  key.RunID,
					State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			}
		},
	))
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	require.NoError(t, registry.Register(NewNilLibrary()))
	readPersisted := func() (chasm.ExecutionKey, *chasmtaskqueuepb.GetTaskQueueUserDataResponse) {
		t.Helper()
		encoded, err := proto.Marshal(persisted)
		require.NoError(t, err)
		restored := &persistencespb.WorkflowMutableState{}
		require.NoError(t, proto.Unmarshal(encoded, restored))
		require.ElementsMatch(t, []string{"", "UserData", "UserData$PerType", "UserData$PerType#1", "UserData$PerType#2"}, slices.Collect(maps.Keys(restored.ChasmNodes)))
		require.Equal(t, ArchetypeID, restored.ChasmNodes[""].GetMetadata().GetComponentAttributes().GetTypeId())
		require.Equal(t, chasm.GenerateTypeID("taskQueue.userData"), restored.ChasmNodes["UserData"].GetMetadata().GetComponentAttributes().GetTypeId())
		taskQueue, chasmContext, err := chasm.NewDetachedExecution[*TaskQueue](context.Background(), restored, registry)
		require.NoError(t, err)
		require.Equal(t, chasm.LifecycleStateRunning, taskQueue.LifecycleState(chasmContext))
		userData := taskQueue.UserData.Get(chasmContext)
		require.Equal(t, chasm.LifecycleStateRunning, userData.LifecycleState(chasmContext))
		return chasmContext.ExecutionKey(), userData.getTaskQueueUserData(chasmContext)
	}

	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	key := readTestTaskQueueExecutionKey(ctx, t, req)
	restoredKey, restoredData := readPersisted()
	require.Equal(t, key, restoredKey)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{Version: 1, PerType: req.PerType, Clock: req.Clock}, restoredData)

	req.ExpectedVersion = restoredData.Version
	req.Clock.WallClock = 20
	req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 2}, response)
	restoredKey, restoredData = readPersisted()
	require.Equal(t, key, restoredKey)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{Version: 2, PerType: req.PerType, Clock: req.Clock}, restoredData)
	_, err = h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	protorequire.ProtoEqual(t, restoredData, readTestTaskQueueUserData(ctx, t, h, req))
}

func TestTaskQueueUserDataCreatesChildInExistingTaskQueue(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	key := chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}
	started, err := chasm.StartExecution(ctx, key,
		func(chasm.MutableContext, struct{}) (*TaskQueue, error) {
			return NewTaskQueue(), nil
		}, struct{}{})
	require.NoError(t, err)

	response, err := h.GetTaskQueueUserData(ctx, &chasmtaskqueuepb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.Nil(t, response)
	req.ExpectedVersion = 1
	_, err = h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))

	req.ExpectedVersion = 0
	created, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &chasmtaskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, created)
	require.Equal(t, started.ExecutionKey, readTestTaskQueueExecutionKey(ctx, t, req))
	expected := common.CloneProto(&chasmtaskqueuepb.GetTaskQueueUserDataResponse{Version: 1, PerType: req.PerType, Clock: req.Clock})
	protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))

	req.Clock.WallClock++
	req.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
	_, err = h.UpsertTaskQueueUserData(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	protorequire.ProtoEqual(t, expected, readTestTaskQueueUserData(ctx, t, h, req))
}

func TestTaskQueueUserDataIsolatedByNamespaceAndFamily(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	requests := []*chasmtaskqueuepb.UpsertTaskQueueUserDataRequest{
		common.CloneProto(req),
		common.CloneProto(req),
		common.CloneProto(req),
	}
	requests[1].NamespaceId = "another-namespace"
	requests[2].TaskQueue = "another-task-queue"
	for index, request := range requests {
		request.Clock.WallClock = int64(index + 10)
		_, err := h.UpsertTaskQueueUserData(ctx, request)
		require.NoError(t, err)
	}

	requests[0].ExpectedVersion = 1
	requests[0].Clock.WallClock = 20
	_, err := h.UpsertTaskQueueUserData(ctx, requests[0])
	require.NoError(t, err)
	for index, request := range requests {
		version := int64(1)
		if index == 0 {
			version = 2
		}
		protorequire.ProtoEqual(t, &chasmtaskqueuepb.GetTaskQueueUserDataResponse{
			Version: version,
			PerType: request.PerType,
			Clock:   request.Clock,
		}, readTestTaskQueueUserData(ctx, t, h, request))
		key := readTestTaskQueueExecutionKey(ctx, t, request)
		require.Equal(t, request.NamespaceId, key.NamespaceID)
		require.Equal(t, request.TaskQueue, key.BusinessID)
	}
}
