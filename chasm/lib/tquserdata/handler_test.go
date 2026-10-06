package tquserdata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func newTestHandler(t *testing.T) (*handler, context.Context, *tquserdatapb.UpsertTaskQueueUserDataRequest) {
	t.Helper()
	h := newHandler(metrics.NoopMetricsHandler)
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError))
	require.NoError(t, registry.Register(newLibrary(h)))
	ctx := chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry))
	return h, ctx, &tquserdatapb.UpsertTaskQueueUserDataRequest{
		NamespaceId: "namespace-id",
		TaskQueue:   "task-queue",
		UserData: &tquserdatapb.TaskQueueUserData{
			Clock: &clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 2},
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
		},
		Precondition: &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectMissing{
			ExpectMissing: true,
		},
	}
}

func readTestUserData(ctx context.Context, t *testing.T, h *handler, req *tquserdatapb.UpsertTaskQueueUserDataRequest) *tquserdatapb.GetTaskQueueUserDataSnapshotResponse {
	t.Helper()
	response, err := h.GetTaskQueueUserDataSnapshot(ctx, &tquserdatapb.GetTaskQueueUserDataSnapshotRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.NoError(t, err)
	return response
}

func TestUserDataVersionIncrementsOnCAS(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(1), response.Version)
	require.Equal(t, int64(1), readTestUserData(ctx, t, h, req).Version)

	for version := int64(1); version < 3; version++ {
		req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: version}
		response, err = h.UpsertTaskQueueUserData(ctx, req)
		require.NoError(t, err)
		require.Equal(t, version+1, response.Version)
		require.Equal(t, version+1, readTestUserData(ctx, t, h, req).Version)
	}
}

func TestUserDataCASConflictPreservesVersionAndData(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		condition func(*tquserdatapb.UpsertTaskQueueUserDataRequest)
	}{
		{"stale version", func(req *tquserdatapb.UpsertTaskQueueUserDataRequest) {
			req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 0}
		}},
		{"create existing", func(*tquserdatapb.UpsertTaskQueueUserDataRequest) {}},
		{"future version", func(req *tquserdatapb.UpsertTaskQueueUserDataRequest) {
			req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 2}
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h, ctx, req := newTestHandler(t)
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			req.UserData.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
			req.UserData.Clock.WallClock = 20
			testCase.condition(req)
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			response := readTestUserData(ctx, t, h, req)
			require.Equal(t, int64(1), response.Version)
			require.Equal(t, int64(10), response.UserData.Clock.WallClock)
			require.Equal(t, enumsspb.FAIRNESS_STATE_V1, response.UserData.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState)
		})
	}
}

func TestUserDataRoundTrip(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	expected := proto.Clone(req.UserData).(*tquserdatapb.TaskQueueUserData)
	encoded, err := proto.Marshal(req)
	require.NoError(t, err)
	decoded := &tquserdatapb.UpsertTaskQueueUserDataRequest{}
	require.NoError(t, proto.Unmarshal(encoded, decoded))
	_, err = h.UpsertTaskQueueUserData(ctx, decoded)
	require.NoError(t, err)

	stored := readTestUserData(ctx, t, h, req)
	require.True(t, proto.Equal(expected, stored.UserData))
	stored.UserData.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].DeploymentData.DeploymentsData["deployment"].RoutingConfig.RevisionNumber = 20
	require.True(t, proto.Equal(expected, readTestUserData(ctx, t, h, req).UserData))

	req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 1}
	req.UserData.PerType[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState = enumsspb.FAIRNESS_STATE_V2
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(2), response.Version)
	require.True(t, proto.Equal(req.UserData, readTestUserData(ctx, t, h, req).UserData))
}

func TestUserDataInvalidPrecondition(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	for _, condition := range []*tquserdatapb.UpsertTaskQueueUserDataRequest{
		{},
		{Precondition: &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectMissing{}},
		{Precondition: &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: -1}},
	} {
		req.Precondition = condition.Precondition
		_, err := h.UpsertTaskQueueUserData(ctx, req)
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		require.Equal(t, int64(1), readTestUserData(ctx, t, h, req).Version)
	}
}

func TestUserDataTerminationIsUnimplemented(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)

	_, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueueUserData](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(userData *TaskQueueUserData, mutableContext chasm.MutableContext, request chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
			return userData.Terminate(mutableContext, request)
		},
		chasm.TerminateComponentRequest{},
	)
	require.ErrorAs(t, err, new(*serviceerror.Unimplemented))
	stored := readTestUserData(ctx, t, h, req)
	require.Equal(t, int64(1), stored.Version)
	require.True(t, proto.Equal(req.UserData, stored.UserData))

	req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 1}
	response, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(2), response.Version)
}

func TestUserDataVersionCASPreservesIncomingClock(t *testing.T) {
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
			h, ctx, req := newTestHandler(t)
			_, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 1}
			req.UserData.Clock = testCase.clock
			response, err := h.UpsertTaskQueueUserData(ctx, req)
			require.NoError(t, err)
			require.Equal(t, int64(2), response.Version)
			stored := readTestUserData(ctx, t, h, req)
			require.Equal(t, int64(2), stored.Version)
			require.True(t, proto.Equal(testCase.clock, stored.UserData.Clock))
		})
	}
}

func TestUserDataUsesTaskQueueNameForExecutionKey(t *testing.T) {
	t.Parallel()
	h, ctx, original := newTestHandler(t)
	req := &tquserdatapb.UpsertTaskQueueUserDataRequest{
		NamespaceId:  original.NamespaceId,
		TaskQueue:    original.TaskQueue,
		UserData:     original.UserData,
		Precondition: original.Precondition,
	}
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)

	stored, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueueUserData](chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.TaskQueue}),
		func(userData *TaskQueueUserData, chasmContext chasm.Context, _ struct{}) (*tquserdatapb.TaskQueueUserData, error) {
			return userData.Data.Get(chasmContext), nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	require.True(t, proto.Equal(req.UserData, stored))

	response, err := h.GetTaskQueueUserDataSnapshot(ctx, &tquserdatapb.GetTaskQueueUserDataSnapshotRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), response.Version)
	require.True(t, proto.Equal(req.UserData, response.UserData))
}

func TestUserDataOperationsAreUnimplemented(t *testing.T) {
	t.Parallel()
	h, ctx, req := newTestHandler(t)
	_, err := h.UpsertTaskQueueUserData(ctx, req)
	require.NoError(t, err)
	var server tquserdatapb.TaskQueueUserDataServiceServer = h
	operations := []struct {
		name string
		call func(*testing.T) error
	}{
		{
			name: "GetTaskQueueUserData",
			call: func(t *testing.T) error {
				response, err := server.GetTaskQueueUserData(ctx, &tquserdatapb.GetTaskQueueUserDataRequest{
					NamespaceId:              req.NamespaceId,
					TaskQueue:                req.TaskQueue,
					TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
					LastKnownUserDataVersion: 1,
					WaitNewData:              true,
				})
				require.Nil(t, response)
				return err
			},
		},
		{
			name: "SyncDeploymentUserData",
			call: func(t *testing.T) error {
				response, err := server.SyncDeploymentUserData(ctx, &tquserdatapb.SyncDeploymentUserDataRequest{
					NamespaceId:         req.NamespaceId,
					TaskQueue:           req.TaskQueue,
					DeploymentName:      "deployment",
					TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
					UpdateRoutingConfig: &deploymentpb.RoutingConfig{RevisionNumber: 20},
				})
				require.Nil(t, response)
				return err
			},
		},
		{
			name: "UpdateTaskQueueConfig",
			call: func(t *testing.T) error {
				response, err := server.UpdateTaskQueueConfig(ctx, &tquserdatapb.UpdateTaskQueueConfigRequest{
					NamespaceId: req.NamespaceId,
					UpdateTaskqueueConfig: &workflowservice.UpdateTaskQueueConfigRequest{
						TaskQueue:     req.TaskQueue,
						TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
					},
				})
				require.Nil(t, response)
				return err
			},
		},
		{
			name: "UpdateFairnessState",
			call: func(t *testing.T) error {
				response, err := server.UpdateFairnessState(ctx, &tquserdatapb.UpdateFairnessStateRequest{
					NamespaceId:   req.NamespaceId,
					TaskQueue:     req.TaskQueue,
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
					FairnessState: enumsspb.FAIRNESS_STATE_V2,
				})
				require.Nil(t, response)
				return err
			},
		},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			require.Equal(t, codes.Unimplemented, status.Code(operation.call(t)))
			data := readTestUserData(ctx, t, h, req)
			require.Equal(t, int64(1), data.Version)
			require.True(t, proto.Equal(req.UserData, data.UserData))
		})
	}
}
