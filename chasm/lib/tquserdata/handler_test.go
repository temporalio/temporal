package tquserdata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
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
		BusinessId:  BusinessID("task-queue"),
		UserData:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 10}},
		Precondition: &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectMissing{
			ExpectMissing: true,
		},
	}
}

func readTestUserData(ctx context.Context, t *testing.T, h *handler, req *tquserdatapb.UpsertTaskQueueUserDataRequest) *tquserdatapb.GetTaskQueueUserDataResponse {
	t.Helper()
	response, err := h.GetTaskQueueUserData(ctx, &tquserdatapb.GetTaskQueueUserDataRequest{
		NamespaceId: req.NamespaceId,
		TaskQueue:   req.TaskQueue,
		BusinessId:  req.BusinessId,
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
			req.UserData.Clock.WallClock = 20
			testCase.condition(req)
			_, err = h.UpsertTaskQueueUserData(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			response := readTestUserData(ctx, t, h, req)
			require.Equal(t, int64(1), response.Version)
			require.Equal(t, int64(10), response.UserData.Clock.WallClock)
		})
	}
}

func TestUserDataVersionCASDoesNotCompareClocks(t *testing.T) {
	t.Parallel()
	for _, incomingClock := range []*clockspb.HybridLogicalClock{nil, {WallClock: 10}, {WallClock: 5}, {WallClock: 20}} {
		h, ctx, req := newTestHandler(t)
		_, err := h.UpsertTaskQueueUserData(ctx, req)
		require.NoError(t, err)
		req.Precondition = &tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion{ExpectedVersion: 1}
		req.UserData.Clock = incomingClock
		response, err := h.UpsertTaskQueueUserData(ctx, req)
		require.NoError(t, err)
		require.Equal(t, int64(2), response.Version)
		stored := readTestUserData(ctx, t, h, req)
		require.Equal(t, int64(2), stored.Version)
		require.True(t, proto.Equal(incomingClock, stored.UserData.Clock))
	}
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
