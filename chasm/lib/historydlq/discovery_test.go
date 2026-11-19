package historydlq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

func TestDiscoverSegmentsQueueScope(t *testing.T) {
	t.Parallel()
	queue := QueueKey{CategoryID: 1, SourceCluster: "source'\\cluster", TargetCluster: "target\"cluster"}
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*Segment](), gomock.Any()).DoAndReturn(func(_ context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
		require.Equal(t, primitives.SystemLocalNamespace, req.NamespaceName)
		require.Contains(t, req.Query, "HistoryDLQCategory = 1")
		require.Contains(t, req.Query, "HistoryDLQSourceCluster = "+sqlparser.String(sqlparser.NewStrVal([]byte(queue.SourceCluster))))

		require.NotContains(t, req.Query, "HistoryDLQTargetCluster")
		require.NotContains(t, req.Query, "HistoryDLQPending")
		return &visibilityservice.ListChasmExecutionsResponse{}, nil
	})
	got, err := DiscoverSegments(t.Context(), vis, queue, math.MaxInt64)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestDiscoverSegmentsAllPages(t *testing.T) {
	t.Parallel()
	queue := testRequest().Queue
	var rows []*chasmspb.VisibilityExecutionInfo
	var want []SegmentRef
	for _, shard := range []int32{7, 8, 9, 10} {
		id, err := SegmentBusinessID(queue, TaskIdentity{SourceShardID: shard, TaskID: 1})
		require.NoError(t, err)
		run := primitives.NewUUID().String()
		rows = append(rows, &chasmspb.VisibilityExecutionInfo{BusinessId: id, RunId: run})
		want = append(want, SegmentRef{Queue: queue, BusinessID: id, RunID: run})
	}
	// Include a duplicate row and a separate run of the same Business ID.
	rows = append(rows, rows[0], &chasmspb.VisibilityExecutionInfo{BusinessId: rows[0].BusinessId, RunId: primitives.NewUUID().String()})
	want = append(want, SegmentRef{Queue: queue, BusinessID: rows[len(rows)-1].BusinessId, RunID: rows[len(rows)-1].RunId})
	slices.SortFunc(want, func(a, b SegmentRef) int {
		if order := cmp.Compare(a.BusinessID, b.BusinessID); order != 0 {
			return order
		}
		return cmp.Compare(a.RunID, b.RunID)
	})
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	calls := 0
	vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*Segment](), gomock.Any()).DoAndReturn(func(_ context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
		calls++
		switch calls {
		case 1:
			require.Empty(t, req.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{Executions: rows[:3], NextPageToken: []byte("page2")}, nil
		case 2:
			require.Equal(t, []byte("page2"), req.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{NextPageToken: []byte("page3")}, nil
		default:
			require.Equal(t, []byte("page3"), req.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{Executions: rows[3:]}, nil
		}
	}).Times(3)
	got, err := DiscoverSegments(t.Context(), vis, queue, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestDelayedSegmentDiscovery(t *testing.T) {
	t.Parallel()
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	queue := testRequest().Queue
	id, err := SegmentBusinessID(queue, testRequest().SourceTaskIdentity)
	require.NoError(t, err)
	run := primitives.NewUUID().String()
	gomock.InOrder(
		vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{}, nil),
		vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{Executions: []*chasmspb.VisibilityExecutionInfo{{BusinessId: id, RunId: run}}}, nil),
	)
	first, err := DiscoverSegments(t.Context(), vis, queue, math.MaxInt64)
	require.NoError(t, err)
	require.Empty(t, first)
	second, err := DiscoverSegments(t.Context(), vis, queue, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, []SegmentRef{{Queue: queue, BusinessID: id, RunID: run}}, second)
}

func TestDiscoverSegmentsFailure(t *testing.T) {
	t.Parallel()
	for name, response := range map[string]*visibilityservice.ListChasmExecutionsResponse{"nil response": nil, "nil row": {Executions: []*chasmspb.VisibilityExecutionInfo{nil}}, "malformed ID": {Executions: []*chasmspb.VisibilityExecutionInfo{{BusinessId: "other", RunId: primitives.NewUUID().String()}}}, "missing run": {Executions: []*chasmspb.VisibilityExecutionInfo{{BusinessId: "historydlq:segment:v0:abc"}}}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
			vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(response, nil)
			_, err := DiscoverSegments(t.Context(), vis, testRequest().Queue, math.MaxInt64)
			require.Error(t, err)
		})
	}
	t.Run("visibility failure", func(t *testing.T) {
		t.Parallel()
		vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
		expected := errors.New("visibility failure")
		vis.EXPECT().ListExecutions(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, expected)
		_, err := DiscoverSegments(t.Context(), vis, testRequest().Queue, math.MaxInt64)
		require.ErrorIs(t, err, expected)
	})
}

func TestDiscoverSegmentsMinimumBound(t *testing.T) {
	t.Parallel()
	for _, bound := range []int64{0, 1000000} {
		t.Run(fmt.Sprint(bound), func(t *testing.T) {
			t.Parallel()
			vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
			vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*Segment](), gomock.Any()).DoAndReturn(func(_ context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
				require.Contains(t, req.Query, "HistoryDLQMinMessageID >= 0")
				require.Contains(t, req.Query, fmt.Sprintf("HistoryDLQMinMessageID <= %d", bound))
				require.NotContains(t, req.Query, "HistoryDLQPending")
				return &visibilityservice.ListChasmExecutionsResponse{}, nil
			})
			_, err := DiscoverSegments(t.Context(), vis, testRequest().Queue, bound)
			require.NoError(t, err)
		})
	}
}

func TestDiscoverAllSegmentsInvalidQueueAttributes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, field string
		value       any
	}{
		{"missing category", "HistoryDLQCategory", nil},
		{"category exceeds int32", "HistoryDLQCategory", int64(1 << 32)},
		{"invalid source type", "HistoryDLQSourceCluster", 42},
		{"empty source", "HistoryDLQSourceCluster", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id, err := SegmentBusinessID(testRequest().Queue, testRequest().SourceTaskIdentity)
			require.NoError(t, err)
			category, err := payload.Encode(int64(1))
			require.NoError(t, err)
			fields := map[string]*commonpb.Payload{
				"HistoryDLQCategory":      category,
				"HistoryDLQSourceCluster": payload.EncodeString("source"),
			}
			if tc.value == nil {
				delete(fields, tc.field)
			} else {
				encoded, err := payload.Encode(tc.value)
				require.NoError(t, err)
				fields[tc.field] = encoded
			}
			vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
			vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*Segment](), gomock.Any()).Return(
				&visibilityservice.ListChasmExecutionsResponse{Executions: []*chasmspb.VisibilityExecutionInfo{{
					BusinessId: id, RunId: "run", ChasmSearchAttributes: &commonpb.SearchAttributes{IndexedFields: fields},
				}}}, nil)
			refs, err := DiscoverAllSegments(t.Context(), vis, "target")
			require.ErrorAs(t, err, new(*serviceerror.DataLoss))
			require.Nil(t, refs)
		})
	}
}

func TestDiscoverAllSegmentsUsesLocalCluster(t *testing.T) {
	t.Parallel()
	queue := testRequest().Queue
	queue.TargetCluster = "local-target"
	id, err := SegmentBusinessID(queue, testRequest().SourceTaskIdentity)
	require.NoError(t, err)
	category, err := payload.Encode(int64(queue.CategoryID))
	require.NoError(t, err)
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*Segment](), gomock.Any()).Return(
		&visibilityservice.ListChasmExecutionsResponse{Executions: []*chasmspb.VisibilityExecutionInfo{{
			BusinessId: id,
			RunId:      "run",
			ChasmSearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"HistoryDLQCategory":      category,
				"HistoryDLQSourceCluster": payload.EncodeString(queue.SourceCluster),
			}},
		}}}, nil)
	refs, err := DiscoverAllSegments(t.Context(), vis, queue.TargetCluster)
	require.NoError(t, err)
	require.Equal(t, []SegmentRef{{Queue: queue, BusinessID: id, RunID: "run"}}, refs)
}

func TestDiscoverAllSegmentsRequiresLocalCluster(t *testing.T) {
	t.Parallel()
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	refs, err := DiscoverAllSegments(t.Context(), vis, "")
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.Nil(t, refs)
}
