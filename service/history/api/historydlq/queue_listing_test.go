package historydlq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	dlq "go.temporal.io/server/chasm/lib/historydlq"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/consts"
	"go.uber.org/mock/gomock"
)

type queueLister struct {
	persistence.HistoryTaskQueueManager
	list func(context.Context, *persistence.ListQueuesRequest) (*persistence.ListQueuesResponse, error)
}

func (q queueLister) ListQueues(ctx context.Context, req *persistence.ListQueuesRequest) (*persistence.ListQueuesResponse, error) {
	return q.list(ctx, req)
}

type segmentDescriber struct {
	segmentClient
	describe func(context.Context, dlq.SegmentRef) (dlq.SegmentStats, error)
}

func (c segmentDescriber) DescribeSegment(ctx context.Context, ref dlq.SegmentRef) (dlq.SegmentStats, error) {
	return c.describe(ctx, ref)
}

func queueListingFixture(t *testing.T) (*Store, []*historyservice.ListQueuesResponse_QueueInfo) {
	t.Helper()
	mixed := dlq.QueueKey{CategoryID: 1, SourceCluster: "mixed_source", TargetCluster: "target"}
	chasmOnly := dlq.QueueKey{CategoryID: 1, SourceCluster: "chasm", TargetCluster: "target"}
	queueName := func(q dlq.QueueKey) string {
		return persistence.GetHistoryTaskQueueName(int(q.CategoryID), q.SourceCluster, q.TargetCluster)
	}
	category, err := payload.Encode(int64(1))
	require.NoError(t, err)
	var rows []*chasmspb.VisibilityExecutionInfo
	stats := make(map[dlq.SegmentRef]dlq.SegmentStats)
	for i, queue := range []dlq.QueueKey{mixed, mixed, chasmOnly} {
		id, err := dlq.SegmentBusinessID(queue, dlq.TaskIdentity{SourceShardID: int32(i + 1), TaskID: 1})
		require.NoError(t, err)
		ref := dlq.SegmentRef{Queue: queue, BusinessID: id, RunID: fmt.Sprint(i)}
		rows = append(rows, &chasmspb.VisibilityExecutionInfo{BusinessId: id, RunId: ref.RunID,
			ChasmSearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				"HistoryDLQCategory":      category,
				"HistoryDLQSourceCluster": payload.EncodeString(queue.SourceCluster),
			}}})
		stats[ref] = []dlq.SegmentStats{{PendingCount: 2, LastMessageID: 100}, {PendingCount: 3, LastMessageID: 90}, {PendingCount: 4, LastMessageID: 200}}[i]
	}
	vis := chasm.NewMockVisibilityManager(gomock.NewController(t))
	vis.EXPECT().ListExecutions(gomock.Any(), reflect.TypeFor[*dlq.Segment](), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ reflect.Type, req *chasm.ListExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, "HistoryDLQMinMessageID >= 0", req.Query)
			if len(req.NextPageToken) == 0 {
				return &visibilityservice.ListChasmExecutionsResponse{Executions: rows[:1], NextPageToken: []byte("visibility-page2")}, nil
			}
			require.Equal(t, []byte("visibility-page2"), req.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{Executions: append(rows[1:], rows[0])}, nil
		}).AnyTimes()
	legacyOnlyName := persistence.GetHistoryTaskQueueName(2, "legacy", "target")
	clusterMetadata := cluster.NewMockMetadata(gomock.NewController(t))
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("target").AnyTimes()
	store := &Store{
		clusterMetadata:   clusterMetadata,
		visibilityManager: vis,
		queueV2Manager: queueLister{list: func(_ context.Context, req *persistence.ListQueuesRequest) (*persistence.ListQueuesResponse, error) {
			require.Equal(t, persistence.QueueTypeHistoryDLQ, req.QueueType)
			if len(req.NextPageToken) == 0 {
				return &persistence.ListQueuesResponse{Queues: []persistence.QueueInfo{{QueueName: queueName(mixed), MessageCount: 5, LastMessageID: 110}}, NextPageToken: []byte("legacy-page2")}, nil
			}
			require.Equal(t, []byte("legacy-page2"), req.NextPageToken)
			return &persistence.ListQueuesResponse{Queues: []persistence.QueueInfo{{QueueName: legacyOnlyName, MessageCount: 7, LastMessageID: 6}}}, nil
		}},
		chasmClient: segmentDescriber{describe: func(_ context.Context, ref dlq.SegmentRef) (dlq.SegmentStats, error) {
			stat, ok := stats[ref]
			if !ok {
				return dlq.SegmentStats{}, fmt.Errorf("unexpected segment reference: %v", ref)
			}
			return stat, nil
		}},
	}
	want := []*historyservice.ListQueuesResponse_QueueInfo{
		{QueueName: queueName(chasmOnly), MessageCount: 4, LastMessageId: 200},
		{QueueName: queueName(mixed), MessageCount: 10, LastMessageId: 110},
		{QueueName: legacyOnlyName, MessageCount: 7, LastMessageId: 6},
	}
	slices.SortFunc(want, func(a, b *historyservice.ListQueuesResponse_QueueInfo) int {
		return strings.Compare(a.QueueName, b.QueueName)
	})
	return store, want
}

func TestMixedStoreQueueList(t *testing.T) {
	t.Parallel()
	store, want := queueListingFixture(t)
	request := &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), IncludeChasm: true, PageSize: 1}
	var got []*historyservice.ListQueuesResponse_QueueInfo
	for i := 0; ; i++ {
		require.Less(t, i, 4)
		response, err := store.ListQueues(t.Context(), request)
		require.NoError(t, err)
		require.Len(t, response.Queues, 1)
		got = append(got, response.Queues...)
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}
	protorequire.ProtoSliceEqual(t, want, got)
}

func TestQueueListUnchanged(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		queueType persistence.QueueV2Type
		include   bool
	}{
		{"legacy caller", persistence.QueueTypeHistoryDLQ, false},
		{"non-DLQ", persistence.QueueTypeHistoryNormal, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &Store{queueV2Manager: queueLister{list: func(_ context.Context, req *persistence.ListQueuesRequest) (*persistence.ListQueuesResponse, error) {
				require.Equal(t, &persistence.ListQueuesRequest{QueueType: tc.queueType, PageSize: 7, NextPageToken: []byte("original")}, req)
				return &persistence.ListQueuesResponse{Queues: []persistence.QueueInfo{{QueueName: "original-queue", MessageCount: 8, LastMessageID: 9}}, NextPageToken: []byte("next")}, nil
			}}}
			got, err := store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(tc.queueType), PageSize: 7, NextPageToken: []byte("original"), IncludeChasm: tc.include})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &historyservice.ListQueuesResponse{Queues: []*historyservice.ListQueuesResponse_QueueInfo{{QueueName: "original-queue", MessageCount: 8, LastMessageId: 9}}, NextPageToken: []byte("next")}, got)
		})
	}
}

func TestSegmentReadFailureIsNotZero(t *testing.T) {
	t.Parallel()
	store, _ := queueListingFixture(t)
	expected := errors.New("segment unavailable")
	store.chasmClient = segmentDescriber{describe: func(context.Context, dlq.SegmentRef) (dlq.SegmentStats, error) { return dlq.SegmentStats{}, expected }}
	got, err := store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 100, IncludeChasm: true})
	require.ErrorIs(t, err, expected)
	require.Nil(t, got)
}

func TestQueueListCursorScope(t *testing.T) {
	t.Parallel()
	store, _ := queueListingFixture(t)
	req := &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 1, IncludeChasm: true}
	first, err := store.ListQueues(t.Context(), req)
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		queueType persistence.QueueV2Type
		include   bool
		token     []byte
	}{
		{"backend mode", persistence.QueueTypeHistoryDLQ, false, first.NextPageToken},
		{"queue type", persistence.QueueTypeHistoryNormal, true, first.NextPageToken},
		{"malformed", persistence.QueueTypeHistoryDLQ, true, []byte("bad-token")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(tc.queueType), PageSize: 1, IncludeChasm: tc.include, NextPageToken: tc.token})
			require.ErrorIs(t, err, consts.ErrInvalidPaginationToken)
		})
	}
	_, err = store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), IncludeChasm: true})
	require.ErrorIs(t, err, consts.ErrInvalidPageSize)
}

func TestCombinedQueueListForMetrics(t *testing.T) {
	t.Parallel()
	store, want := queueListingFixture(t)
	page, err := store.ListQueuesForMetrics(t.Context(), &persistence.ListQueuesRequest{QueueType: persistence.QueueTypeHistoryDLQ, PageSize: 100})
	require.NoError(t, err)
	expected := &persistence.ListQueuesResponse{}
	for _, queue := range want {
		expected.Queues = append(expected.Queues, persistence.QueueInfo{QueueName: queue.QueueName, MessageCount: queue.MessageCount, LastMessageID: queue.LastMessageId})
	}
	require.Equal(t, expected, page)
}

func TestQueueListDescribesOnlyReturnedQueues(t *testing.T) {
	t.Parallel()
	store, want := queueListingFixture(t)
	var calls atomic.Int32
	store.chasmClient = segmentDescriber{describe: func(_ context.Context, ref dlq.SegmentRef) (dlq.SegmentStats, error) {
		calls.Add(1)
		if ref.Queue.SourceCluster != "chasm" {
			return dlq.SegmentStats{}, fmt.Errorf("read a queue outside the response page: %s", ref.Queue.SourceCluster)
		}
		return dlq.SegmentStats{PendingCount: 4, LastMessageID: 200}, nil
	}}
	page, err := store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 1, IncludeChasm: true})
	require.NoError(t, err)
	protorequire.ProtoSliceEqual(t, want[:1], page.Queues)
	require.Equal(t, int32(1), calls.Load())
}

func TestQueueListEmptySegments(t *testing.T) {
	t.Parallel()
	store, want := queueListingFixture(t)
	store.chasmClient = segmentDescriber{describe: func(context.Context, dlq.SegmentRef) (dlq.SegmentStats, error) { return dlq.SegmentStats{}, nil }}
	want[0].MessageCount = 0
	want[0].LastMessageId = -1
	want[1].MessageCount = 5
	page, err := store.ListQueues(t.Context(), &historyservice.ListQueuesRequest{QueueType: int32(persistence.QueueTypeHistoryDLQ), PageSize: 100, IncludeChasm: true})
	require.NoError(t, err)
	protorequire.ProtoSliceEqual(t, want, page.Queues)
}
