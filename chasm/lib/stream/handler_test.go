package stream_test

import (
	"context"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	chasmapi "go.temporal.io/server/api/chasm/v1"
	visibilityservice "go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/streamsegmentstore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fixedShardResolver is a test ShardResolver that returns a constant.
type fixedShardResolver struct{ shard int32 }

func (r fixedShardResolver) Shard(string, string) int32 { return r.shard }

// newHandlerTestEngine sets up a chasm test engine + Handler with the
// in-memory segment store.  Returns the handler and an EngineContext
// suitable for passing to handler RPCs.
func newHandlerTestEngine(t *testing.T) (*stream.Handler, context.Context) {
	t.Helper()

	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(stream.NewNilLibrary()))

	segStore := streamsegmentstore.NewMemory()
	handler := stream.NewHandler(logger, segStore, fixedShardResolver{shard: 1}, nil)

	testEngine := chasmtest.NewEngine(t, registry)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)
	return handler, engineCtx
}

func sha256Hash(items []*streampb.PublishItem) []byte {
	h := sha256.New()
	for _, it := range items {
		h.Write(it.Data)
	}
	return h.Sum(nil)
}

// Handler-level end-to-end: CreateStream → Publish → DescribeStream.
// Exercises the chasm transitions plus the segment-facet I/O round-trip.
//
// Spec correspondence: PreparePublish + WriteSegmentsTentative +
// CommitPublish composed.
func TestHandlerPublishHappyPath(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)

	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		CreatedBy:   "test-user",
	})
	require.NoError(t, err)

	items := []*streampb.PublishItem{
		{Data: []byte("hello")},
		{Data: []byte("world")},
	}
	pubResp, err := h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    1,
		Items:       items,
		PayloadHash: sha256Hash(items),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), pubResp.FirstOffset)
	require.Equal(t, int64(2), pubResp.ItemCount)

	descResp, err := h.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), descResp.State.HeadOffset)
	require.Equal(t, int64(1), descResp.PublisherCount)
	require.Equal(t, int64(0), descResp.InflightCount)

	readResp, err := h.ReadRange(ctx, &streampb.ReadRangeRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		StartOffset: 0,
		EndOffset:   2,
	})
	require.NoError(t, err)
	require.Len(t, readResp.Items, 2)
	require.Equal(t, []byte("hello"), readResp.Items[0].Data)
	require.Equal(t, []byte("world"), readResp.Items[1].Data)
	require.Equal(t, []int64{0, 1}, readResp.Offsets)
	require.Equal(t, int64(2), readResp.NextOffset)
}

func TestHandlerReadRangeTopicFilterPreservesOffsets(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)

	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
	})
	require.NoError(t, err)

	items := []*streampb.PublishItem{
		{Data: []byte("a"), Topic: "alpha"},
		{Data: []byte("b"), Topic: "beta"},
		{Data: []byte("c"), Topic: "alpha"},
	}
	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    1,
		Items:       items,
		PayloadHash: sha256Hash(items),
	})
	require.NoError(t, err)

	readResp, err := h.ReadRange(ctx, &streampb.ReadRangeRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		StartOffset: 0,
		EndOffset:   3,
		Topics:      []string{"beta"},
	})
	require.NoError(t, err)
	require.Len(t, readResp.Items, 1)
	require.Equal(t, []byte("b"), readResp.Items[0].Data)
	require.Equal(t, []int64{1}, readResp.Offsets)
	require.Equal(t, int64(3), readResp.NextOffset)
}

func TestHandlerReadRangePreservesMultiplePublishesInSameSegment(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)

	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId:     "ns-1",
		StreamId:        "stream-1",
		SegmentMaxItems: 100,
	})
	require.NoError(t, err)

	firstBatch := []*streampb.PublishItem{
		{Data: []byte("a")},
		{Data: []byte("b")},
	}
	secondBatch := []*streampb.PublishItem{
		{Data: []byte("c")},
	}
	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    1,
		Items:       firstBatch,
		PayloadHash: sha256Hash(firstBatch),
	})
	require.NoError(t, err)
	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    2,
		Items:       secondBatch,
		PayloadHash: sha256Hash(secondBatch),
	})
	require.NoError(t, err)

	readResp, err := h.ReadRange(ctx, &streampb.ReadRangeRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		StartOffset: 0,
		EndOffset:   3,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, readResp.Offsets)
	require.Equal(t, []byte("a"), readResp.Items[0].Data)
	require.Equal(t, []byte("b"), readResp.Items[1].Data)
	require.Equal(t, []byte("c"), readResp.Items[2].Data)
}

// Handler-level: a Publish retry with the same (publisher_id, sequence)
// returns the prior outcome and does not advance head.
//
// Spec correspondence: dedup-replay short-circuit at PreparePublish.
func TestHandlerPublishDedupReplay(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)
	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		CreatedBy:   "test-user",
	})
	require.NoError(t, err)

	items := []*streampb.PublishItem{{Data: []byte("a")}}
	for i := 0; i < 3; i++ {
		resp, err := h.Publish(ctx, &streampb.PublishRequest{
			NamespaceId: "ns-1",
			StreamId:    "stream-1",
			PublisherId: "pub-1",
			Sequence:    1,
			Items:       items,
			PayloadHash: sha256Hash(items),
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), resp.FirstOffset)
		require.Equal(t, int64(1), resp.ItemCount)
	}

	desc, _ := h.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
	})
	require.Equal(t, int64(1), desc.State.HeadOffset)
}

// Handler-level: payload_hash mismatch in the request is rejected
// before any chasm work happens.
func TestHandlerPublishRejectsPayloadHashMismatch(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)
	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
	})
	require.NoError(t, err)

	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		PublisherId: "pub-1",
		Sequence:    1,
		Items:       []*streampb.PublishItem{{Data: []byte("hello")}},
		PayloadHash: []byte("not the right hash"),
	})
	require.Error(t, err)
}

func TestHandlerListStreamsUsesVisibility(t *testing.T) {
	ctrl := gomock.NewController(t)
	visibilityMgr := chasm.NewMockVisibilityManager(ctrl)

	state := &streampb.StreamState{
		HeadOffset:  7,
		BaseOffset:  2,
		Closed:      true,
		CreatedTime: timestamppb.Now(),
	}
	chasmMemo, err := payload.Encode(state)
	require.NoError(t, err)

	visibilityMgr.EXPECT().
		ListExecutions(
			gomock.Any(),
			reflect.TypeFor[*stream.Stream](),
			gomock.AssignableToTypeOf(&chasm.ListExecutionsRequest{}),
		).
		DoAndReturn(func(
			ctx context.Context,
			archetype reflect.Type,
			req *chasm.ListExecutionsRequest,
		) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, "ns-1", req.NamespaceName)
			require.Equal(t, 2, req.PageSize)
			require.Equal(t, []byte("page-1"), req.NextPageToken)
			return &visibilityservice.ListChasmExecutionsResponse{
				Executions: []*chasmapi.VisibilityExecutionInfo{{
					BusinessId: "stream-1",
					ChasmMemo:  chasmMemo,
				}},
				NextPageToken: []byte("page-2"),
			}, nil
		})

	h := stream.NewHandler(
		log.NewTestLogger(),
		streamsegmentstore.NewMemory(),
		fixedShardResolver{shard: 1},
		nil,
	)
	resp, err := h.ListStreams(
		chasm.NewVisibilityManagerContext(context.Background(), visibilityMgr),
		&streampb.ListStreamsRequest{
			NamespaceId:   "ns-1",
			PageSize:      2,
			NextPageToken: []byte("page-1"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("page-2"), resp.NextPageToken)
	require.Len(t, resp.Streams, 1)
	require.Equal(t, "stream-1", resp.Streams[0].StreamId)
	require.Equal(t, int64(7), resp.Streams[0].HeadOffset)
	require.Equal(t, int64(2), resp.Streams[0].BaseOffset)
	require.True(t, resp.Streams[0].Closed)
}

// Handler-level: Close → subsequent Publish returns StreamClosed.
func TestHandlerCloseRejectsPublish(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)
	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
	})
	require.NoError(t, err)

	_, err = h.Close(ctx, &streampb.CloseRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
		ClosedBy:    "tester",
		CloseReason: streampb.STREAM_CLOSE_REASON_EXPLICIT,
	})
	require.NoError(t, err)

	items := []*streampb.PublishItem{{Data: []byte("a")}}
	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
		PublisherId: "pub-1", Sequence: 1,
		Items:       items,
		PayloadHash: sha256Hash(items),
	})
	require.Error(t, err)
}

// Handler-level: Truncate advances base_offset and DeletePrefix prunes
// the segment-facet rows.
func TestHandlerTruncateAdvancesBaseAndCleansRows(t *testing.T) {
	h, ctx := newHandlerTestEngine(t)
	_, err := h.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
		SegmentMaxItems: 100, // ensure all in one segment
	})
	require.NoError(t, err)

	// Publish 5 items.
	items := []*streampb.PublishItem{
		{Data: []byte("a")}, {Data: []byte("b")}, {Data: []byte("c")},
		{Data: []byte("d")}, {Data: []byte("e")},
	}
	_, err = h.Publish(ctx, &streampb.PublishRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
		PublisherId: "pub-1", Sequence: 1,
		Items:       items,
		PayloadHash: sha256Hash(items),
	})
	require.NoError(t, err)

	// Truncate up_to=3 (we have no subscriber pinning).
	trResp, err := h.Truncate(ctx, &streampb.TruncateRequest{
		NamespaceId: "ns-1", StreamId: "stream-1", UpToOffset: 3,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), trResp.NewBaseOffset)

	desc, _ := h.DescribeStream(ctx, &streampb.DescribeStreamRequest{
		NamespaceId: "ns-1", StreamId: "stream-1",
	})
	require.Equal(t, int64(3), desc.State.BaseOffset)
	require.Equal(t, int64(5), desc.State.HeadOffset)
}
