package historydlq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestSegmentMinimumPendingMessageID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	require.Equal(t, int64(-1), segment.GetMinMessageId())
	require.Equal(t, minMessageIDSearchAttribute.Value(-1), segment.SearchAttributes(ctx)[2])
	first, err := segment.Append(ctx, req)
	require.NoError(t, err)
	now = now.Add(-time.Hour)
	req.SourceTaskIdentity.TaskID++
	second, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Less(t, second.MessageRef.MessageID, first.MessageRef.MessageID)
	require.Equal(t, second.MessageRef.MessageID, segment.GetMinMessageId())
	require.Equal(t, minMessageIDSearchAttribute.Value(second.MessageRef.MessageID), segment.SearchAttributes(ctx)[2])
	require.NoError(t, segment.ResolveEntry(ctx, second.MessageRef, historydlqpb.RESOLUTION_PURGED))
	require.Equal(t, first.MessageRef.MessageID, segment.GetMinMessageId())
	require.NoError(t, segment.ResolveEntry(ctx, first.MessageRef, historydlqpb.RESOLUTION_PURGED))
	require.Equal(t, int64(-1), segment.GetMinMessageId())
	require.Equal(t, minMessageIDSearchAttribute.Value(-1), segment.SearchAttributes(ctx)[2])
}

func TestSegmentReadMessagesBoundedPage(t *testing.T) {
	t.Parallel()
	segment, ctx, ref, refs := metadataFixture(t)
	req := ReadSegmentMessagesRequest{Segment: ref, PageSize: 1, InclusiveMaxMessageID: refs[0].MessageID}
	page, err := segment.ReadSegmentMessages(ctx, req)
	require.NoError(t, err)
	require.Len(t, page.Messages, 1)
	require.Equal(t, refs[0], page.Messages[0].Ref)
	protorequire.ProtoEqual(t, testEnqueueRequest().Task, page.Messages[0].Task)
	require.NotEmpty(t, page.NextPageToken)
	require.NoError(t, segment.ResolveEntry(ctx, refs[0], historydlqpb.RESOLUTION_PURGED))
	protorequire.ProtoEqual(t, testEnqueueRequest().Task, page.Messages[0].Task)
	req.NextPageToken = page.NextPageToken
	second, err := segment.ReadSegmentMessages(ctx, req)
	require.NoError(t, err)
	require.Len(t, second.Messages, 1)
	require.Equal(t, refs[1], second.Messages[0].Ref)
	require.Empty(t, second.NextPageToken)
	req.NextPageToken = nil
	req.InclusiveMaxMessageID--
	empty, err := segment.ReadSegmentMessages(ctx, req)
	require.NoError(t, err)
	require.Equal(t, MessagePage{}, empty)
}

func TestSegmentReadMessagesReportsMissingPayload(t *testing.T) {
	t.Parallel()
	segment, ctx, ref, refs := metadataFixture(t)
	segment.Entries[refs[0].EntryKey].Get(ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
	_, err := segment.ReadSegmentMessages(ctx, ReadSegmentMessagesRequest{Segment: ref, PageSize: 10, InclusiveMaxMessageID: refs[0].MessageID})
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestSegmentMessagesSurviveResolveAfterRead(t *testing.T) {
	t.Parallel()
	h, ctx, _, _ := newHandlerTestEnvironment(t)
	rpc := testRPCClient{enqueue: h.Enqueue, readSegment: func(ctx context.Context, req *historydlqpb.ReadSegmentMessagesRequest) (*historydlqpb.ReadSegmentMessagesResponse, error) {
		response, err := h.ReadSegmentMessages(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, message := range response.Messages {
			_, err = h.Resolve(ctx, &historydlqpb.ResolveRequest{SystemNamespaceId: primitives.SystemNamespaceID, Ref: message.Ref, Resolution: historydlqpb.RESOLUTION_PURGED})
			if err != nil {
				return nil, err
			}
		}
		return response, nil
	}}
	client := NewClient(log.NewNoopLogger(), rpc)
	accepted, err := client.Enqueue(ctx, testEnqueueRequest())
	require.NoError(t, err)
	req := ReadSegmentMessagesRequest{Segment: SegmentRef{Queue: accepted.Queue, BusinessID: accepted.BusinessID, RunID: accepted.RunID}, PageSize: 10, InclusiveMaxMessageID: accepted.MessageID}
	page, err := client.ReadSegmentMessages(ctx, req)
	require.NoError(t, err)
	require.Len(t, page.Messages, 1)
	protorequire.ProtoEqual(t, testEnqueueRequest().Task, page.Messages[0].Task)
	page.Messages[0].Task = nil
	require.Equal(t, MessagePage{Messages: []StoredMessage{{Ref: accepted, SourceTaskIdentity: testEnqueueRequest().SourceTaskIdentity}}}, page)
	empty, err := client.ReadSegmentMessages(ctx, req)
	require.NoError(t, err)
	require.Equal(t, MessagePage{}, empty)
}

func TestSegmentMessagesRejectMalformedResponses(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"nil response", "wrong run", "missing task"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			wire := testMessageRefProto(t)
			ref, err := messageProtoToRef(wire)
			require.NoError(t, err)
			response := &historydlqpb.ReadSegmentMessagesResponse{Messages: []*historydlqpb.StoredMessage{{Ref: wire, SourceTaskIdentity: taskIdentityToProto(testEnqueueRequest().SourceTaskIdentity), Task: testEnqueueRequest().Task}}}
			segment := SegmentRef{Queue: ref.Queue, BusinessID: ref.BusinessID, RunID: ref.RunID}
			switch kind {
			case "nil response":
				response = nil
			case "wrong run":
				wire.RunId = "wrong"
			case "missing task":
				response.Messages[0].Task = nil
			default:
				t.Fatalf("unknown test case: %s", kind)
			}
			client := NewClient(log.NewNoopLogger(), testRPCClient{readSegment: func(context.Context, *historydlqpb.ReadSegmentMessagesRequest) (*historydlqpb.ReadSegmentMessagesResponse, error) {
				return response, nil
			}})
			_, err = client.ReadSegmentMessages(t.Context(), ReadSegmentMessagesRequest{Segment: segment, PageSize: 10, InclusiveMaxMessageID: ref.MessageID})
			require.Error(t, err)
		})
	}
}

func TestSegmentMinimumWithNumericTies(t *testing.T) {
	t.Parallel()
	segment, ctx, _, refs := metadataFixture(t)
	require.Equal(t, refs[0].MessageID, segment.GetMinMessageId())
	require.NoError(t, segment.ResolveEntry(ctx, refs[0], historydlqpb.RESOLUTION_PURGED))
	require.Equal(t, refs[1].MessageID, segment.GetMinMessageId())
	require.NoError(t, segment.ResolveEntry(ctx, refs[1], historydlqpb.RESOLUTION_PURGED))
	require.Equal(t, int64(-1), segment.GetMinMessageId())
}
