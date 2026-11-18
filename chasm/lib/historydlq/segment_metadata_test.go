package historydlq

import (
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
)

func metadataFixture(t *testing.T) (*Segment, *chasm.MockMutableContext, SegmentRef, []MessageRef) {
	t.Helper()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	var refs []MessageRef
	for _, id := range []int64{7115, 7134} {
		req.SourceTaskIdentity.TaskID = id
		result, err := segment.Append(ctx, req)
		require.NoError(t, err)
		refs = append(refs, result.MessageRef)
	}
	slices.SortFunc(refs, func(a, b MessageRef) int { return cmp.Compare(a.EntryKey, b.EntryKey) })
	return segment, ctx, SegmentRef{Queue: req.Queue, BusinessID: refs[0].BusinessID, RunID: refs[0].RunID}, refs
}

func TestSegmentListMetadataOnly(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"existing", "deleted", "empty", "corrupt child", "corrupt timestamp", "corrupt identity"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			segment, ctx, ref, refs := metadataFixture(t)
			for key, field := range segment.Entries {
				entry := field.Get(ctx)
				entry.Task = chasm.Field[*commonspb.HistoryTask]{}
				if key == refs[0].EntryKey {
					switch name {
					case "corrupt child":
						segment.Entries[key] = chasm.Field[*Entry]{}
					case "corrupt timestamp":
						entry.EnqueuedAt = nil
					case "corrupt identity":
						entry.SourceTaskIdentity.TaskId++
					case "existing", "deleted", "empty":
					default:
						t.Fatalf("unknown test case: %s", name)
					}
				}
			}
			switch name {
			case "deleted":
				require.NoError(t, segment.ResolveEntry(ctx, refs[0], historydlqpb.RESOLUTION_PURGED))
				refs = refs[1:]
			case "empty":
				clear(segment.Entries)
				refs = nil
			case "existing", "corrupt child", "corrupt timestamp", "corrupt identity":
			default:
				t.Fatalf("unknown test case: %s", name)
			}
			got, err := segment.ListEntries(ctx, ListEntriesRequest{Segment: ref, PageSize: 10, InclusiveMaxMessageID: math.MaxInt64})
			if name == "corrupt child" || name == "corrupt timestamp" || name == "corrupt identity" {
				require.ErrorAs(t, err, new(*serviceerror.DataLoss))
				return
			}
			require.NoError(t, err)
			require.Equal(t, EntryPage{Refs: refs}, got)
		})
	}
}

func TestSegmentSummary(t *testing.T) {
	t.Parallel()
	for _, remaining := range []int{0, 1, 2} {
		t.Run(string(rune('0'+remaining)), func(t *testing.T) {
			t.Parallel()
			segment, ctx, ref, refs := metadataFixture(t)
			for _, r := range refs[remaining:] {
				require.NoError(t, segment.ResolveEntry(ctx, r, historydlqpb.RESOLUTION_PURGED))
			}
			want := SegmentStats{PendingCount: int64(remaining)}
			if remaining > 0 {
				want.LastMessageID = refs[0].MessageID
			}
			got, err := segment.DescribeSegment(ctx, ref)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestSegmentPaginationTies(t *testing.T) {
	t.Parallel()
	segment, ctx, ref, refs := metadataFixture(t)
	require.Equal(t, refs[0].MessageID, refs[1].MessageID)
	req := ListEntriesRequest{Segment: ref, PageSize: 1, InclusiveMaxMessageID: refs[0].MessageID}
	first, err := segment.ListEntries(ctx, req)
	require.NoError(t, err)
	require.Equal(t, refs[:1], first.Refs)
	require.NotEmpty(t, first.NextPageToken)
	var cursor segmentCursor
	require.NoError(t, json.Unmarshal(first.NextPageToken, &cursor))
	require.Equal(t, segmentCursor{
		Version:   1,
		Segment:   ref,
		Bound:     req.InclusiveMaxMessageID,
		MessageID: first.Refs[0].MessageID,
		EntryKey:  first.Refs[0].EntryKey,
	}, cursor)
	req.NextPageToken = first.NextPageToken
	req.PageSize = 10
	second, err := segment.ListEntries(ctx, req)
	require.NoError(t, err)
	require.Equal(t, EntryPage{Refs: refs[1:]}, second)
	req.NextPageToken = nil
	req.InclusiveMaxMessageID--
	empty, err := segment.ListEntries(ctx, req)
	require.NoError(t, err)
	require.Equal(t, EntryPage{}, empty)
}

func TestSegmentListEntriesRejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*ListEntriesRequest)
	}{
		{"wrong queue", func(req *ListEntriesRequest) { req.Segment.Queue.SourceCluster = "other-source" }},
		{"wrong business ID", func(req *ListEntriesRequest) { req.Segment.BusinessID = "other-business" }},
		{"wrong run ID", func(req *ListEntriesRequest) { req.Segment.RunID = "other-run" }},
		{"missing business ID", func(req *ListEntriesRequest) { req.Segment.BusinessID = "" }},
		{"missing run ID", func(req *ListEntriesRequest) { req.Segment.RunID = "" }},
		{"negative bound", func(req *ListEntriesRequest) { req.InclusiveMaxMessageID = -1 }},
		{"zero page size", func(req *ListEntriesRequest) { req.PageSize = 0 }},
		{"negative page size", func(req *ListEntriesRequest) { req.PageSize = -1 }},
		{"malformed token", func(req *ListEntriesRequest) { req.NextPageToken = []byte("bad") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			segment, ctx, ref, _ := metadataFixture(t)
			req := ListEntriesRequest{Segment: ref, PageSize: 1, InclusiveMaxMessageID: math.MaxInt64}
			tc.change(&req)
			page, err := segment.ListEntries(ctx, req)
			switch tc.name {
			case "wrong queue":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			case "wrong business ID", "wrong run ID":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			}
			require.Equal(t, EntryPage{}, page)
		})
	}
}

func TestSegmentSummaryDropsDeletedMaximum(t *testing.T) {
	t.Parallel()
	segment, ctx, ref, refs := metadataFixture(t)
	entry := segment.Entries[refs[0].EntryKey].Get(ctx)
	entry.MessageId++
	refs[0].MessageID = entry.MessageId
	for _, field := range segment.Entries {
		field.Get(ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
	}
	stats, err := segment.DescribeSegment(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{PendingCount: 2, LastMessageID: refs[0].MessageID}, stats)
	require.NoError(t, segment.ResolveEntry(ctx, refs[0], historydlqpb.RESOLUTION_PURGED))
	stats, err = segment.DescribeSegment(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{PendingCount: 1, LastMessageID: refs[1].MessageID}, stats)
}

func TestSegmentListEntriesSortedAndDistinct(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		pageSize int
	}{
		{name: "single entry pages", pageSize: 1},
		{name: "ties across pages", pageSize: 2},
		{name: "one page", pageSize: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)

			var accepted []MessageRef
			baseTime := now
			for _, entry := range []struct {
				taskID     int64
				timeOffset time.Duration
			}{
				{taskID: 7115},
				{taskID: 7134},
				{taskID: 7116, timeOffset: time.Hour},
				{taskID: 7117, timeOffset: -time.Hour},
			} {
				now = baseTime.Add(entry.timeOffset)
				req.SourceTaskIdentity.TaskID = entry.taskID
				result, err := segment.Append(ctx, req)
				require.NoError(t, err)
				accepted = append(accepted, result.MessageRef)
			}
			require.Equal(t, accepted[0].MessageID, accepted[1].MessageID)
			require.Less(t, accepted[3].MessageID, accepted[0].MessageID)
			require.Greater(t, accepted[2].MessageID, accepted[0].MessageID)
			if accepted[0].EntryKey > accepted[1].EntryKey {
				accepted[0], accepted[1] = accepted[1], accepted[0]
			}
			want := []MessageRef{accepted[3], accepted[0], accepted[1], accepted[2]}

			duplicate, err := segment.Append(ctx, req)
			require.NoError(t, err)
			require.Equal(t, accepted[3], duplicate.MessageRef)

			request := ListEntriesRequest{
				Segment: SegmentRef{
					Queue:      req.Queue,
					BusinessID: accepted[0].BusinessID,
					RunID:      accepted[0].RunID,
				},
				InclusiveMaxMessageID: math.MaxInt64,
				PageSize:              tc.pageSize,
			}
			var got []MessageRef
			for range len(want) {
				page, err := segment.ListEntries(ctx, request)
				require.NoError(t, err)
				got = append(got, page.Refs...)
				if len(page.NextPageToken) == 0 {
					break
				}
				request.NextPageToken = page.NextPageToken
			}
			require.Equal(t, want, got)
		})
	}
}
