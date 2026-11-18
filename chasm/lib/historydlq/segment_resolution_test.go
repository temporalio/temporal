package historydlq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func resolutionFixture(t *testing.T) (*Segment, *chasm.MockMutableContext, []MessageRef, int64) {
	t.Helper()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	emptyBytes := segment.GetRetainedBytes()
	var refs []MessageRef
	for i, taskID := range []int64{7115, 7134, 7116} {
		if i == 2 {
			now = now.Add(time.Hour)
		}
		req.SourceTaskIdentity.TaskID = taskID
		result, err := segment.Append(ctx, req)
		require.NoError(t, err)
		refs = append(refs, result.MessageRef)
	}
	require.Equal(t, refs[0].MessageID, refs[1].MessageID)
	require.Less(t, refs[1].MessageID, refs[2].MessageID)
	return segment, ctx, refs, emptyBytes
}

func resolutionRequest(ref MessageRef, entryKeys ...string) ResolveEntriesRequest {
	return ResolveEntriesRequest{
		Segment:    SegmentRef{Queue: ref.Queue, BusinessID: ref.BusinessID, RunID: ref.RunID},
		EntryKeys:  entryKeys,
		Resolution: historydlqpb.RESOLUTION_PURGED,
	}
}

func TestResolveEntriesBatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		selected, remaining []int
		duplicate, absent   bool
	}{
		{name: "empty", remaining: []int{0, 1, 2}},
		{name: "absent", remaining: []int{0, 1, 2}, absent: true},
		{name: "nonminimum", selected: []int{2}, remaining: []int{0, 1}},
		{name: "one tied minimum", selected: []int{0}, remaining: []int{1, 2}},
		{name: "both tied minima", selected: []int{0, 1}, remaining: []int{2}},
		{name: "duplicates and absent", selected: []int{0, 1}, remaining: []int{2}, duplicate: true, absent: true},
		{name: "all", selected: []int{0, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, emptyBytes := resolutionFixture(t)
			request := resolutionRequest(refs[0])
			for _, index := range tc.selected {
				request.EntryKeys = append(request.EntryKeys, refs[index].EntryKey)
			}
			if tc.duplicate {
				request.EntryKeys = append(request.EntryKeys, request.EntryKeys...)
			}
			if tc.absent {
				request.EntryKeys = append(request.EntryKeys, "already-resolved")
			}
			expectedBytes := emptyBytes
			var expectedKeys []string
			expectedMinimum := int64(-1)
			for _, index := range tc.remaining {
				ref := refs[index]
				entry := segment.Entries[ref.EntryKey].Get(ctx)
				expectedBytes += int64(proto.Size(entry.EntryState)+len(ref.EntryKey)) + entry.GetPayloadSizeBytes() + segmentNodeReserve
				expectedKeys = append(expectedKeys, ref.EntryKey)
				if expectedMinimum < 0 || ref.MessageID < expectedMinimum {
					expectedMinimum = ref.MessageID
				}
			}
			for _, field := range segment.Entries {
				field.Get(ctx).Task = chasm.Field[*commonspb.HistoryTask]{}
			}
			require.NoError(t, segment.ResolveEntries(ctx, request))
			var actualKeys []string
			for key := range segment.Entries {
				actualKeys = append(actualKeys, key)
			}
			require.ElementsMatch(t, expectedKeys, actualKeys)
			require.Equal(t, expectedBytes, segment.GetRetainedBytes())
			require.Equal(t, expectedMinimum, segment.GetMinMessageId())
			require.Equal(t, minMessageIDSearchAttribute.Value(expectedMinimum), segment.SearchAttributes(ctx)[2])
			require.NoError(t, segment.ResolveEntries(ctx, request))
			require.Equal(t, expectedBytes, segment.GetRetainedBytes())
		})
	}
}

func TestResolveEntriesMinimum(t *testing.T) {
	t.Parallel()
	segment, ctx, refs, emptyBytes := resolutionFixture(t)
	require.NoError(t, segment.ResolveEntries(ctx, resolutionRequest(refs[0], refs[0].EntryKey, refs[1].EntryKey)))
	require.Equal(t, refs[2].MessageID, segment.GetMinMessageId())
	require.NoError(t, segment.ResolveEntries(ctx, resolutionRequest(refs[2], refs[2].EntryKey)))
	require.Equal(t, int64(-1), segment.GetMinMessageId())
	require.Equal(t, emptyBytes, segment.GetRetainedBytes())
}

func TestResolveEntriesInvalidBatchIsAtomic(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"empty key", "too many keys", "invalid reason", "wrong run", "wrong business", "wrong queue", "missing selected metadata", "missing remaining metadata"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, _ := resolutionFixture(t)
			request := resolutionRequest(refs[0], refs[0].EntryKey, refs[1].EntryKey)
			switch kind {
			case "empty key":
				request.EntryKeys = append(request.EntryKeys, "")
			case "too many keys":
				for len(request.EntryKeys) <= 1000 {
					request.EntryKeys = append(request.EntryKeys, refs[0].EntryKey)
				}
			case "invalid reason":
				request.Resolution = historydlqpb.RESOLUTION_UNSPECIFIED
			case "wrong run":
				request.Segment.RunID = "different"
			case "wrong business":
				request.Segment.BusinessID = "different"
			case "wrong queue":
				request.Segment.Queue.SourceCluster = "different"
			case "missing selected metadata":
				segment.Entries[refs[1].EntryKey] = chasm.Field[*Entry]{}
			case "missing remaining metadata":
				segment.Entries[refs[2].EntryKey] = chasm.Field[*Entry]{}
			default:
				t.Fatalf("unknown test case: %s", kind)
			}
			beforeState := proto.Clone(segment.SegmentState)
			beforeEntries := make(chasm.Map[string, *Entry], len(segment.Entries))
			beforeMetadata := make(map[string]*historydlqpb.EntryState, len(segment.Entries))
			for key, field := range segment.Entries {
				beforeEntries[key] = field
				if entry, ok := field.TryGet(ctx); ok {
					beforeMetadata[key] = proto.Clone(entry.EntryState).(*historydlqpb.EntryState)
				}
			}
			err := segment.ResolveEntries(ctx, request)
			switch kind {
			case "wrong run", "wrong business":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			case "wrong queue":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			case "missing selected metadata", "missing remaining metadata":
				require.ErrorAs(t, err, new(*serviceerror.DataLoss))
			default:
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			}
			protorequire.ProtoEqual(t, beforeState, segment.SegmentState)
			require.Equal(t, beforeEntries, segment.Entries)
			for key, state := range beforeMetadata {
				protorequire.ProtoEqual(t, state, segment.Entries[key].Get(ctx).EntryState)
			}
		})
	}
}

func TestResolveEntriesBatchLimit(t *testing.T) {
	t.Parallel()
	segment, ctx, refs, _ := resolutionFixture(t)
	request := resolutionRequest(refs[0])
	for range 1000 {
		request.EntryKeys = append(request.EntryKeys, "already-resolved")
	}
	require.NoError(t, segment.ResolveEntries(ctx, request))
	request.EntryKeys = append(request.EntryKeys, "one-more")
	require.ErrorAs(t, segment.ResolveEntries(ctx, request), new(*serviceerror.InvalidArgument))
}
