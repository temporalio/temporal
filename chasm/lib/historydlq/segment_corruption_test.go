package historydlq

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

type corruptionTestContext struct {
	*chasm.MockMutableContext
	logger log.Logger
}

func (c corruptionTestContext) Logger() log.Logger { return c.logger }

func TestSegmentCorruptMetadataPagination(t *testing.T) {
	t.Parallel()
	segment, ctx, refs, _ := resolutionFixture(t)
	segmentRef := resolutionRequest(refs[0]).Segment
	segment.Entries[refs[2].EntryKey].Get(ctx).SourceTaskIdentity.TaskId++
	beforeState := proto.Clone(segment.SegmentState)
	corruptState := proto.Clone(segment.Entries[refs[2].EntryKey].Get(ctx).EntryState)
	wantRefs := slices.Clone(refs[:2])
	slices.SortFunc(wantRefs, func(a, b MessageRef) int {
		return compareEntryTuple(a.MessageID, a.EntryKey, b.MessageID, b.EntryKey)
	})
	core, logs := observer.New(zap.ErrorLevel)
	observedCtx := corruptionTestContext{MockMutableContext: ctx, logger: log.NewZapLogger(zap.New(core))}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	ctx.HandleMetricsHandler = func() metrics.Handler { return handler }
	req := ListEntriesRequest{Segment: segmentRef, InclusiveMaxMessageID: math.MaxInt64, PageSize: 1}
	for i := range 2 {
		page, err := segment.ListEntries(observedCtx, req)
		require.NoError(t, err)
		require.Equal(t, wantRefs[i:i+1], page.Refs)
		require.Equal(t, []string{refs[2].EntryKey}, page.CorruptEntryKeys)
		require.Equal(t, i == 0, len(page.NextPageToken) > 0)
		req.NextPageToken = page.NextPageToken
	}
	stats, err := segment.DescribeSegment(observedCtx, segmentRef)
	require.NoError(t, err)
	require.Equal(t, SegmentStats{
		PendingCount: 3, LastMessageID: refs[0].MessageID, CorruptEntryKeys: []string{refs[2].EntryKey},
	}, stats)
	require.Len(t, logs.All(), 3)
	for _, entry := range logs.All() {
		require.Equal(t, refs[2].EntryKey, entry.ContextMap()["entry-key"])
		require.Equal(t, refs[2].MessageID, entry.ContextMap()["dlq-message-id"])
		require.EqualValues(t, segmentRef.Queue.CategoryID, entry.ContextMap()["queue-task-category-id"])
		require.Equal(t, segmentRef.Queue.SourceCluster, entry.ContextMap()["xdc-source-cluster"])
		require.Equal(t, segmentRef.Queue.TargetCluster, entry.ContextMap()["xdc-target-cluster"])
		require.NotEmpty(t, entry.ContextMap()["operation"])
		require.NotEmpty(t, entry.ContextMap()["error"])
	}
	recordings := capture.SnapshotMetric(metrics.HistoryDLQCorruptEntry.Name())
	require.Len(t, recordings, 3)
	for _, recording := range recordings {
		require.Equal(t, int64(1), recording.Value)
		require.Empty(t, recording.Tags)
	}
	protorequire.ProtoEqual(t, beforeState, segment.SegmentState)
	protorequire.ProtoEqual(t, corruptState, segment.Entries[refs[2].EntryKey].Get(ctx).EntryState)
}

func TestResolveEntriesMergeRetainsCorruptMetadata(t *testing.T) {
	t.Parallel()
	for _, corruption := range []string{"missing child", "missing state", "missing timestamp", "identity mismatch", "invalid message ID"} {
		t.Run(corruption, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, _ := resolutionFixture(t)
			corruptKey := refs[2].EntryKey
			entry := segment.Entries[corruptKey].Get(ctx)
			expectedMinimum := int64(0)
			switch corruption {
			case "missing child":
				segment.Entries[corruptKey] = chasm.Field[*Entry]{}
			case "missing state":
				entry.EntryState = nil
			case "missing timestamp":
				entry.EnqueuedAt = nil
				expectedMinimum = refs[2].MessageID
			case "identity mismatch":
				entry.SourceTaskIdentity.TaskId++
				expectedMinimum = refs[2].MessageID
			case "invalid message ID":
				entry.MessageId = -1
			default:
				t.Fatalf("unknown corruption: %s", corruption)
			}
			corruptField := segment.Entries[corruptKey]
			request := resolutionRequest(refs[0], refs[0].EntryKey, corruptKey, refs[1].EntryKey, corruptKey, "absent")
			request.Resolution = historydlqpb.RESOLUTION_MERGED
			resolvedKeys := []string{refs[0].EntryKey, refs[1].EntryKey, "absent"}
			slices.Sort(resolvedKeys)
			want := ResolveEntriesResult{ResolvedEntryKeys: resolvedKeys, CorruptEntryKeys: []string{corruptKey}}
			result, err := segment.ResolveEntries(ctx, request)
			require.NoError(t, err)
			require.Equal(t, want, result)
			require.Equal(t, chasm.Map[string, *Entry]{corruptKey: corruptField}, segment.Entries)
			require.Equal(t, expectedMinimum, segment.GetMinMessageId())
			retainedBytes := segment.GetRetainedBytes()
			retry, err := segment.ResolveEntries(ctx, request)
			require.NoError(t, err)
			require.Equal(t, want, retry)
			require.Equal(t, retainedBytes, segment.GetRetainedBytes())
			err = segment.ResolveEntry(ctx, refs[2], historydlqpb.RESOLUTION_MERGED)
			require.ErrorAs(t, err, new(*serviceerror.DataLoss))
			require.ErrorContains(t, err, corruptKey)
			page, err := segment.ListEntries(ctx, ListEntriesRequest{
				Segment: request.Segment, InclusiveMaxMessageID: 0, PageSize: 1,
			})
			require.NoError(t, err)
			require.Equal(t, EntryPage{CorruptEntryKeys: []string{corruptKey}}, page)
		})
	}
}

func TestResolveEntriesPurgeDeletesCorruptMetadata(t *testing.T) {
	t.Parallel()
	for _, corruption := range []string{"missing child", "missing state", "missing timestamp", "identity mismatch", "invalid message ID"} {
		t.Run(corruption, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, emptyBytes := resolutionFixture(t)
			corruptKey := refs[2].EntryKey
			entry := segment.Entries[corruptKey].Get(ctx)
			originalState := proto.Clone(entry.EntryState).(*historydlqpb.EntryState)
			chargedBytes := entryRetainedBytes(corruptKey, originalState)
			// Entries without metadata reclaim a lower bound, so part of their charge stays retained.
			// Corrupt state that is still present is trusted for the reclaimed size.
			var unreclaimedBytes int64
			switch corruption {
			case "missing child":
				segment.Entries[corruptKey] = chasm.Field[*Entry]{}
				unreclaimedBytes = chargedBytes - entryRetainedBytes(corruptKey, nil)
			case "missing state":
				entry.EntryState = nil
				unreclaimedBytes = int64(proto.Size(originalState))
			case "missing timestamp":
				entry.EnqueuedAt = nil
				unreclaimedBytes = chargedBytes - entryRetainedBytes(corruptKey, entry.EntryState)
			case "identity mismatch":
				entry.SourceTaskIdentity.TaskId++
				unreclaimedBytes = chargedBytes - entryRetainedBytes(corruptKey, entry.EntryState)
			case "invalid message ID":
				entry.MessageId = -1
				unreclaimedBytes = chargedBytes - entryRetainedBytes(corruptKey, entry.EntryState)
			default:
				t.Fatalf("unknown corruption: %s", corruption)
			}
			core, logs := observer.New(zap.WarnLevel)
			observedCtx := corruptionTestContext{MockMutableContext: ctx, logger: log.NewZapLogger(zap.New(core))}
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			t.Cleanup(func() { handler.StopCapture(capture) })
			ctx.HandleMetricsHandler = func() metrics.Handler { return handler }
			request := resolutionRequest(refs[0], refs[0].EntryKey, corruptKey, refs[1].EntryKey, corruptKey, "absent")
			resolvedKeys := []string{refs[0].EntryKey, refs[1].EntryKey, corruptKey, "absent"}
			slices.Sort(resolvedKeys)
			want := ResolveEntriesResult{ResolvedEntryKeys: resolvedKeys}
			result, err := segment.ResolveEntries(observedCtx, request)
			require.NoError(t, err)
			require.Equal(t, want, result)
			require.Empty(t, segment.Entries)
			require.Equal(t, int64(-1), segment.GetMinMessageId())
			require.Equal(t, emptyBytes+unreclaimedBytes, segment.GetRetainedBytes())
			require.Len(t, logs.All(), 1)
			require.Equal(t, zap.WarnLevel, logs.All()[0].Level)
			require.Equal(t, "History DLQ message with invalid metadata was purged", logs.All()[0].Message)
			require.Equal(t, corruptKey, logs.All()[0].ContextMap()["entry-key"])
			require.Empty(t, capture.SnapshotMetric(metrics.HistoryDLQCorruptEntry.Name()))
			retry, err := segment.ResolveEntries(ctx, request)
			require.NoError(t, err)
			require.Equal(t, want, retry)
			require.Equal(t, emptyBytes+unreclaimedBytes, segment.GetRetainedBytes())
		})
	}
}

func TestResolveEntriesMinimumIncludesCorruptEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		corruptMessageID int64
		expectedMinimum  int64
	}{
		{name: "corrupt below healthy", corruptMessageID: 15, expectedMinimum: 15},
		{name: "corrupt above healthy", corruptMessageID: 30, expectedMinimum: 20},
		{name: "corrupt tied with healthy", corruptMessageID: 20, expectedMinimum: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, _ := resolutionFixture(t)
			segment.Entries[refs[0].EntryKey].Get(ctx).MessageId = 10
			segment.Entries[refs[1].EntryKey].Get(ctx).MessageId = 20
			corruptEntry := segment.Entries[refs[2].EntryKey].Get(ctx)
			corruptEntry.MessageId = tc.corruptMessageID
			corruptEntry.SourceTaskIdentity.TaskId++
			segment.MinMessageId = 10
			result, err := segment.ResolveEntries(ctx, resolutionRequest(refs[0], refs[0].EntryKey))
			require.NoError(t, err)
			require.Equal(t, ResolveEntriesResult{ResolvedEntryKeys: []string{refs[0].EntryKey}}, result)
			require.Equal(t, tc.expectedMinimum, segment.GetMinMessageId())
			require.Contains(t, segment.Entries, refs[1].EntryKey)
			require.Contains(t, segment.Entries, refs[2].EntryKey)
		})
	}
}

func TestResolveEntriesWithCorruptUnselectedEntry(t *testing.T) {
	t.Parallel()
	segment, ctx, refs, _ := resolutionFixture(t)
	segment.Entries[refs[2].EntryKey] = chasm.Field[*Entry]{}
	result, err := segment.ResolveEntries(ctx, resolutionRequest(refs[0], refs[0].EntryKey, refs[1].EntryKey))
	require.NoError(t, err)
	resolvedKeys := []string{refs[0].EntryKey, refs[1].EntryKey}
	slices.Sort(resolvedKeys)
	require.Equal(t, ResolveEntriesResult{ResolvedEntryKeys: resolvedKeys}, result)
	require.Equal(t, chasm.Map[string, *Entry]{refs[2].EntryKey: {}}, segment.Entries)
	require.Equal(t, int64(0), segment.GetMinMessageId())
}

func TestResolveEntriesLogsCorruptEntryOnce(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		corruptIndex int
		selected     bool
	}{
		{name: "selected and minimum recomputed", corruptIndex: 2, selected: true},
		{name: "unselected and minimum recomputed", corruptIndex: 2},
		{name: "selected without minimum recomputation", corruptIndex: 0, selected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			segment, ctx, refs, _ := resolutionFixture(t)
			corruptKey := refs[tc.corruptIndex].EntryKey
			segment.Entries[corruptKey] = chasm.Field[*Entry]{}
			request := resolutionRequest(refs[0])
			request.Resolution = historydlqpb.RESOLUTION_MERGED
			if tc.selected {
				request.EntryKeys = append(request.EntryKeys, corruptKey, corruptKey)
			}
			if tc.corruptIndex == 2 {
				request.EntryKeys = append(request.EntryKeys, refs[0].EntryKey, refs[1].EntryKey)
			}
			core, logs := observer.New(zap.ErrorLevel)
			observedCtx := corruptionTestContext{MockMutableContext: ctx, logger: log.NewZapLogger(zap.New(core))}
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			t.Cleanup(func() { handler.StopCapture(capture) })
			ctx.HandleMetricsHandler = func() metrics.Handler { return handler }
			_, err := segment.ResolveEntries(observedCtx, request)
			require.NoError(t, err)
			require.Len(t, logs.All(), 1)
			require.Equal(t, corruptKey, logs.All()[0].ContextMap()["entry-key"])
			require.NotContains(t, logs.All()[0].ContextMap(), "dlq-message-id")
			require.EqualValues(t, request.Segment.Queue.CategoryID, logs.All()[0].ContextMap()["queue-task-category-id"])
			require.Equal(t, request.Segment.Queue.SourceCluster, logs.All()[0].ContextMap()["xdc-source-cluster"])
			require.Equal(t, request.Segment.Queue.TargetCluster, logs.All()[0].ContextMap()["xdc-target-cluster"])
			recordings := capture.SnapshotMetric(metrics.HistoryDLQCorruptEntry.Name())
			require.Equal(t, []*metricstest.CapturedRecording{{
				Value: int64(1), Tags: map[string]string{},
			}}, recordings)
		})
	}
}

func TestSegmentCorruptPayloadPagination(t *testing.T) {
	t.Parallel()
	for _, nilPayload := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing payload", true: "nil payload"}[nilPayload], func(t *testing.T) {
			t.Parallel()
			segment, ctx, segmentRef, refs := metadataFixture(t)
			field := chasm.Field[*commonspb.HistoryTask]{}
			if nilPayload {
				field = chasm.NewDataField[*commonspb.HistoryTask](ctx, nil)
			}
			segment.Entries[refs[0].EntryKey].Get(ctx).Task = field
			core, logs := observer.New(zap.ErrorLevel)
			observedCtx := corruptionTestContext{MockMutableContext: ctx, logger: log.NewZapLogger(zap.New(core))}
			req := ReadSegmentMessagesRequest{Segment: segmentRef, InclusiveMaxMessageID: math.MaxInt64, PageSize: 1}
			first, err := segment.ReadSegmentMessages(observedCtx, req)
			require.NoError(t, err)
			require.Empty(t, first.Messages)
			require.Equal(t, []string{refs[0].EntryKey}, first.CorruptEntryKeys)
			require.NotEmpty(t, first.NextPageToken)
			req.NextPageToken = first.NextPageToken
			second, err := segment.ReadSegmentMessages(observedCtx, req)
			require.NoError(t, err)
			require.Len(t, second.Messages, 1)
			require.Equal(t, refs[1], second.Messages[0].Ref)
			protorequire.ProtoEqual(t, testEnqueueRequest().Task, second.Messages[0].Task)
			require.Empty(t, second.NextPageToken)
			require.Empty(t, second.CorruptEntryKeys)
			require.Len(t, segment.Entries, 2)
			require.Len(t, logs.All(), 1)
			require.Equal(t, refs[0].MessageID, logs.All()[0].ContextMap()["dlq-message-id"])
			require.Equal(t, "ReadSegmentMessages", logs.All()[0].ContextMap()["operation"])
		})
	}
}
