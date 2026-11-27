package historydlq

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSegmentNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		namespaceID string
	}{
		{"empty", ""},
		{"user namespace", "user-namespace"},
		{"namespace name instead of ID", "temporal-system"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			key := ctx.ExecutionKey()
			key.NamespaceID = tc.namespaceID
			ctx.HandleExecutionKey = func() chasm.ExecutionKey { return key }
			segment, err := NewSegment(ctx, testEnqueueRequest())
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Nil(t, segment)
		})
	}
}

func TestSegmentNamespaceAccess(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"append", "duplicate", "read", "resolve"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			refResult, err := segment.Append(ctx, req)
			ref := refResult.MessageRef
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			wrongCtx := newSegmentContext(&now)
			key := ctx.ExecutionKey()
			key.NamespaceID = "user-namespace"
			wrongCtx.HandleExecutionKey = func() chasm.ExecutionKey { return key }
			switch operation {
			case "append", "duplicate":
				if operation == "append" {
					req.SourceTaskIdentity.TaskID++
				}
				_, err = segment.Append(wrongCtx, req)
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			case "read":
				_, err = segment.ReadEntry(wrongCtx, ref)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			case "resolve":
				err = segment.ResolveEntry(wrongCtx, ref, historydlqpb.RESOLUTION_PURGED)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				t.Fatalf("unknown operation %q", operation)
			}
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Len(t, segment.Entries, 1)
			stored, err := segment.ReadEntry(ctx, ref)
			require.NoError(t, err)
			require.NotNil(t, stored.Task)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
		})
	}
}

func TestSegmentQueueIdentity(t *testing.T) {
	t.Parallel()
	req := testEnqueueRequest()
	segmentID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	retryID, err := SegmentBusinessID(req.Queue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, segmentID, retryID)

	changedOriginal := req
	changedOriginal.SourceTaskIdentity.TaskID++
	unchangedID, err := SegmentBusinessID(changedOriginal.Queue, changedOriginal.SourceTaskIdentity)
	require.NoError(t, err)
	require.Equal(t, segmentID, unchangedID)

	changedQueue := req.Queue
	changedQueue.TargetCluster = "other-target"
	otherID, err := SegmentBusinessID(changedQueue, req.SourceTaskIdentity)
	require.NoError(t, err)
	require.NotEqual(t, segmentID, otherID)

	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	require.Equal(t, req.Queue.CategoryID, segment.GetQueue().GetTaskCategory())
	require.Equal(t, req.Queue.SourceCluster, segment.GetQueue().GetSourceCluster())
	require.Equal(t, req.Queue.TargetCluster, segment.GetQueue().GetTargetCluster())
	require.Empty(t, segment.Entries)
	require.Equal(t, int32(7), segment.GetSourceShardId())
	require.Equal(t, []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(req.Queue.CategoryID)),
		sourceSearchAttribute.Value(req.Queue.SourceCluster),
		minMessageIDSearchAttribute.Value(-1),
	}, segment.SearchAttributes(ctx))
	require.IsType(t, &emptypb.Empty{}, segment.Memo(ctx))
}

func TestSegmentAppendDistinctEntries(t *testing.T) {
	t.Parallel()
	for _, encoding := range []enumspb.EncodingType{enumspb.ENCODING_TYPE_PROTO3, enumspb.ENCODING_TYPE_JSON} {
		t.Run(encoding.String(), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC)
			ctx := newSegmentContext(&now)
			first := testEnqueueRequest()
			first.Task.Blob.EncodingType = encoding
			segment, err := NewSegment(ctx, first)
			require.NoError(t, err)
			firstRefResult, err := segment.Append(ctx, first)
			firstRef := firstRefResult.MessageRef
			require.NoError(t, err)
			second := testEnqueueRequest()
			second.Task.Blob.EncodingType = encoding
			second.SourceTaskIdentity.TaskID++
			secondRefResult, err := segment.Append(ctx, second)
			secondRef := secondRefResult.MessageRef
			require.NoError(t, err)

			require.NotEqual(t, firstRef.EntryKey, secondRef.EntryKey)
			require.Equal(t, firstRef.BusinessID, secondRef.BusinessID)
			require.Equal(t, firstRef.RunID, secondRef.RunID)
			require.Equal(t, first.Queue, firstRef.Queue)
			require.Equal(t, second.Queue, secondRef.Queue)
			require.Len(t, segment.Entries, 2)
			require.Equal(t, minMessageIDSearchAttribute.Value(min(firstRef.MessageID, secondRef.MessageID)), segment.SearchAttributes(ctx)[2])

			stored, err := segment.ReadEntry(ctx, secondRef)
			require.NoError(t, err)
			require.Equal(t, secondRef, stored.Ref)
			require.NotNil(t, stored.Task)
			protorequire.ProtoEqual(t, second.Task, stored.Task)
			second.Task.Blob.Data[0] = 'x'
			stored.Task.Blob.Data[0] = 'y'
			again, err := segment.ReadEntry(ctx, secondRef)
			require.NoError(t, err)
			require.Equal(t, []byte("task payload"), again.Task.Blob.Data)
		})
	}
}

func TestSegmentAppendDuplicate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*EnqueueRequest, *time.Time)
	}{
		{"later time", func(_ *EnqueueRequest, now *time.Time) { *now = now.Add(time.Hour) }},
		{"clock before epoch", func(_ *EnqueueRequest, now *time.Time) {
			*now = time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
		}},
		{"different payload", func(req *EnqueueRequest, _ *time.Time) {
			req.Task.Blob.Data = []byte("different payload")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			first, err := segment.Append(ctx, req)
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			wantIdentity := req.SourceTaskIdentity
			wantTask := proto.Clone(req.Task)
			tc.change(&req, &now)
			duplicate, err := segment.Append(ctx, req)
			require.NoError(t, err)
			require.Equal(t, appendResult{MessageRef: first.MessageRef}, duplicate)
			require.Len(t, segment.Entries, 1)
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			stored, err := segment.ReadEntry(ctx, first.MessageRef)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, wantTask, stored.Task)
			stored.Task = nil
			require.Equal(t, StoredMessage{Ref: first.MessageRef, SourceTaskIdentity: wantIdentity}, stored)
		})
	}
}

func TestSegmentRetryAfterResolution(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	acceptedResult, err := segment.Append(ctx, req)
	accepted := acceptedResult.MessageRef
	require.NoError(t, err)
	require.NoError(t, segment.ResolveEntry(ctx, accepted, historydlqpb.RESOLUTION_MERGED))
	now = now.Add(time.Hour)
	retryResult, err := segment.Append(ctx, req)
	retry := retryResult.MessageRef
	require.NoError(t, err)
	require.Equal(t, accepted.EntryKey, retry.EntryKey)
	require.NotEqual(t, accepted, retry)
	stored, err := segment.ReadEntry(ctx, retry)
	require.NoError(t, err)
	require.NotNil(t, stored.Task)
	protorequire.ProtoEqual(t, req.Task, stored.Task)
	require.NoError(t, segment.ResolveEntry(ctx, accepted, historydlqpb.RESOLUTION_PURGED))
	_, err = segment.ReadEntry(ctx, retry)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestSegmentResolveOneEntry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := testEnqueueRequest()
	second := testEnqueueRequest()
	second.SourceTaskIdentity.TaskID++
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRefResult, err := segment.Append(ctx, first)
	firstRef := firstRefResult.MessageRef
	require.NoError(t, err)
	secondRefResult, err := segment.Append(ctx, second)
	secondRef := secondRefResult.MessageRef
	require.NoError(t, err)

	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_MERGED))
	_, err = segment.ReadEntry(ctx, firstRef)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.NotContains(t, segment.Entries, firstRef.EntryKey)
	pending, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.NotNil(t, pending.Task)
}

func TestSegmentLateRetryRecreatesEntryWithSameClock(t *testing.T) {
	t.Parallel()
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			first, err := segment.Append(ctx, req)
			require.NoError(t, err)
			require.NoError(t, segment.ResolveEntry(ctx, first.MessageRef, reason))
			retry, err := segment.Append(ctx, req)
			require.NoError(t, err)
			stored, err := segment.ReadEntry(ctx, retry.MessageRef)
			require.NoError(t, err)
			require.NotNil(t, stored.Task)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			require.Len(t, segment.Entries, 1)
		})
	}
}

func TestSegmentReferenceValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		change   func(*MessageRef)
		notFound bool
	}{
		{"business ID", func(ref *MessageRef) { ref.BusinessID = "other-segment" }, true},
		{"run ID", func(ref *MessageRef) { ref.RunID = "replacement-run" }, true},
		{"entry key", func(ref *MessageRef) { ref.EntryKey = "other-entry" }, true},
		{"queue", func(ref *MessageRef) { ref.Queue.TargetCluster = "other-target" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			refResult, err := segment.Append(ctx, req)
			ref := refResult.MessageRef
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			invalid := ref
			tc.change(&invalid)
			_, readErr := segment.ReadEntry(ctx, invalid)
			resolveErr := segment.ResolveEntry(ctx, invalid, historydlqpb.RESOLUTION_PURGED)
			if tc.name == "entry key" {
				require.ErrorAs(t, readErr, new(*serviceerror.NotFound))
				require.NoError(t, resolveErr)
			} else {
				for _, err := range []error{readErr, resolveErr} {
					if tc.notFound {
						require.ErrorAs(t, err, new(*serviceerror.NotFound))
					} else {
						require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
					}
				}
			}
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			stored, err := segment.ReadEntry(ctx, ref)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			stored.Task = nil
			require.Equal(t, StoredMessage{
				Ref:                ref,
				SourceTaskIdentity: req.SourceTaskIdentity,
			}, stored)
		})
	}
}

func TestSegmentPersistence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 123, time.UTC)
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	backend := &chasm.MockNodeBackend{
		HandleNow: func() time.Time { return now },
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey(primitives.SystemNamespaceID, "segment-business", "segment-run")
		},
		HandleGetExecutionInfo:    func() *persistencespb.WorkflowExecutionInfo { return &persistencespb.WorkflowExecutionInfo{} },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	root := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(t.Context(), root)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	admissionTime := now
	now = now.Add(time.Second)
	refResult, err := segment.Append(ctx, req)
	ref := refResult.MessageRef
	require.NoError(t, err)
	wantID, err := messageID(req.Queue, req.SourceTaskIdentity, admissionTime)
	require.NoError(t, err)
	require.Equal(t, admissionTime, ref.EnqueuedAt)
	require.Equal(t, wantID, ref.MessageID)
	require.NoError(t, root.SetRootComponent(segment))
	reload := func() chasm.NodesMutation {
		wantState := proto.Clone(segment.SegmentState)
		mutation, err := root.CloseTransaction(t.Context(), true)
		require.NoError(t, err)
		nodes := root.Snapshot(nil).Nodes
		for path, node := range nodes {
			nodes[path] = proto.Clone(node).(*persistencespb.ChasmNode)
		}
		root, err = chasm.NewTreeFromDB(nodes, registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
		require.NoError(t, err)
		now = now.Add(time.Hour)
		ctx = chasm.NewMutableContext(t.Context(), root)
		component, err := root.Component(ctx, chasm.ComponentRef{})
		require.NoError(t, err)
		segment = component.(*Segment)
		protorequire.ProtoEqual(t, wantState, segment.SegmentState)
		return mutation
	}
	checkStored := func(want StoredMessage) {
		stored, err := segment.ReadEntry(ctx, want.Ref)
		require.NoError(t, err)
		protorequire.ProtoEqual(t, want.Task, stored.Task)
		want.Task, stored.Task = nil, nil
		require.Equal(t, want, stored)
	}
	reload()
	checkStored(StoredMessage{
		Ref:                ref,
		SourceTaskIdentity: req.SourceTaskIdentity,
		Task:               req.Task,
	})
	duplicateResult, err := segment.Append(ctx, req)
	duplicate := duplicateResult.MessageRef
	require.NoError(t, err)
	require.Equal(t, ref, duplicate)
	second := testEnqueueRequest()
	second.SourceTaskIdentity.TaskID++
	secondRefResult, err := segment.Append(ctx, second)
	secondRef := secondRefResult.MessageRef
	require.NoError(t, err)
	require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_PURGED))
	mutation := reload()
	require.Len(t, mutation.DeletedNodes, 2, "resolution must delete the entry component and its task DATA node")
	require.NotContains(t, segment.Entries, ref.EntryKey)
	_, err = segment.ReadEntry(ctx, ref)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_PURGED))
	checkStored(StoredMessage{
		Ref:                secondRef,
		SourceTaskIdentity: second.SourceTaskIdentity,
		Task:               second.Task,
	})
	duplicateResult, err = segment.Append(ctx, req)
	duplicate = duplicateResult.MessageRef
	require.NoError(t, err)
	require.Equal(t, ref.EntryKey, duplicate.EntryKey)
	require.NotEqual(t, ref, duplicate)
	require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_MERGED))
	require.NoError(t, segment.ResolveEntry(ctx, duplicate, historydlqpb.RESOLUTION_MERGED))
	reload()
	require.Equal(t, chasm.LifecycleStateRunning, segment.LifecycleState(ctx))
	require.NotContains(t, segment.Entries, ref.EntryKey)
	_, err = segment.ReadEntry(ctx, duplicate)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestSegmentResolveDeletesEntry(t *testing.T) {
	t.Parallel()
	for _, reason := range []historydlqpb.Resolution{historydlqpb.RESOLUTION_MERGED, historydlqpb.RESOLUTION_PURGED} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			emptyRetainedBytes := segment.GetRetainedBytes()
			refResult, err := segment.Append(ctx, req)
			ref := refResult.MessageRef
			require.NoError(t, err)
			require.NoError(t, segment.ResolveEntry(ctx, ref, reason))
			require.Empty(t, segment.Entries)
			require.Equal(t, emptyRetainedBytes, segment.GetRetainedBytes())
			require.Equal(t, minMessageIDSearchAttribute.Value(-1), segment.SearchAttributes(ctx)[2])
			require.Equal(t, chasm.LifecycleStateRunning, segment.LifecycleState(ctx))
			_, err = segment.ReadEntry(ctx, ref)
			require.ErrorAs(t, err, new(*serviceerror.NotFound))
			require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_MERGED))
			require.NoError(t, segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_PURGED))
			require.Equal(t, emptyRetainedBytes, segment.GetRetainedBytes())
		})
	}
}

func TestSegmentResolveMissingEntryValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*MessageRef)
	}{
		{"business ID", func(ref *MessageRef) { ref.BusinessID = "other-segment" }},
		{"run ID", func(ref *MessageRef) { ref.RunID = "other-run" }},
		{"queue", func(ref *MessageRef) { ref.Queue.TargetCluster = "other-target" }},
		{"missing entry key", func(ref *MessageRef) { ref.EntryKey = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			result, err := segment.Append(ctx, req)
			require.NoError(t, err)
			require.NoError(t, segment.ResolveEntry(ctx, result.MessageRef, historydlqpb.RESOLUTION_PURGED))
			before := proto.Clone(segment.SegmentState)
			ref := result.MessageRef
			tc.change(&ref)
			err = segment.ResolveEntry(ctx, ref, historydlqpb.RESOLUTION_MERGED)
			switch tc.name {
			case "business ID", "run ID":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			case "queue":
				require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			default:
				require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			}
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Empty(t, segment.Entries)
		})
	}
}

func TestSegmentNumericIDCollision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := testEnqueueRequest()
	first.SourceTaskIdentity.TaskID = 7115
	second := testEnqueueRequest()
	second.SourceTaskIdentity.TaskID = 7134
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRefResult, err := segment.Append(ctx, first)
	firstRef := firstRefResult.MessageRef
	require.NoError(t, err)
	secondRefResult, err := segment.Append(ctx, second)
	secondRef := secondRefResult.MessageRef
	require.NoError(t, err)

	require.Equal(t, firstRef.MessageID, secondRef.MessageID)
	require.NotEqual(t, firstRef.EntryKey, secondRef.EntryKey)
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	_, err = segment.ReadEntry(ctx, firstRef)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	secondStored, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	require.NotNil(t, secondStored.Task)
}

func TestSegmentAppendSparseTaskIDs(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	refs := make(map[string]MessageRef)
	for _, taskID := range []int64{1, 129, 1 << 40, math.MaxInt64} {
		req.SourceTaskIdentity.TaskID = taskID
		refResult, err := segment.Append(ctx, req)
		ref := refResult.MessageRef
		require.NoError(t, err)
		require.NotContains(t, refs, ref.EntryKey)
		refs[ref.EntryKey] = ref
		stored, err := segment.ReadEntry(ctx, ref)
		require.NoError(t, err)
		require.Equal(t, req.SourceTaskIdentity, stored.SourceTaskIdentity)
		protorequire.ProtoEqual(t, req.Task, stored.Task)
		now = now.Add(time.Hour)
		duplicateResult, err := segment.Append(ctx, req)
		duplicate := duplicateResult.MessageRef
		require.NoError(t, err)
		require.Equal(t, ref, duplicate)
	}
	require.Len(t, segment.Entries, 4)
}

func TestSegmentMembership(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*Segment, *EnqueueRequest){
		"queue":        func(_ *Segment, r *EnqueueRequest) { r.Queue.TargetCluster += "-other" },
		"source shard": func(_ *Segment, r *EnqueueRequest) { r.SourceTaskIdentity.SourceShardID++ },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			acceptedResult, err := segment.Append(ctx, req)
			accepted := acceptedResult.MessageRef
			require.NoError(t, err)
			req.SourceTaskIdentity.TaskID++
			change(segment, &req)
			before := proto.Clone(segment.SegmentState)
			refResult, err := segment.Append(ctx, req)
			ref := refResult.MessageRef
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Equal(t, MessageRef{}, ref)
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Len(t, segment.Entries, 1)
			stored, err := segment.ReadEntry(ctx, accepted)
			require.NoError(t, err)
			require.NotNil(t, stored.Task)
		})
	}
}

func TestSegmentAdmissionAcrossTaskIDs(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	first := testEnqueueRequest()
	first.Task.Blob.Data = bytes.Repeat([]byte{42}, 3<<20)
	segment, err := NewSegment(ctx, first)
	require.NoError(t, err)
	firstRefResult, err := segment.Append(ctx, first)
	firstRef := firstRefResult.MessageRef
	require.NoError(t, err)
	before := proto.Clone(segment.SegmentState)
	second := testEnqueueRequest()
	second.SourceTaskIdentity.TaskID += 1 << 40
	second.Task.Blob.Data = bytes.Repeat([]byte{42}, 2<<20)
	_, err = segment.Append(ctx, second)
	require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
	protorequire.ProtoEqual(t, before, segment.SegmentState)
	require.Len(t, segment.Entries, 1)
	independent := second
	independent.SourceTaskIdentity.SourceShardID++
	independent.Task = proto.Clone(second.Task).(*commonspb.HistoryTask)
	independent.Task.ShardId = independent.SourceTaskIdentity.SourceShardID
	otherSegment, err := NewSegment(ctx, independent)
	require.NoError(t, err)
	_, err = otherSegment.Append(ctx, independent)
	require.NoError(t, err)
	duplicateResult, err := segment.Append(ctx, first)
	duplicate := duplicateResult.MessageRef
	require.NoError(t, err)
	require.Equal(t, firstRef, duplicate)
	require.NoError(t, segment.ResolveEntry(ctx, firstRef, historydlqpb.RESOLUTION_PURGED))
	secondRefResult, err := segment.Append(ctx, second)
	secondRef := secondRefResult.MessageRef
	require.NoError(t, err)
	stored, err := segment.ReadEntry(ctx, secondRef)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, second.Task, stored.Task)
	before = proto.Clone(segment.SegmentState)
	first.Task.Blob.Data = bytes.Repeat([]byte{42}, 4<<20)
	_, err = segment.Append(ctx, first)
	require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
	protorequire.ProtoEqual(t, before, segment.SegmentState)
	require.LessOrEqual(t, segment.GetRetainedBytes(), int64(4<<20))
}

func TestSegmentOversizedEntry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 4<<20)
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	before := proto.Clone(segment.SegmentState)
	_, err = segment.Append(ctx, req)
	require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
	protorequire.ProtoEqual(t, before, segment.SegmentState)
	require.Empty(t, segment.Entries)
}

func newSegmentContext(now *time.Time) *chasm.MockMutableContext {
	return &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return *now },
		HandleExecutionKey: func() chasm.ExecutionKey {
			return chasm.ExecutionKey{NamespaceID: primitives.SystemNamespaceID, BusinessID: "segment-business", RunID: "segment-run"}
		},
	}}
}

func TestSegmentEntryIdentityUsesRootShard(t *testing.T) {
	t.Parallel()
	for _, shardID := range []int32{7, 42} {
		t.Run(fmt.Sprint(shardID), func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			req.SourceTaskIdentity.SourceShardID = shardID
			req.Task.ShardId = shardID
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			result, err := segment.Append(ctx, req)
			require.NoError(t, err)
			entry := segment.Entries[result.MessageRef.EntryKey].Get(ctx)
			require.Nil(t, entry.GetSourceTaskIdentity().ProtoReflect().Descriptor().Fields().ByName("source_shard_id"),
				"persisted entry identity must not duplicate the segment source shard")
			stored, err := segment.ReadEntry(ctx, result.MessageRef)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			stored.Task = nil
			require.Equal(t, StoredMessage{Ref: result.MessageRef, SourceTaskIdentity: req.SourceTaskIdentity}, stored)
			entry.Task = chasm.Field[*commonspb.HistoryTask]{}
			page, err := segment.ListEntries(ctx, ListEntriesRequest{
				Segment:               SegmentRef{Queue: req.Queue, BusinessID: result.MessageRef.BusinessID, RunID: result.MessageRef.RunID},
				InclusiveMaxMessageID: math.MaxInt64, PageSize: 1,
			})
			require.NoError(t, err)
			require.Equal(t, EntryPage{Refs: []MessageRef{result.MessageRef}}, page)
		})
	}
}

func TestSegmentRejectsInvalidTask(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*EnqueueRequest)
	}{
		{"nil task", func(req *EnqueueRequest) { req.Task = nil }},
		{"nil blob", func(req *EnqueueRequest) { req.Task.Blob = nil }},
		{"empty payload", func(req *EnqueueRequest) { req.Task.Blob.Data = nil }},
		{"unspecified encoding", func(req *EnqueueRequest) { req.Task.Blob.EncodingType = enumspb.ENCODING_TYPE_UNSPECIFIED }},
		{"unsupported encoding", func(req *EnqueueRequest) { req.Task.Blob.EncodingType = enumspb.EncodingType(100) }},
		{"wrong source shard", func(req *EnqueueRequest) { req.Task.ShardId++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			before := proto.Clone(segment.SegmentState)
			tc.change(&req)
			result, err := segment.Append(ctx, req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Equal(t, appendResult{}, result)
			protorequire.ProtoEqual(t, before, segment.SegmentState)
			require.Empty(t, segment.Entries)
		})
	}
}

func TestSegmentWarningThreshold(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ctx := newSegmentContext(&now)
	req := testEnqueueRequest()
	segment, err := NewSegment(ctx, req)
	require.NoError(t, err)
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 2<<20)
	first, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.False(t, first.CrossedWarningThreshold)
	require.Less(t, segment.GetRetainedBytes(), segmentWarningBytesThreshold)
	req.SourceTaskIdentity.TaskID++
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 1<<20)
	crossing, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.True(t, crossing.CrossedWarningThreshold)
	require.GreaterOrEqual(t, segment.GetRetainedBytes(), segmentWarningBytesThreshold)
	duplicate, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.Equal(t, appendResult{MessageRef: crossing.MessageRef}, duplicate)
	req.SourceTaskIdentity.TaskID++
	req.Task.Blob.Data = []byte("small payload")
	above, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.False(t, above.CrossedWarningThreshold)
	require.NoError(t, segment.ResolveEntry(ctx, crossing.MessageRef, historydlqpb.RESOLUTION_PURGED))
	require.Less(t, segment.GetRetainedBytes(), segmentWarningBytesThreshold)
	req.SourceTaskIdentity.TaskID++
	req.Task.Blob.Data = bytes.Repeat([]byte{42}, 1<<20)
	recrossing, err := segment.Append(ctx, req)
	require.NoError(t, err)
	require.True(t, recrossing.CrossedWarningThreshold)
}

func TestSegmentReadRejectsChangedMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*MessageRef)
	}{
		{"message ID", func(ref *MessageRef) { ref.MessageID++ }},
		{"enqueue time", func(ref *MessageRef) { ref.EnqueuedAt = ref.EnqueuedAt.Add(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			ctx := newSegmentContext(&now)
			req := testEnqueueRequest()
			segment, err := NewSegment(ctx, req)
			require.NoError(t, err)
			accepted, err := segment.Append(ctx, req)
			require.NoError(t, err)
			ref := accepted.MessageRef
			tc.change(&ref)
			stored, err := segment.ReadEntry(ctx, ref)
			require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
			require.Equal(t, StoredMessage{}, stored)
		})
	}
}
