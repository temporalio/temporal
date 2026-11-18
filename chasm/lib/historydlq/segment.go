package historydlq

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// Reserve headroom under the default 8 MiB mutable-state limit.
	segmentRetainedBytesLimit    int64 = 4 << 20
	segmentWarningBytesThreshold int64 = 3 << 20
	// Covers framework node metadata and paths.
	segmentNodeReserve int64 = 1024
)

type Segment struct {
	chasm.UnimplementedComponent
	*historydlqpb.SegmentState
	Entries    chasm.Map[string, *Entry]
	Visibility chasm.Field[*chasm.Visibility]
}

type appendResult struct {
	MessageRef              MessageRef
	CrossedWarningThreshold bool
}

var (
	entryNotFoundErr         = serviceerror.NewNotFound("history DLQ segment entry not found")
	segmentBudgetExceededErr = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "history DLQ segment storage budget exceeded",
	}
	_ chasm.RootComponent                      = (*Segment)(nil)
	_ chasm.VisibilitySearchAttributesProvider = (*Segment)(nil)
	_ chasm.VisibilityMemoProvider             = (*Segment)(nil)
)

func NewSegment(ctx chasm.MutableContext, req EnqueueRequest) (*Segment, error) {
	if ctx.ExecutionKey().NamespaceID != primitives.SystemNamespaceID {
		return nil, serviceerror.NewInvalidArgument("history DLQ segments require the temporal-system namespace")
	}
	if err := validateIdentity(req.Queue, req.SourceTaskIdentity); err != nil {
		return nil, err
	}
	state := &historydlqpb.SegmentState{
		Queue:         queueKeyToProto(req.Queue),
		SourceShardId: req.SourceTaskIdentity.SourceShardID,
		MinMessageId:  -1,
	}
	state.RetainedBytes = int64(proto.Size(state)) + segmentNodeReserve
	if state.RetainedBytes > segmentRetainedBytesLimit {
		return nil, segmentBudgetExceededErr
	}
	return &Segment{
		SegmentState: state,
		Entries:      make(chasm.Map[string, *Entry]),
		Visibility:   chasm.NewComponentField(ctx, chasm.NewVisibility(ctx)),
	}, nil
}

func (s *Segment) Append(ctx chasm.MutableContext, req EnqueueRequest) (appendResult, error) {
	if ctx.ExecutionKey().NamespaceID != primitives.SystemNamespaceID {
		return appendResult{}, serviceerror.NewInvalidArgument("history DLQ segments require the temporal-system namespace")
	}
	if queueProtoToKey(s.GetQueue()) != req.Queue ||
		req.SourceTaskIdentity.SourceShardID != s.GetSourceShardId() {
		return appendResult{}, serviceerror.NewInvalidArgument("history DLQ entry does not belong to this segment")
	}
	entryKey, err := EntryKey(req.Queue, req.SourceTaskIdentity)
	if err != nil {
		return appendResult{}, err
	}
	if entryField, ok := s.Entries[entryKey]; ok {
		entry, ok := entryField.TryGet(ctx)
		if !ok {
			return appendResult{}, serviceerror.NewDataLoss("history DLQ segment entry is missing")
		}
		return appendResult{MessageRef: s.messageReference(ctx, entryKey, entry.EntryState)}, nil
	}
	if int64(proto.Size(req.Task)) > segmentRetainedBytesLimit-s.GetRetainedBytes() {
		return appendResult{}, segmentBudgetExceededErr
	}
	entry, err := newEntry(ctx, req, ctx.Now(s))
	if err != nil {
		return appendResult{}, err
	}
	entryBytes := int64(proto.Size(entry.EntryState)+len(entryKey)) + entry.GetPayloadSizeBytes() + segmentNodeReserve
	if entryBytes > segmentRetainedBytesLimit-s.GetRetainedBytes() {
		return appendResult{}, segmentBudgetExceededErr
	}
	s.Entries[entryKey] = chasm.NewComponentField(ctx, entry)
	if s.GetMinMessageId() < 0 || entry.GetMessageId() < s.GetMinMessageId() {
		s.MinMessageId = entry.GetMessageId()
	}
	previousRetainedBytes := s.GetRetainedBytes()
	s.RetainedBytes += entryBytes
	return appendResult{
		MessageRef:              s.messageReference(ctx, entryKey, entry.EntryState),
		CrossedWarningThreshold: previousRetainedBytes < segmentWarningBytesThreshold && s.GetRetainedBytes() >= segmentWarningBytesThreshold,
	}, nil
}

func (s *Segment) ReadEntry(ctx chasm.Context, ref MessageRef) (StoredMessage, error) {
	entry, err := s.findEntry(ctx, ref)
	if err != nil {
		return StoredMessage{}, err
	}
	if !sameEntryReference(s.messageReference(ctx, ref.EntryKey, entry.EntryState), ref) {
		return StoredMessage{}, serviceerror.NewFailedPrecondition("history DLQ reference does not match the stored entry")
	}
	return entry.Read(ctx, ref, s.GetSourceShardId())
}

// ResolveEntry deletes an entry and its payload. Missing entries are already resolved.
func (s *Segment) ResolveEntry(ctx chasm.MutableContext, ref MessageRef, reason historydlqpb.Resolution) error {
	return s.ResolveEntries(ctx, ResolveEntriesRequest{
		Segment:    SegmentRef{Queue: ref.Queue, BusinessID: ref.BusinessID, RunID: ref.RunID},
		EntryKeys:  []string{ref.EntryKey},
		Resolution: reason,
	})
}

func (s *Segment) LifecycleState(chasm.Context) chasm.LifecycleState {
	// TODO: Evaluate segment closure after the full feature release.
	return chasm.LifecycleStateRunning
}

func (s *Segment) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("history DLQ segments do not support termination")
}

func (s *Segment) ContextMetadata(chasm.Context) map[string]string { return nil }

func (s *Segment) SearchAttributes(chasm.Context) []chasm.SearchAttributeKeyValue {
	queue := s.GetQueue()
	return []chasm.SearchAttributeKeyValue{
		categorySearchAttribute.Value(int64(queue.GetTaskCategory())),
		sourceSearchAttribute.Value(queue.GetSourceCluster()),
		targetSearchAttribute.Value(queue.GetTargetCluster()),
		minMessageIDSearchAttribute.Value(s.GetMinMessageId()),
	}
}

func (s *Segment) Memo(chasm.Context) proto.Message { return &emptypb.Empty{} }

func (s *Segment) findEntry(ctx chasm.Context, ref MessageRef) (*Entry, error) {
	key := ctx.ExecutionKey()
	if key.NamespaceID != primitives.SystemNamespaceID || ref.BusinessID != key.BusinessID || ref.RunID != key.RunID {
		return nil, serviceerror.NewNotFound("history DLQ segment entry not found")
	}
	if ref.Queue != queueProtoToKey(s.GetQueue()) {
		return nil, serviceerror.NewFailedPrecondition("history DLQ reference does not match the segment queue")
	}
	entryField, ok := s.Entries[ref.EntryKey]
	if !ok {
		return nil, entryNotFoundErr
	}
	entry, ok := entryField.TryGet(ctx)
	if !ok {
		return nil, serviceerror.NewDataLoss("history DLQ segment entry is missing")
	}
	return entry, nil
}

func (s *Segment) messageReference(ctx chasm.Context, entryKey string, state *historydlqpb.EntryState) MessageRef {
	key := ctx.ExecutionKey()
	return MessageRef{
		BusinessID: key.BusinessID,
		RunID:      key.RunID,
		MessageID:  state.GetMessageId(),
		Queue:      queueProtoToKey(s.GetQueue()),
		EnqueuedAt: state.GetEnqueuedAt().AsTime(),
		EntryKey:   entryKey,
	}
}

func sameEntryReference(a, b MessageRef) bool {
	return a.BusinessID == b.BusinessID && a.RunID == b.RunID &&
		a.MessageID == b.MessageID && a.Queue == b.Queue && a.EnqueuedAt.Equal(b.EnqueuedAt) && a.EntryKey == b.EntryKey
}
