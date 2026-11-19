package historydlq

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
)

type SegmentRef struct {
	Queue             QueueKey
	BusinessID, RunID string
}

func (s *Segment) logCorruptEntry(ctx chasm.Context, key, operation string, err error) {
	ctx.Logger().Error("History DLQ message could not be processed and was retained", s.corruptEntryTags(ctx, key, operation, err)...)
	metrics.HistoryDLQCorruptEntry.With(ctx.MetricsHandler()).Record(1)
}

func (s *Segment) logPurgedCorruptEntry(ctx chasm.Context, key string, err error) {
	ctx.Logger().Warn("History DLQ message with invalid metadata was purged", s.corruptEntryTags(ctx, key, "ResolveEntries", err)...)
}

func (s *Segment) corruptEntryTags(ctx chasm.Context, key, operation string, err error) []tag.Tag {
	queue := s.GetQueue()
	tags := []tag.Tag{
		tag.TaskCategoryID(int(queue.GetTaskCategory())),
		tag.SourceCluster(queue.GetSourceCluster()),
		tag.TargetCluster(queue.GetTargetCluster()),
		tag.String("entry-key", key),
		tag.Operation(operation),
		tag.Error(err),
	}
	entry, ok := s.Entries[key].TryGet(ctx)
	if ok && entry != nil && entry.EntryState != nil && entry.GetMessageId() >= 0 {
		tags = append(tags, tag.DLQMessageID(entry.GetMessageId()))
	}
	return tags
}

func validateSegmentRef(ref SegmentRef) error {
	if ref.BusinessID == "" || ref.RunID == "" {
		return serviceerror.NewInvalidArgument("history DLQ requires an exact segment Business ID and Run ID")
	}
	return validateQueue(ref.Queue)
}

func (s *Segment) validateSegment(ctx chasm.Context, ref SegmentRef) error {
	if err := validateSegmentRef(ref); err != nil {
		return err
	}
	key := ctx.ExecutionKey()
	if key.NamespaceID != primitives.SystemNamespaceID || key.BusinessID != ref.BusinessID || key.RunID != ref.RunID {
		return serviceerror.NewNotFound("history DLQ segment not found")
	}
	if queueProtoToKey(s.GetQueue()) != ref.Queue {
		return serviceerror.NewFailedPrecondition("history DLQ reference does not match the segment queue")
	}
	return nil
}

func (s *Segment) entryMetadata(ctx chasm.Context, key string, field chasm.Field[*Entry]) (MessageRef, error) {
	entry, ok := field.TryGet(ctx)
	if !ok || entry == nil || entry.EntryState == nil || entry.GetEnqueuedAt() == nil || entry.GetEnqueuedAt().CheckValid() != nil {
		return MessageRef{}, serviceerror.NewDataLoss("history DLQ segment entry metadata is missing or invalid")
	}
	ref := s.messageReference(ctx, key, entry.EntryState)
	identity := entryTaskIdentityProtoToIdentity(entry.GetSourceTaskIdentity(), s.GetSourceShardId())
	expectedKey, err := EntryKey(ref.Queue, identity)
	if err != nil || expectedKey != key || validateMessageRef(ref) != nil {
		return MessageRef{}, serviceerror.NewDataLoss("history DLQ segment entry metadata does not match its identity")
	}
	return ref, nil
}
