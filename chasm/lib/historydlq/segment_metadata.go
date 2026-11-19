package historydlq

import (
	"slices"

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

type ListEntriesRequest struct {
	Segment               SegmentRef
	InclusiveMaxMessageID int64
	PageSize              int
	NextPageToken         []byte
}

type EntryPage struct {
	Refs          []MessageRef
	NextPageToken []byte
	// CorruptEntryKeys lists retained entries across the segment, repeated on each page.
	CorruptEntryKeys []string
}

type SegmentStats struct {
	PendingCount, LastMessageID int64
	// CorruptEntryKeys identifies entries excluded from LastMessageID.
	CorruptEntryKeys []string
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

func validateListEntriesRequest(req ListEntriesRequest) error {
	if err := validateSegmentRef(req.Segment); err != nil {
		return err
	}
	if req.PageSize <= 0 || req.InclusiveMaxMessageID < 0 {
		return serviceerror.NewInvalidArgument("history DLQ requires a positive page size and nonnegative message bound")
	}
	return nil
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

// ListEntries returns pending entry references without loading task payloads.
// Callers must process Refs and report CorruptEntryKeys as partial failure.
func (s *Segment) ListEntries(ctx chasm.Context, req ListEntriesRequest) (EntryPage, error) {
	if err := validateListEntriesRequest(req); err != nil {
		return EntryPage{}, err
	}
	cursor, err := decodeSegmentCursor(req)
	if err != nil {
		return EntryPage{}, err
	}
	if err := s.validateSegment(ctx, req.Segment); err != nil {
		return EntryPage{}, err
	}
	var refs []MessageRef
	var corruptEntryKeys []string
	for key, field := range s.Entries {
		ref, err := s.entryMetadata(ctx, key, field)
		if err != nil {
			s.logCorruptEntry(ctx, key, "ListEntries", err)
			corruptEntryKeys = append(corruptEntryKeys, key)
			continue
		}
		if ref.MessageID <= req.InclusiveMaxMessageID && (cursor == nil || compareEntryTuple(ref.MessageID, ref.EntryKey, cursor.MessageID, cursor.EntryKey) > 0) {
			refs = append(refs, ref)
		}
	}
	slices.SortFunc(refs, func(a, b MessageRef) int {
		return compareEntryTuple(a.MessageID, a.EntryKey, b.MessageID, b.EntryKey)
	})
	slices.Sort(corruptEntryKeys)
	page := EntryPage{Refs: refs, CorruptEntryKeys: corruptEntryKeys}
	if len(refs) > req.PageSize {
		page.Refs = refs[:req.PageSize]
		page.NextPageToken = encodeSegmentCursor(req, page.Refs[len(page.Refs)-1])
	}
	return page, nil
}

func (s *Segment) DescribeSegment(ctx chasm.Context, ref SegmentRef) (SegmentStats, error) {
	if err := s.validateSegment(ctx, ref); err != nil {
		return SegmentStats{}, err
	}
	stats := SegmentStats{}
	for key, field := range s.Entries {
		stats.PendingCount++
		metadata, err := s.entryMetadata(ctx, key, field)
		if err != nil {
			s.logCorruptEntry(ctx, key, "DescribeSegment", err)
			stats.CorruptEntryKeys = append(stats.CorruptEntryKeys, key)
			continue
		}
		stats.LastMessageID = max(stats.LastMessageID, metadata.MessageID)
	}
	slices.Sort(stats.CorruptEntryKeys)
	return stats, nil
}
