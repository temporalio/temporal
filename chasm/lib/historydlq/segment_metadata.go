package historydlq

import (
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
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
}

type SegmentStats struct{ PendingCount, LastMessageID int64 }

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
	for key, field := range s.Entries {
		ref, err := s.entryMetadata(ctx, key, field)
		if err != nil {
			return EntryPage{}, err
		}
		if ref.MessageID <= req.InclusiveMaxMessageID && (cursor == nil || compareEntryTuple(ref.MessageID, ref.EntryKey, cursor.MessageID, cursor.EntryKey) > 0) {
			refs = append(refs, ref)
		}
	}
	slices.SortFunc(refs, func(a, b MessageRef) int {
		return compareEntryTuple(a.MessageID, a.EntryKey, b.MessageID, b.EntryKey)
	})
	page := EntryPage{Refs: refs}
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
		metadata, err := s.entryMetadata(ctx, key, field)
		if err != nil {
			return SegmentStats{}, err
		}
		stats.PendingCount++
		stats.LastMessageID = max(stats.LastMessageID, metadata.MessageID)
	}
	return stats, nil
}
