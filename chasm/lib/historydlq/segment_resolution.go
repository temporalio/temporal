package historydlq

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"google.golang.org/protobuf/proto"
)

// MaxResolveBatchSize limits the number of entry keys in one segment update.
const MaxResolveBatchSize = 1000

type ResolveEntriesRequest struct {
	Segment    SegmentRef
	EntryKeys  []string
	Resolution historydlqpb.Resolution
}

func validateResolveEntriesRequest(req ResolveEntriesRequest) error {
	if err := validateSegmentRef(req.Segment); err != nil {
		return err
	}
	if err := validateResolutionReason(req.Resolution); err != nil {
		return err
	}
	if len(req.EntryKeys) > MaxResolveBatchSize {
		return serviceerror.NewInvalidArgument("history DLQ resolve batch exceeds 1000 entry keys")
	}
	for _, entryKey := range req.EntryKeys {
		if entryKey == "" {
			return serviceerror.NewInvalidArgument("history DLQ resolve requires nonempty entry keys")
		}
	}
	return nil
}

// ResolveEntries deletes selected entries and their payloads in one update.
// Absent keys are already resolved; duplicate keys are deleted once.
func (s *Segment) ResolveEntries(ctx chasm.MutableContext, req ResolveEntriesRequest) error {
	if err := validateResolveEntriesRequest(req); err != nil {
		return err
	}
	if err := s.validateSegment(ctx, req.Segment); err != nil {
		return err
	}
	selectedKeys := make(map[string]struct{}, len(req.EntryKeys))
	var reclaimedBytes int64
	minimum := s.GetMinMessageId()
	recomputeMinimum := false
	for _, entryKey := range req.EntryKeys {
		if _, selected := selectedKeys[entryKey]; selected {
			continue
		}
		selectedKeys[entryKey] = struct{}{}
		field, present := s.Entries[entryKey]
		if !present {
			continue
		}
		entry, ok := field.TryGet(ctx)
		if !ok || entry == nil || entry.EntryState == nil {
			return serviceerror.NewDataLoss("history DLQ segment entry metadata is missing")
		}
		reclaimedBytes += int64(proto.Size(entry.EntryState)+len(entryKey)) + entry.GetPayloadSizeBytes() + segmentNodeReserve
		recomputeMinimum = recomputeMinimum || entry.GetMessageId() == minimum
	}
	if recomputeMinimum {
		minimum = -1
		for entryKey, field := range s.Entries {
			if _, selected := selectedKeys[entryKey]; selected {
				continue
			}
			metadata, err := s.entryMetadata(ctx, entryKey, field)
			if err != nil {
				return err
			}
			if minimum < 0 || metadata.MessageID < minimum {
				minimum = metadata.MessageID
			}
		}
	}
	// Prepare accounting and the new minimum before deleting any entries.
	for entryKey := range selectedKeys {
		delete(s.Entries, entryKey)
	}
	s.RetainedBytes -= reclaimedBytes
	s.MinMessageId = minimum
	return nil
}
