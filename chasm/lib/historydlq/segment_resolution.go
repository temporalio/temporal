package historydlq

import (
	"slices"

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

type ResolveEntriesResult struct {
	// ResolvedEntryKeys includes requested keys that were already absent.
	ResolvedEntryKeys []string
	// CorruptEntryKeys lists requested entries that were retained.
	CorruptEntryKeys []string
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
// Callers must report CorruptEntryKeys after committing the healthy deletions.
func (s *Segment) ResolveEntries(ctx chasm.MutableContext, req ResolveEntriesRequest) (ResolveEntriesResult, error) {
	if err := validateResolveEntriesRequest(req); err != nil {
		return ResolveEntriesResult{}, err
	}
	if err := s.validateSegment(ctx, req.Segment); err != nil {
		return ResolveEntriesResult{}, err
	}
	result := ResolveEntriesResult{}
	entryKeys := slices.Clone(req.EntryKeys)
	slices.Sort(entryKeys)
	selectedKeys := make(map[string]struct{}, len(req.EntryKeys))
	corruptEntryErrors := make(map[string]error)
	var reclaimedBytes int64
	minimum := s.GetMinMessageId()
	recomputeMinimum := false
	for _, entryKey := range slices.Compact(entryKeys) {
		field, present := s.Entries[entryKey]
		if !present {
			result.ResolvedEntryKeys = append(result.ResolvedEntryKeys, entryKey)
			continue
		}
		if _, err := s.entryMetadata(ctx, entryKey, field); err != nil {
			corruptEntryErrors[entryKey] = err
			result.CorruptEntryKeys = append(result.CorruptEntryKeys, entryKey)
			continue
		}
		entry := field.Get(ctx)
		selectedKeys[entryKey] = struct{}{}
		result.ResolvedEntryKeys = append(result.ResolvedEntryKeys, entryKey)
		reclaimedBytes += int64(proto.Size(entry.EntryState)+len(entryKey)) + entry.GetPayloadSizeBytes() + segmentNodeReserve
		recomputeMinimum = recomputeMinimum || minimum == 0 || entry.GetMessageId() <= minimum
	}
	if recomputeMinimum {
		minimum = -1
		for entryKey, field := range s.Entries {
			if _, selected := selectedKeys[entryKey]; selected {
				continue
			}
			entry, ok := field.TryGet(ctx)
			if !ok || entry == nil || entry.EntryState == nil || entry.GetMessageId() < 0 {
				corruptEntryErrors[entryKey] = serviceerror.NewDataLoss("history DLQ segment entry message ID is missing or invalid")
				// Unknown message IDs require a conservative lower bound.
				minimum = 0
				continue
			}
			if minimum < 0 || entry.GetMessageId() < minimum {
				minimum = entry.GetMessageId()
			}
		}
	}
	for entryKey, err := range corruptEntryErrors {
		s.logCorruptEntry(ctx, entryKey, "ResolveEntries", err)
	}
	// Prepare accounting and the new minimum before deleting any entries.
	for entryKey := range selectedKeys {
		delete(s.Entries, entryKey)
	}
	s.RetainedBytes -= reclaimedBytes
	s.MinMessageId = minimum
	return result, nil
}
