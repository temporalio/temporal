package historydlq

import (
	"errors"
	"slices"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
)

type ReadSegmentMessagesRequest ListEntriesRequest

type MessagePage struct {
	Messages      []StoredMessage
	NextPageToken []byte
	// CorruptEntryKeys includes retained metadata failures and unreadable payloads on this page.
	CorruptEntryKeys []string
}

// ReadSegmentMessages copies bounded metadata and payloads under one component read context.
func (s *Segment) ReadSegmentMessages(ctx chasm.Context, req ReadSegmentMessagesRequest) (MessagePage, error) {
	entries, err := s.ListEntries(ctx, ListEntriesRequest(req))
	if err != nil {
		return MessagePage{}, err
	}
	page := MessagePage{NextPageToken: entries.NextPageToken, CorruptEntryKeys: entries.CorruptEntryKeys}
	for _, ref := range entries.Refs {
		message, err := s.ReadEntry(ctx, ref)
		if err != nil {
			if _, corrupt := errors.AsType[*serviceerror.DataLoss](err); corrupt {
				s.logCorruptEntry(ctx, ref.EntryKey, "ReadSegmentMessages", err)
				page.CorruptEntryKeys = append(page.CorruptEntryKeys, ref.EntryKey)
				continue
			}
			return MessagePage{}, err
		}
		page.Messages = append(page.Messages, message)
	}
	slices.Sort(page.CorruptEntryKeys)
	return page, nil
}
