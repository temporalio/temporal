package historydlq

import "go.temporal.io/server/chasm"

type ReadSegmentMessagesRequest ListEntriesRequest

type MessagePage struct {
	Messages      []StoredMessage
	NextPageToken []byte
}

// ReadSegmentMessages copies bounded metadata and payloads under one component read context.
func (s *Segment) ReadSegmentMessages(ctx chasm.Context, req ReadSegmentMessagesRequest) (MessagePage, error) {
	entries, err := s.ListEntries(ctx, ListEntriesRequest(req))
	if err != nil {
		return MessagePage{}, err
	}
	page := MessagePage{NextPageToken: entries.NextPageToken}
	for _, ref := range entries.Refs {
		message, err := s.ReadEntry(ctx, ref)
		if err != nil {
			return MessagePage{}, err
		}
		page.Messages = append(page.Messages, message)
	}
	return page, nil
}
