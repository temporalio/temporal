package historydlq

import (
	"bytes"
	"cmp"
	"encoding/json"
	"io"

	"go.temporal.io/api/serviceerror"
)

type segmentCursor struct {
	Version   int
	Segment   SegmentRef
	Bound     int64
	MessageID int64
	EntryKey  string
}

func compareEntryTuple(idA int64, keyA string, idB int64, keyB string) int {
	if order := cmp.Compare(idA, idB); order != 0 {
		return order
	}
	return cmp.Compare(keyA, keyB)
}

func encodeSegmentCursor(req ListEntriesRequest, ref MessageRef) []byte {
	token, _ := json.Marshal(segmentCursor{Version: 1, Segment: req.Segment, Bound: req.InclusiveMaxMessageID, MessageID: ref.MessageID, EntryKey: ref.EntryKey})
	return token
}

func decodeSegmentCursor(req ListEntriesRequest) (*segmentCursor, error) {
	if len(req.NextPageToken) == 0 {
		return nil, nil
	}
	var token struct {
		Version   int
		Segment   SegmentRef
		Bound     *int64
		MessageID *int64
		EntryKey  string
	}
	decoder := json.NewDecoder(bytes.NewReader(req.NextPageToken))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&token); err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid history DLQ segment page token")
	}
	if decoder.Decode(new(any)) != io.EOF || token.Version != 1 || token.Segment != req.Segment || token.Bound == nil || token.MessageID == nil || *token.Bound != req.InclusiveMaxMessageID || *token.MessageID < 0 || *token.MessageID > *token.Bound || token.EntryKey == "" {
		return nil, serviceerror.NewInvalidArgument("history DLQ segment page token does not match the request")
	}
	return &segmentCursor{Version: token.Version, Segment: token.Segment, Bound: *token.Bound, MessageID: *token.MessageID, EntryKey: token.EntryKey}, nil
}
