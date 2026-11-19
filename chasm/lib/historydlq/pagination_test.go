package historydlq

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
)

func testCursorRequest() ListEntriesRequest {
	return ListEntriesRequest{
		Segment:               SegmentRef{Queue: testRequest().Queue, BusinessID: "segment-business", RunID: "segment-run"},
		PageSize:              1,
		InclusiveMaxMessageID: 10,
	}
}

func TestSegmentCursorRejectsMalformedEnvelopes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		field  string
		value  json.RawMessage
		suffix string
	}{
		{name: "unknown version", field: "Version", value: json.RawMessage(`2`)},
		{name: "missing version", field: "Version"},
		{name: "unknown field", field: "Unknown", value: json.RawMessage(`true`)},
		{name: "missing bound", field: "Bound"},
		{name: "null bound", field: "Bound", value: json.RawMessage(`null`)},
		{name: "missing message ID", field: "MessageID"},
		{name: "null message ID", field: "MessageID", value: json.RawMessage(`null`)},
		{name: "negative message ID", field: "MessageID", value: json.RawMessage(`-1`)},
		{name: "message ID above bound", field: "MessageID", value: json.RawMessage(`11`)},
		{name: "empty entry key", field: "EntryKey", value: json.RawMessage(`""`)},
		{name: "trailing object", suffix: ` {}`},
		{name: "trailing garbage", suffix: ` invalid`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := testCursorRequest()
			req.NextPageToken = encodeSegmentCursor(req, MessageRef{MessageID: 5, EntryKey: "entry"})
			_, err := decodeSegmentCursor(req)
			require.NoError(t, err)
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(req.NextPageToken, &envelope))
			if tc.field != "" {
				if tc.value == nil {
					delete(envelope, tc.field)
				} else {
					envelope[tc.field] = tc.value
				}
			}
			req.NextPageToken, err = json.Marshal(envelope)
			require.NoError(t, err)
			req.NextPageToken = append(req.NextPageToken, tc.suffix...)
			cursor, err := decodeSegmentCursor(req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Nil(t, cursor)
		})
	}
}

func TestSegmentCursorBindsIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*ListEntriesRequest)
	}{
		{"queue", func(req *ListEntriesRequest) { req.Segment.Queue.SourceCluster = "other-source" }},
		{"business ID", func(req *ListEntriesRequest) { req.Segment.BusinessID = "other-business" }},
		{"run ID", func(req *ListEntriesRequest) { req.Segment.RunID = "other-run" }},
		{"bound", func(req *ListEntriesRequest) { req.InclusiveMaxMessageID++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := testCursorRequest()
			req.NextPageToken = encodeSegmentCursor(req, MessageRef{MessageID: 5, EntryKey: "entry"})
			_, err := decodeSegmentCursor(req)
			require.NoError(t, err)
			tc.change(&req)
			cursor, err := decodeSegmentCursor(req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			require.Nil(t, cursor)
		})
	}
}

func TestSegmentCursorRequiresZeroNumericFields(t *testing.T) {
	t.Parallel()
	for _, missing := range []string{"Bound", "MessageID"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()
			req := testCursorRequest()
			req.InclusiveMaxMessageID = 0
			body := encodeSegmentCursor(req, MessageRef{MessageID: 0, EntryKey: "entry"})
			req.NextPageToken = body
			valid, err := decodeSegmentCursor(req)
			require.NoError(t, err)
			require.Equal(t, int64(0), valid.MessageID)
			var envelope map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &envelope))
			delete(envelope, missing)
			req.NextPageToken, err = json.Marshal(envelope)
			require.NoError(t, err)
			_, err = decodeSegmentCursor(req)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
