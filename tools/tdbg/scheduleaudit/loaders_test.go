package scheduleaudit

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

// TestDecodePayloads covers the skip-on-failure contract of decodeScheduledByID and decodeNominalStartTime so the
// caller can drop visibility rows with malformed search-attribute payloads instead of grouping them under empty keys.
func TestDecodePayloads(t *testing.T) {
	makePayload := func(encoding string, data []byte) *commonpb.Payload {
		return &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(encoding)},
			Data:     data,
		}
	}

	t.Run("decodeScheduledByID nil payload returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(nil)
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID wrong encoding returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID malformed json returns false", func(t *testing.T) {
		_, ok := decodeScheduledByID(makePayload("json/plain", []byte("not-json-quoted")))
		require.False(t, ok)
	})
	t.Run("decodeScheduledByID valid json returns the string", func(t *testing.T) {
		actual, ok := decodeScheduledByID(makePayload("json/plain", []byte(`"my-schedule-id"`)))
		require.True(t, ok)
		require.Equal(t, "my-schedule-id", actual)
	})

	t.Run("decodeNominalStartTime nil payload returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(nil)
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime wrong encoding returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("proto/binary", []byte("anything")))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime non-RFC3339 returns zero", func(t *testing.T) {
		_, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"not-a-timestamp"`)))
		require.False(t, ok)
	})
	t.Run("decodeNominalStartTime valid RFC3339 parses", func(t *testing.T) {
		actual, ok := decodeNominalStartTime(makePayload("json/plain", []byte(`"2026-05-19T18:00:00Z"`)))
		require.True(t, ok)
		require.Equal(t, mustParseTime("2026-05-19T18:00:00Z"), actual)
	})
}
