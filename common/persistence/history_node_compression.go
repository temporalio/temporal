package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	// minCompressionSize is the absolute minimum blob size in bytes worth compressing.
	// Below this, compression framing overhead negates any savings.
	minCompressionSize = 1024
)

// HistoryNodeEncodingString returns the encoding string to persist for a history node.
// It uses EncodingOverride if set, otherwise falls back to the DataBlob's EncodingType.
func HistoryNodeEncodingString(node InternalHistoryNode) string {
	if node.EncodingOverride != "" {
		return node.EncodingOverride
	}
	return node.Events.EncodingType.String()
}

// maybeCompressHistoryBlob compresses the blob if it exceeds the configured threshold.
func (m *executionManagerImpl) maybeCompressHistoryBlob(blob *commonpb.DataBlob) (*commonpb.DataBlob, string, error) {
	threshold := m.historyNodeBlobCompressionThreshold()
	if threshold <= 0 || len(blob.Data) < max(threshold, minCompressionSize) {
		return blob, "", nil
	}

	compressed, encodingStr, err := serialization.CompressHistoryEventBlob(blob)
	if err != nil {
		return nil, "", err
	}
	return compressed, encodingStr, nil
}
