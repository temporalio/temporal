package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/codec"
)

func TestVisibilityArchivalRecordHash(t *testing.T) {
	require.Equal(
		t,
		"44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
		VisibilityArchivalRecordHash([]byte("{}")),
	)
}

func TestVisibilityArchivalRecordHashDeterministicForMapOrder(t *testing.T) {
	record1 := visibilityRecordWithMapInsertionOrder([]string{"first", "second"})
	record2 := visibilityRecordWithMapInsertionOrder([]string{"second", "first"})
	encoder := codec.NewJSONPBEncoder()

	encodedRecord1, err := encoder.Encode(record1)
	require.NoError(t, err)
	encodedRecord2, err := encoder.Encode(record2)
	require.NoError(t, err)

	require.Equal(t, encodedRecord1, encodedRecord2)
	require.Equal(t, VisibilityArchivalRecordHash(encodedRecord1), VisibilityArchivalRecordHash(encodedRecord2))
}

func visibilityRecordWithMapInsertionOrder(keys []string) *archiverspb.VisibilityRecord {
	searchAttributes := make(map[string]string, len(keys))
	memoFields := make(map[string]*commonpb.Payload, len(keys))
	for _, key := range keys {
		metadata := make(map[string][]byte, len(keys))
		for _, metadataKey := range keys {
			metadata[metadataKey] = []byte(metadataKey)
		}
		searchAttributes[key] = key
		memoFields[key] = &commonpb.Payload{
			Metadata: metadata,
			Data:     []byte(key),
		}
	}

	return &archiverspb.VisibilityRecord{
		Memo: &commonpb.Memo{
			Fields: memoFields,
		},
		SearchAttributes: searchAttributes,
	}
}
