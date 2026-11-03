package archiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
)

func TestVisibilityArchivalRecordHash(t *testing.T) {
	testCases := []struct {
		name         string
		record       *archiverspb.VisibilityRecord
		expectedHash string
	}{
		{
			name:         "empty record",
			record:       &archiverspb.VisibilityRecord{},
			expectedHash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name: "record with field",
			record: &archiverspb.VisibilityRecord{
				NamespaceId: "namespace-id",
			},
			expectedHash: "7812eece02c96e6a695f88b1b83aa826aa29e558b33961f411f2e7f572937e94",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recordHash, err := VisibilityArchivalRecordHash(testCase.record)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedHash, recordHash)
		})
	}
}

func TestVisibilityArchivalRecordHashDeterministicForMapOrder(t *testing.T) {
	record1 := visibilityRecordWithMapInsertionOrder([]string{"first", "second"})
	record2 := visibilityRecordWithMapInsertionOrder([]string{"second", "first"})

	recordHash1, err := VisibilityArchivalRecordHash(record1)
	require.NoError(t, err)
	recordHash2, err := VisibilityArchivalRecordHash(record2)
	require.NoError(t, err)

	require.Equal(t, recordHash1, recordHash2)
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
