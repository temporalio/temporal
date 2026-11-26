package persistence

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

func TestNewDataBlob_DecompressesZstd(t *testing.T) {
	original := bytes.Repeat([]byte("test data "), 500)
	blob := &commonpb.DataBlob{
		Data:         original,
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}

	compressed, encodingStr, err := serialization.CompressHistoryEventBlob(blob)
	require.NoError(t, err)

	result := NewDataBlob(compressed.Data, encodingStr)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, result.EncodingType)
	require.Equal(t, original, result.Data)
}

func TestNewDataBlob_PlainProto3Unchanged(t *testing.T) {
	data := []byte("plain proto3 data")
	result := NewDataBlob(data, enumspb.ENCODING_TYPE_PROTO3.String())
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, result.EncodingType)
	require.Equal(t, data, result.Data)
}

func TestNewDataBlob_CorruptedZstdFallsBack(t *testing.T) {
	result := NewDataBlob([]byte("not zstd"), serialization.EncodingTypeProto3Zstd)
	require.Equal(t, enumspb.ENCODING_TYPE_UNSPECIFIED, result.EncodingType)
}
