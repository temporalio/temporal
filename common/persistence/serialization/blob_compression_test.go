package serialization

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestCompressDecompressRoundTrip(t *testing.T) {
	original := bytes.Repeat([]byte("hello world "), 200)
	blob := &commonpb.DataBlob{
		Data:         original,
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}

	compressed, encodingStr, err := CompressHistoryEventBlob(blob)
	require.NoError(t, err)
	require.Equal(t, EncodingTypeProto3Zstd, encodingStr)
	require.Less(t, len(compressed.Data), len(original))

	decompressed, err := DecompressHistoryEventBlob(compressed.Data, encodingStr)
	require.NoError(t, err)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, decompressed.EncodingType)
	require.Equal(t, original, decompressed.Data)
}

func TestCompressHistoryEventBlob_NilBlob(t *testing.T) {
	result, encodingStr, err := CompressHistoryEventBlob(nil)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Empty(t, encodingStr)
}

func TestCompressHistoryEventBlob_EmptyData(t *testing.T) {
	blob := &commonpb.DataBlob{
		Data:         []byte{},
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}
	result, encodingStr, err := CompressHistoryEventBlob(blob)
	require.NoError(t, err)
	// Empty data: compressed will be larger due to zstd framing, so original returned.
	require.Same(t, blob, result)
	require.Empty(t, encodingStr)
}

func TestDecompressHistoryEventBlob_CorruptedData(t *testing.T) {
	_, err := DecompressHistoryEventBlob([]byte("not zstd data"), EncodingTypeProto3Zstd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decompress")
}

func TestCompressHistoryEventBlob_IncompressibleData(t *testing.T) {
	// Use already-compressed data to guarantee incompressibility:
	// zstd output has maximum entropy and cannot be compressed further.
	randomSource := bytes.Repeat([]byte("ab"), 1024)
	preCompressed := zstdEncoder.EncodeAll(randomSource, nil)

	blob := &commonpb.DataBlob{
		Data:         preCompressed,
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}

	result, encodingStr, err := CompressHistoryEventBlob(blob)
	require.NoError(t, err)
	// Should return original blob unchanged since compression doesn't help.
	require.Same(t, blob, result)
	require.Empty(t, encodingStr)
}

func TestDecompressHistoryEventBlob_PlainEncoding(t *testing.T) {
	data := []byte("plain proto3 data")
	blob, err := DecompressHistoryEventBlob(data, enumspb.ENCODING_TYPE_PROTO3.String())
	require.NoError(t, err)
	require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.EncodingType)
	require.Equal(t, data, blob.Data)
}

func TestIsCompressedEncoding(t *testing.T) {
	require.True(t, IsCompressedEncoding(EncodingTypeProto3Zstd))
	require.True(t, IsCompressedEncoding("PROTO3+ZSTD"))
	require.True(t, IsCompressedEncoding("proto3+zstd"))
	require.True(t, IsCompressedEncoding("Json+zstd"))
	require.False(t, IsCompressedEncoding("proto3"))
	require.False(t, IsCompressedEncoding("json"))
	require.False(t, IsCompressedEncoding(""))
	require.False(t, IsCompressedEncoding("+zstd"))
}

func TestCompressDecompressRoundTrip_JSON(t *testing.T) {
	original := bytes.Repeat([]byte(`{"event":"data"}`), 200)
	blob := &commonpb.DataBlob{
		Data:         original,
		EncodingType: enumspb.ENCODING_TYPE_JSON,
	}

	compressed, encodingStr, err := CompressHistoryEventBlob(blob)
	require.NoError(t, err)
	require.Equal(t, "Json+zstd", encodingStr)
	require.Less(t, len(compressed.Data), len(original))

	decompressed, err := DecompressHistoryEventBlob(compressed.Data, encodingStr)
	require.NoError(t, err)
	require.Equal(t, enumspb.ENCODING_TYPE_JSON, decompressed.EncodingType)
	require.Equal(t, original, decompressed.Data)
}

func TestCompressHistoryEventBlob_ExceedsMaxDecompressedSize(t *testing.T) {
	// Blob at the limit should be returned unchanged (not compressed).
	blob := &commonpb.DataBlob{
		Data:         make([]byte, MaxDecompressedSize),
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	}

	result, encodingStr, err := CompressHistoryEventBlob(blob)
	require.NoError(t, err)
	require.Same(t, blob, result)
	require.Empty(t, encodingStr)
}
