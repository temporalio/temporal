package serialization

import (
	"fmt"
	"strings"

	"github.com/klauspost/compress/zstd"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	// zstdSuffix is appended to the base encoding type to indicate zstd compression.
	// Example: "Proto3" -> "Proto3+zstd", "Json" -> "Json+zstd".
	zstdSuffix = "+zstd"

	// EncodingTypeProto3Zstd is the canonical encoding string for compressed proto3 blobs.
	// Kept as a constant for backward compatibility and common-case optimization.
	EncodingTypeProto3Zstd = "Proto3+zstd"

	// MaxDecompressedSize is the safety limit in bytes for decompressed output.
	// Blobs larger than this are not eligible for compression, and the decoder
	// will refuse to produce output exceeding this size.
	MaxDecompressedSize = 16 * 1024 * 1024
)

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic("failed to initialize zstd encoder: " + err.Error()) //nolint:forbidigo // must fail fast on broken build
	}
	zstdDecoder, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxDecompressedSize))
	if err != nil {
		panic("failed to initialize zstd decoder: " + err.Error()) //nolint:forbidigo // must fail fast on broken build
	}
}

// IsCompressedEncoding returns true if the encoding string indicates zstd compression
// (e.g. "Proto3+zstd", "Json+zstd").
func IsCompressedEncoding(encoding string) bool {
	return len(encoding) > len(zstdSuffix) && strings.HasSuffix(strings.ToLower(encoding), zstdSuffix)
}

// CompressHistoryEventBlob compresses the data in a DataBlob using zstd.
// Returns the original blob unchanged if compression doesn't reduce size or if
// the blob exceeds MaxDecompressedSize (would fail to decompress later).
func CompressHistoryEventBlob(blob *commonpb.DataBlob) (*commonpb.DataBlob, string, error) {
	if blob == nil {
		return nil, "", nil
	}

	// Blobs at or above the decoder limit cannot be decompressed later.
	if len(blob.Data) >= MaxDecompressedSize {
		return blob, "", nil
	}

	compressed := zstdEncoder.EncodeAll(blob.Data, make([]byte, 0, len(blob.Data)/2))
	// If compression didn't reduce size, store uncompressed to avoid overhead on read.
	if len(compressed) >= len(blob.Data) {
		return blob, "", nil
	}

	encodingStr := blob.EncodingType.String() + zstdSuffix
	return &commonpb.DataBlob{
		Data:         compressed,
		EncodingType: blob.EncodingType,
	}, encodingStr, nil
}

// DecompressHistoryEventBlob decompresses a zstd-compressed DataBlob back to its
// original encoding. The base encoding type is parsed from the encoding string
// (e.g. "Proto3+zstd" -> ENCODING_TYPE_PROTO3). If the encoding is not compressed,
// it returns the blob unchanged.
func DecompressHistoryEventBlob(data []byte, encodingStr string) (*commonpb.DataBlob, error) {
	if !IsCompressedEncoding(encodingStr) {
		encodingType, err := enumspb.EncodingTypeFromString(encodingStr)
		if err != nil {
			encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
		}
		return &commonpb.DataBlob{
			Data:         data,
			EncodingType: encodingType,
		}, nil
	}

	decompressed, err := zstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress history node blob (encoding=%q, size=%d): %w", encodingStr, len(data), err)
	}

	// Parse base encoding from the marker (strip "+zstd" suffix).
	baseEncodingStr := encodingStr[:len(encodingStr)-len(zstdSuffix)]
	baseEncoding, err := enumspb.EncodingTypeFromString(baseEncodingStr)
	if err != nil {
		baseEncoding = enumspb.ENCODING_TYPE_PROTO3
	}

	return &commonpb.DataBlob{
		Data:         decompressed,
		EncodingType: baseEncoding,
	}, nil
}
