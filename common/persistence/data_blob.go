package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

// NewDataBlob returns a new DataBlob. If the encoding indicates zstd compression,
// the data is transparently decompressed.
// TODO: return an UnknowEncodingType error with the actual type string when encodingTypeStr is invalid
func NewDataBlob(data []byte, encodingTypeStr string) *commonpb.DataBlob {
	if serialization.IsCompressedEncoding(encodingTypeStr) {
		blob, err := serialization.DecompressHistoryEventBlob(data, encodingTypeStr)
		if err != nil {
			// Decompression failed — return raw data with UNSPECIFIED encoding.
			// The downstream deserializer will produce an actionable error including
			// the context from DecompressHistoryEventBlob (encoding type, blob size).
			return &commonpb.DataBlob{
				Data:         data,
				EncodingType: enumspb.ENCODING_TYPE_UNSPECIFIED,
			}
		}
		return blob
	}

	encodingType, err := enumspb.EncodingTypeFromString(encodingTypeStr)
	if err != nil {
		// encodingTypeStr not valid, an error will be returned on deserialization
		encodingType = enumspb.ENCODING_TYPE_UNSPECIFIED
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: encodingType,
	}
}
