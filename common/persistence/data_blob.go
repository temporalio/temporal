package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// NewDataBlob returns a new DataBlob.
// TODO: return an UnknowEncodingType error with the actual type string when encodingTypeStr is invalid
func NewDataBlob(data []byte, encodingTypeStr string) *commonpb.DataBlob {
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
