package persistence

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

// NewDataBlob returns a new DataBlob
func NewDataBlob(data []byte, encodingTypeStr string) *commonpb.DataBlob {
	if len(data) == 0 {
		return nil
	}

	encodingType, ok := enumspb.EncodingType_value[encodingTypeStr]
	if !ok || (enumspb.EncodingType(encodingType) != enumspb.ENCODING_TYPE_PROTO3 &&
		enumspb.EncodingType(encodingType) != enumspb.ENCODING_TYPE_JSON) {
		panic(fmt.Sprintf("Invalid encoding: \"%v\"", encodingTypeStr))
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: enumspb.EncodingType(encodingType),
	}
}
