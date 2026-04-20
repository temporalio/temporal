package sadefs

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	MetadataType = "type"
)

func GetMetadataType(p *commonpb.Payload) enumspb.IndexedValueType {
	t, err := enumspb.IndexedValueTypeFromString(string(p.Metadata[MetadataType]))
	if err != nil {
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}
	return t
}

func SetMetadataType(p *commonpb.Payload, t enumspb.IndexedValueType) {
	if t == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
		return
	}

	_, isValidT := enumspb.IndexedValueType_name[int32(t)]
	if !isValidT {
		// nolint: forbidigo
		panic(fmt.Sprintf("unknown index value type %v", t))
	}
	p.Metadata[MetadataType] = []byte(t.String())
}
