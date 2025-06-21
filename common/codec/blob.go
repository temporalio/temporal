package codec

import (
	"errors"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/proto"
)

// ProtoEncodeBlob encodes the given proto message using proto3 encoding and
// returns a DataBlob. The encoding parameter must be ENCODING_TYPE_PROTO3.
func ProtoEncodeBlob(m proto.Message, encoding enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if encoding != enumspb.ENCODING_TYPE_PROTO3 {
		return nil, fmt.Errorf("unknown or unsupported encoding type %v, supported types: %v", encoding.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		return &commonpb.DataBlob{EncodingType: encoding}, nil
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("error serializing using %v encoding: %w", encoding, err)
	}
	return &commonpb.DataBlob{EncodingType: encoding, Data: data}, nil
}

// EncodeBlob encodes the proto message using the provided encoding and returns
// a DataBlob.
func EncodeBlob(o proto.Message, encoding enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if o == nil || (reflect.ValueOf(o).Kind() == reflect.Ptr && reflect.ValueOf(o).IsNil()) {
		return &commonpb.DataBlob{EncodingType: encoding}, nil
	}

	switch encoding {
	case enumspb.ENCODING_TYPE_JSON:
		blob, err := NewJSONPBEncoder().Encode(o)
		if err != nil {
			return nil, err
		}
		return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_JSON, Data: blob}, nil
	case enumspb.ENCODING_TYPE_PROTO3:
		return ProtoEncodeBlob(o, enumspb.ENCODING_TYPE_PROTO3)
	default:
		return nil, fmt.Errorf("unknown or unsupported encoding type %v", encoding.String())
	}
}

// ProtoDecodeBlob decodes a proto3 encoded DataBlob into the provided result.
func ProtoDecodeBlob(data *commonpb.DataBlob, result proto.Message) error {
	if data == nil {
		return errors.New("cannot decode nil")
	}
	if data.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		return fmt.Errorf("unknown or unsupported encoding type %v, supported types: %v", data.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	return proto.Unmarshal(data.Data, result)
}

// DecodeBlob decodes the given DataBlob into the provided result according to
// the blob's encoding type.
func DecodeBlob(data *commonpb.DataBlob, result proto.Message) error {
	if data == nil {
		return errors.New("cannot decode nil")
	}

	if data.Data == nil {
		return nil
	}

	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_JSON:
		return NewJSONPBEncoder().Decode(data.Data, result)
	case enumspb.ENCODING_TYPE_PROTO3:
		return ProtoDecodeBlob(data, result)
	default:
		return fmt.Errorf("unknown or unsupported encoding type %v", data.EncodingType.String())
	}
}
