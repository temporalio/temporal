package serialization

import (
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
)

func ProtoEncode(m proto.Message) (*commonpb.DataBlob, error) {
	return encodeBlob(m, enumspb.ENCODING_TYPE_PROTO3)
}

func encodeBlob(m proto.Message, encoding enumspb.EncodingType) (*commonpb.DataBlob, error) {
	if m == nil {
		return &commonpb.DataBlob{
			Data:         nil,
			EncodingType: encoding,
		}, nil
	}

	switch encoding {
	case enumspb.ENCODING_TYPE_JSON:
		blob, err := codec.NewJSONPBEncoder().Encode(m)
		if err != nil {
			return nil, err
		}
		return &commonpb.DataBlob{
			Data:         blob,
			EncodingType: enumspb.ENCODING_TYPE_JSON,
		}, nil
	case enumspb.ENCODING_TYPE_PROTO3:
		data, err := proto.Marshal(m)
		if err != nil {
			return nil, NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
		}
		return &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         data,
		}, nil
	default:
		return nil, NewUnknownEncodingTypeError(encoding.String(), enumspb.ENCODING_TYPE_JSON, enumspb.ENCODING_TYPE_PROTO3)
	}
}

func Decode(data *commonpb.DataBlob, result proto.Message) error {
	if data == nil {
		return NewDeserializationError(enumspb.ENCODING_TYPE_UNSPECIFIED, errors.New("cannot decode nil"))
	}

	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_JSON:
		return codec.NewJSONPBEncoder().Decode(data.Data, result)
	case enumspb.ENCODING_TYPE_PROTO3:
		err := proto.Unmarshal(data.Data, result)
		if err != nil {
			return NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
		}
		return nil
	default:
		return NewUnknownEncodingTypeError(data.EncodingType.String(), enumspb.ENCODING_TYPE_JSON, enumspb.ENCODING_TYPE_PROTO3)
	}
}
