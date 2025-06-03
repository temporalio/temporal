package serialization

import (
	"errors"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
)

// CodecEnvVar is the environment variable that selects the persistence encoding.
const CodecEnvVar = "TEMPORAL_SERIALIZER_CODEC"

// Codec encodes/decodes persistence payloads.
type Codec interface {
	Encode(proto.Message) (*commonpb.DataBlob, error)
	Decode(*commonpb.DataBlob, proto.Message) error
	EncodingType() enumspb.EncodingType
}

type proto3Codec struct{}

type jsonCodec struct{}

func NewProto3Codec() Codec { return proto3Codec{} }
func NewJsonCodec() Codec   { return jsonCodec{} }

func (proto3Codec) EncodingType() enumspb.EncodingType { return enumspb.ENCODING_TYPE_PROTO3 }
func (jsonCodec) EncodingType() enumspb.EncodingType   { return enumspb.ENCODING_TYPE_JSON }

func (proto3Codec) Encode(m proto.Message) (*commonpb.DataBlob, error) {
	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}, nil
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{Data: data, EncodingType: enumspb.ENCODING_TYPE_PROTO3}, nil
}

func (proto3Codec) Decode(blob *commonpb.DataBlob, m proto.Message) error {
	if blob == nil {
		// TODO: should we return nil or error?
		return NewDeserializationError(enumspb.ENCODING_TYPE_UNSPECIFIED, errors.New("cannot decode nil"))
	}
	if blob.Data == nil {
		return nil
	}
	if blob.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		return NewUnknownEncodingTypeError(blob.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	return proto.Unmarshal(blob.Data, m)
}

func (jsonCodec) Encode(m proto.Message) (*commonpb.DataBlob, error) {
	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_JSON}, nil
	}
	data, err := codec.NewJSONPBEncoder().Encode(m)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{Data: data, EncodingType: enumspb.ENCODING_TYPE_JSON}, nil
}

func (jsonCodec) Decode(blob *commonpb.DataBlob, m proto.Message) error {
	if blob == nil {
		// TODO: should we return nil or error?
		return NewDeserializationError(enumspb.ENCODING_TYPE_UNSPECIFIED, errors.New("cannot decode nil"))
	}
	if blob.Data == nil {
		return nil
	}
	if blob.EncodingType != enumspb.ENCODING_TYPE_JSON {
		return NewUnknownEncodingTypeError(blob.EncodingType.String(), enumspb.ENCODING_TYPE_JSON)
	}
	return codec.NewJSONPBEncoder().Decode(blob.Data, m)
}
