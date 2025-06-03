package serialization

import (
	"errors"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
)

// EncodingEnvVar is the environment variable that selects the persistence encoding.
const EncodingEnvVar = "TEMPORAL_SERIALIZER_ENCODING"

// EncodingHelper provides encode/decode helpers for persistence payloads.
type EncodingHelper interface {
	Encode(proto.Message) (*commonpb.DataBlob, error)
	Decode(*commonpb.DataBlob, proto.Message) error
	EncodingType() enumspb.EncodingType
}

type proto3EncodingHelper struct{}

type jsonEncodingHelper struct{}

func NewProto3EncodingHelper() EncodingHelper { return proto3EncodingHelper{} }
func NewJSONEncodingHelper() EncodingHelper   { return jsonEncodingHelper{} }

func (proto3EncodingHelper) EncodingType() enumspb.EncodingType { return enumspb.ENCODING_TYPE_PROTO3 }
func (jsonEncodingHelper) EncodingType() enumspb.EncodingType   { return enumspb.ENCODING_TYPE_JSON }

func (proto3EncodingHelper) Encode(m proto.Message) (*commonpb.DataBlob, error) {
	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3}, nil
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{Data: data, EncodingType: enumspb.ENCODING_TYPE_PROTO3}, nil
}

func (proto3EncodingHelper) Decode(blob *commonpb.DataBlob, m proto.Message) error {
	if blob == nil {
		return errors.New("cannot decode nil")
	}
	if blob.EncodingType != enumspb.ENCODING_TYPE_PROTO3 {
		return NewUnknownEncodingTypeError(blob.EncodingType.String(), enumspb.ENCODING_TYPE_PROTO3)
	}
	if blob.Data == nil {
		return nil
	}
	return proto.Unmarshal(blob.Data, m)
}

func (jsonEncodingHelper) Encode(m proto.Message) (*commonpb.DataBlob, error) {
	if m == nil || (reflect.ValueOf(m).Kind() == reflect.Ptr && reflect.ValueOf(m).IsNil()) {
		return &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_JSON}, nil
	}
	data, err := codec.NewJSONPBEncoder().Encode(m)
	if err != nil {
		return nil, err
	}
	return &commonpb.DataBlob{Data: data, EncodingType: enumspb.ENCODING_TYPE_JSON}, nil
}

func (jsonEncodingHelper) Decode(blob *commonpb.DataBlob, m proto.Message) error {
	if blob == nil {
		return errors.New("cannot decode nil")
	}
	if blob.EncodingType != enumspb.ENCODING_TYPE_JSON {
		return NewUnknownEncodingTypeError(blob.EncodingType.String(), enumspb.ENCODING_TYPE_JSON)
	}
	if blob.Data == nil {
		return nil
	}
	return codec.NewJSONPBEncoder().Decode(blob.Data, m)
}
