package serialization

import (
	"errors"
	"fmt"
	"os"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
)

// SerializerDataEncodingEnvVar controls which codec is used for encoding DataBlobs.
//
// Currently supported values (case-insensitive):
//   - "json"
//   - "proto3"
//
// Decoding always support all encodings regardless of this setting.
//
// WARNING: This environment variable should only be used for testing; and never set it in production.
const SerializerDataEncodingEnvVar = "TEMPORAL_TEST_DATA_ENCODING"

// encodingTypeFromEnv returns an EncodingType based on the environment variable `TEMPORAL_TEST_DATA_ENCODING`.
// It defaults to "ENCODING_TYPE_PROTO3" codec if the environment variable is not set.
func encodingTypeFromEnv() enumspb.EncodingType {
	codecType := os.Getenv(SerializerDataEncodingEnvVar)
	switch strings.ToLower(codecType) {
	case "", "proto3":
		return enumspb.ENCODING_TYPE_PROTO3
	case "json":
		return enumspb.ENCODING_TYPE_JSON
	default:
		//nolint:forbidigo // should fail fast and hard if used incorrectly
		panic(fmt.Sprintf("unknown codec %q for environment variable %s", codecType, SerializerDataEncodingEnvVar))
	}
}

type (
	encodeOptions struct {
		deterministic bool
	}
	EncodeOption func(*encodeOptions)
)

// WithDeterministicProto3 uses deterministic marshaling when Encode selects proto3.
//
// Deterministic encoding sorts map keys before encoding, making byte comparison
// a reliable equality check for any well-formed proto message. For messages
// without map fields this is a no-op with no performance overhead.
var WithDeterministicProto3 EncodeOption = func(opts *encodeOptions) {
	opts.deterministic = true
}

// Encode encodes the given proto message. It respects the `TEMPORAL_TEST_DATA_ENCODING` environment variable;
// otherwise, it defaults to "ENCODING_TYPE_PROTO3".
func Encode(m proto.Message, options ...EncodeOption) (*commonpb.DataBlob, error) {
	return encodeBlob(m, encodingTypeFromEnv(), options...)
}

func encodeBlob(
	m proto.Message,
	encoding enumspb.EncodingType,
	options ...EncodeOption,
) (*commonpb.DataBlob, error) {
	opts := encodeOptions{}
	for _, option := range options {
		option(&opts)
	}

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
		data, err := proto.MarshalOptions{Deterministic: opts.deterministic}.Marshal(m)
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
