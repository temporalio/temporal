package codec

import (
	"errors"
	"fmt"
	"os"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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

// EncodingTypeFromEnv returns an EncodingType based on the environment variable `TEMPORAL_TEST_DATA_ENCODING`.
// It defaults to "ENCODING_TYPE_PROTO3" codec if the environment variable is not set.
func EncodingTypeFromEnv() enumspb.EncodingType {
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
	// SerializationError is an error type for serialization
	SerializationError struct {
		encodingType enumspb.EncodingType
		wrappedErr   error
	}

	// DeserializationError is an error type for deserialization
	DeserializationError struct {
		encodingType enumspb.EncodingType
		wrappedErr   error
	}

	// UnknownEncodingTypeError is an error type for unknown or unsupported encoding type
	UnknownEncodingTypeError struct {
		providedType        string
		expectedEncodingStr []string
	}

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
	return EncodeBlob(m, EncodingTypeFromEnv(), options...)
}

func EncodeBlob(
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
		blob, err := NewJSONPBEncoder().Encode(m)
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
		return NewJSONPBEncoder().Decode(data.Data, result)
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

// NewUnknownEncodingTypeError returns a new instance of encoding type error
func NewUnknownEncodingTypeError(
	providedType string,
	expectedEncoding ...enumspb.EncodingType,
) error {
	if len(expectedEncoding) == 0 {
		for encodingType := range enumspb.EncodingType_name {
			expectedEncoding = append(expectedEncoding, enumspb.EncodingType(encodingType))
		}
	}
	expectedEncodingStr := make([]string, 0, len(expectedEncoding))
	for _, encodingType := range expectedEncoding {
		expectedEncodingStr = append(expectedEncodingStr, encodingType.String())
	}
	return &UnknownEncodingTypeError{
		providedType:        providedType,
		expectedEncodingStr: expectedEncodingStr,
	}
}

func (e *UnknownEncodingTypeError) Error() string {
	return fmt.Sprintf("unknown or unsupported encoding type %v, supported types: %v",
		e.providedType,
		strings.Join(e.expectedEncodingStr, ","),
	)
}

// IsTerminalTaskError informs our task processing subsystem that it is impossible
// to retry this error
func (e *UnknownEncodingTypeError) IsTerminalTaskError() bool { return true }

// NewSerializationError returns a SerializationError
func NewSerializationError(
	encodingType enumspb.EncodingType,
	serializationErr error,
) error {
	return &SerializationError{
		encodingType: encodingType,
		wrappedErr:   serializationErr,
	}
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("error serializing using %v encoding: %v", e.encodingType, e.wrappedErr)
}

func (e *SerializationError) Unwrap() error {
	return e.wrappedErr
}

// NewDeserializationError returns a DeserializationError
func NewDeserializationError(
	encodingType enumspb.EncodingType,
	deserializationErr error,
) error {
	return &DeserializationError{
		encodingType: encodingType,
		wrappedErr:   deserializationErr,
	}
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("error deserializing using %v encoding: %v", e.encodingType, e.wrappedErr)
}

func (e *DeserializationError) Unwrap() error {
	return e.wrappedErr
}

// IsTerminalTaskError informs our task processing subsystem that it is impossible to
// retry this error and that the task should be sent to a DLQ
func (e *DeserializationError) IsTerminalTaskError() bool { return true }
