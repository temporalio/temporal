package serialization

import "go.temporal.io/server/common/codec"

// Deprecated: these moved to common/codec. They remain here so that code outside this repository
// that uses them keeps building.
type (
	EncodeOption             = codec.EncodeOption
	SerializationError       = codec.SerializationError
	DeserializationError     = codec.DeserializationError
	UnknownEncodingTypeError = codec.UnknownEncodingTypeError
)

// Deprecated: these moved to common/codec. They remain here so that code outside this repository
// that uses them keeps building.
var (
	WithDeterministicProto3     = codec.WithDeterministicProto3
	Encode                      = codec.Encode
	Decode                      = codec.Decode
	NewSerializationError       = codec.NewSerializationError
	NewDeserializationError     = codec.NewDeserializationError
	NewUnknownEncodingTypeError = codec.NewUnknownEncodingTypeError
)
