package sdk

import (
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

var (
	// PreferProtoDataConverter is like the default data converter defined in the SDK, except
	// that it prefers encoding proto messages with the binary encoding instead of json.
	PreferProtoDataConverter = converter.NewCompositeDataConverter(
		converter.NewNilPayloadConverter(),
		converter.NewByteSlicePayloadConverter(),
		converter.NewProtoPayloadConverter(),
		converter.NewProtoJSONPayloadConverter(),
		converter.NewJSONPayloadConverter(),
	)
)

// LenientFromPayloadProtoConverter decodes a payload to a proto message, discarding unknown fields.
// This is useful for forward compatibility when older code needs to decode payloads
// that may have been written by newer code with additional fields.
func LenientFromPayloadProtoConverter(payload *commonpb.Payload, valuePtr proto.Message) error {
	if payload == nil {
		return nil
	}

	encoding := string(payload.GetMetadata()["encoding"])
	switch encoding {
	case "binary/protobuf":
		return proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(payload.GetData(), valuePtr)
	case "json/protobuf":
		return temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}.Unmarshal(payload.GetData(), valuePtr)
	default:
		return errors.New("unable to decode: unsupported encoding type: " + encoding)
	}
}
