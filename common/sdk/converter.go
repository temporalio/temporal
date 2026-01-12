package sdk

import (
	"go.temporal.io/sdk/converter"
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

	// PreferProtoDataConverterAllowUnknownJSONFields is like the default data converter defined in the SDK, except
	// that it prefers encoding proto messages with the binary encoding instead of json, and, if decoding with json,
	// allows unknown fields.
	PreferProtoDataConverterAllowUnknownJSONFields = converter.NewCompositeDataConverter(
		converter.NewNilPayloadConverter(),
		converter.NewByteSlicePayloadConverter(),
		converter.NewProtoPayloadConverter(),
		converter.NewProtoJSONPayloadConverterWithOptions(converter.ProtoJSONPayloadConverterOptions{AllowUnknownFields: true}),
		converter.NewProtoJSONPayloadConverterWithOptions(converter.ProtoJSONPayloadConverterOptions{AllowUnknownFields: true}),
	)
)
