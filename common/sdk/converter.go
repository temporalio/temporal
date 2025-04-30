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
)
