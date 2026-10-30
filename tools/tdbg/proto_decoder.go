package tdbg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// decodePayloadsInJSON parses JSON data, finds Payload objects, and makes them
// human-readable: metadata bytes are decoded from base64 to strings, and data
// is decoded based on the encoding (binary/protobuf, json/plain, etc.).
// If data decoding fails, the data field is left as its original base64 string.
func decodePayloadsInJSON(data []byte) []byte {
	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return data
	}
	encoder := codec.NewJSONPBEncoder()
	decodePayloadsRecursive(parsed, encoder)
	result, err := json.Marshal(parsed)
	if err != nil {
		return data
	}
	return result
}

func decodePayloadsRecursive(v any, encoder codec.JSONPBEncoder) {
	switch val := v.(type) {
	case map[string]any:
		tryDecodePayloadJSON(val, encoder)
		for _, child := range val {
			decodePayloadsRecursive(child, encoder)
		}
	case []any:
		for _, item := range val {
			decodePayloadsRecursive(item, encoder)
		}
	default:
		// Other types don't need decoding.
	}
}

func tryDecodePayloadJSON(obj map[string]any, encoder codec.JSONPBEncoder) {
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return
	}

	// Decode all metadata bytes values from base64 to strings.
	for key, val := range metadata {
		b64, ok := val.(string)
		if !ok {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			continue
		}
		metadata[key] = string(decoded)
	}

	encoding, ok := metadata["encoding"].(string)
	if !ok {
		encoding = ""
	}
	messageType, ok := metadata["messageType"].(string)
	if !ok {
		messageType = ""
	}

	dataB64, ok := obj["data"].(string)
	if !ok {
		return
	}

	// For binary/protobuf with a known messageType, unmarshal and re-encode as JSON.
	if messageType != "" && encoding == "binary/protobuf" {
		dataBytes, err := base64.StdEncoding.DecodeString(dataB64)
		if err != nil {
			return
		}
		msg, err := unmarshalProtoByTypeName(messageType, dataBytes)
		if err != nil {
			return
		}
		jsonBytes, err := encoder.Encode(msg)
		if err != nil {
			return
		}
		var decoded any
		if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
			return
		}
		obj["data"] = decoded
		return
	}

	// For JSON-based encodings, base64-decode and parse as JSON.
	if strings.HasPrefix(encoding, "json/") {
		dataBytes, err := base64.StdEncoding.DecodeString(dataB64)
		if err != nil {
			return
		}
		var decoded any
		if err := json.Unmarshal(dataBytes, &decoded); err != nil {
			return
		}
		obj["data"] = decoded
	}
}

// unmarshalProtoByTypeName resolves a proto type by its full name from the
// global registry and unmarshals the given bytes into it.
func unmarshalProtoByTypeName(typeName string, data []byte) (proto.Message, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeName))
	if err != nil {
		return nil, fmt.Errorf("unable to find %s type: %w", typeName, err)
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("unable to unmarshal to %s: %w", typeName, err)
	}
	return msg, nil
}
