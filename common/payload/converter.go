package payload

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// This file implements the subset of the Go SDK's default data converter that the server uses
// (nil, []byte, google.golang.org/protobuf messages and JSON values), producing identical payloads.
// The SDK's converter package is not used because it depends on the SDK's gRPC client and every
// API service. Gogo protobuf messages are not supported.

const (
	metadataEncoding    = "encoding"
	metadataMessageType = "messageType"

	encodingNil       = "binary/null"
	encodingBinary    = "binary/plain"
	encodingProtoJSON = "json/protobuf"
	encodingProto     = "binary/protobuf"
	encodingJSON      = "json/plain"
)

var (
	ErrUnableToDecode = errors.New("unable to decode")

	errMetadataIsNotSet             = errors.New("metadata is not set")
	errEncodingIsNotSet             = errors.New("payload encoding metadata is not set")
	errEncodingIsNotSupported       = errors.New("payload encoding is not supported")
	errUnableToEncode               = errors.New("unable to encode")
	errUnableToSetValue             = errors.New("unable to set value")
	errTypeNotImplementProtoMessage = errors.New("type doesn't implement proto.Message")
	errValuePtrIsNotPointer         = errors.New("not a pointer type")
	errValuePtrMustConcreteType     = errors.New("must be a concrete type, not interface")
	errTypeIsNotByteSlice           = errors.New("type is not *[]byte")
)

func toPayload(value any) (*commonpb.Payload, error) {
	if isInterfaceNil(value) {
		return newPayload(nil, encodingNil), nil
	}
	if b, ok := value.([]byte); ok {
		return newPayload(b, encodingBinary), nil
	}
	if m, ok := asProtoMessage(value); ok {
		data, err := protojson.MarshalOptions{}.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errUnableToEncode, err)
		}
		p := newPayload(data, encodingProtoJSON)
		p.Metadata[metadataMessageType] = []byte(m.ProtoReflect().Descriptor().FullName())
		return p, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errUnableToEncode, err)
	}
	return newPayload(data, encodingJSON), nil
}

func fromPayload(p *commonpb.Payload, valuePtr any) error {
	if p == nil {
		return nil
	}
	enc, err := encoding(p)
	if err != nil {
		return err
	}
	switch enc {
	case encodingNil:
		return fromNilPayload(valuePtr)
	case encodingBinary:
		return fromBinaryPayload(p, valuePtr)
	case encodingProtoJSON:
		return fromProtoPayload(p, valuePtr, true, protojson.UnmarshalOptions{}.Unmarshal)
	case encodingProto:
		return fromProtoPayload(p, valuePtr, false, proto.Unmarshal)
	case encodingJSON:
		if err := json.Unmarshal(p.GetData(), valuePtr); err != nil {
			return fmt.Errorf("%w: %v", ErrUnableToDecode, err)
		}
		return nil
	default:
		return fmt.Errorf("encoding %s: %w", enc, errEncodingIsNotSupported)
	}
}

func payloadToString(p *commonpb.Payload) string {
	if p == nil {
		return ""
	}
	enc, err := encoding(p)
	if err != nil {
		return err.Error()
	}
	switch enc {
	case encodingNil:
		return "nil"
	case encodingBinary, encodingProto:
		return base64.RawStdEncoding.EncodeToString(p.GetData())
	case encodingProtoJSON, encodingJSON:
		return string(p.GetData())
	default:
		return fmt.Errorf("encoding %s: %w", enc, errEncodingIsNotSupported).Error()
	}
}

func newPayload(data []byte, encoding string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{metadataEncoding: []byte(encoding)},
		Data:     data,
	}
}

func encoding(p *commonpb.Payload) (string, error) {
	metadata := p.GetMetadata()
	if metadata == nil {
		return "", errMetadataIsNotSet
	}
	if e, ok := metadata[metadataEncoding]; ok {
		return string(e), nil
	}
	return "", errEncodingIsNotSet
}

func asProtoMessage(value any) (proto.Message, bool) {
	if m, ok := value.(proto.Message); ok {
		return m, true
	}
	m, ok := pointerTo(value).Interface().(proto.Message)
	return m, ok
}

func fromNilPayload(valuePtr any) error {
	v, err := settableElem(valuePtr)
	if err != nil {
		return err
	}
	v.Set(reflect.Zero(v.Type()))
	return nil
}

func fromBinaryPayload(p *commonpb.Payload, valuePtr any) error {
	rv := reflect.ValueOf(valuePtr)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("type: %T: %w", valuePtr, errValuePtrIsNotPointer)
	}
	v := rv.Elem()
	switch {
	case v.Kind() == reflect.Interface:
		v.Set(reflect.ValueOf(p.Data))
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		v.SetBytes(p.Data)
	default:
		return fmt.Errorf("type %T: %w", valuePtr, errTypeIsNotByteSlice)
	}
	return nil
}

func fromProtoPayload(
	p *commonpb.Payload,
	valuePtr any,
	jsonNullIsNil bool,
	unmarshal func([]byte, proto.Message) error,
) error {
	originalValue, err := settableElem(valuePtr)
	if err != nil {
		return err
	}
	if jsonNullIsNil && string(p.GetData()) == "null" {
		originalValue.Set(reflect.Zero(originalValue.Type()))
		return nil
	}
	if originalValue.Kind() == reflect.Interface {
		return fmt.Errorf("value type: %s: %w", originalValue.Type().String(), errValuePtrMustConcreteType)
	}
	value := originalValue
	if originalValue.Kind() != reflect.Pointer {
		value = pointerTo(originalValue.Interface())
	}
	if _, ok := value.Interface().(proto.Message); !ok {
		return fmt.Errorf("type: %T: %w", value.Interface(), errTypeNotImplementProtoMessage)
	}
	if originalValue.Kind() == reflect.Pointer && originalValue.IsNil() {
		value = reflect.New(originalValue.Type().Elem())
		originalValue.Set(value)
	}
	err = unmarshal(p.GetData(), value.Interface().(proto.Message))
	if originalValue.Kind() != reflect.Pointer {
		originalValue.Set(value.Elem())
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnableToDecode, err)
	}
	return nil
}

func settableElem(valuePtr any) (reflect.Value, error) {
	v := reflect.ValueOf(valuePtr)
	if v.Kind() != reflect.Pointer {
		return reflect.Value{}, fmt.Errorf("type: %T: %w", valuePtr, errValuePtrIsNotPointer)
	}
	v = v.Elem()
	if !v.CanSet() {
		return reflect.Value{}, fmt.Errorf("type: %T: %w", valuePtr, errUnableToSetValue)
	}
	return v, nil
}

func pointerTo(val any) reflect.Value {
	valPtr := reflect.New(reflect.TypeOf(val))
	valPtr.Elem().Set(reflect.ValueOf(val))
	return valPtr
}

func isInterfaceNil(i any) bool {
	v := reflect.ValueOf(i)
	return i == nil || (v.Kind() == reflect.Pointer && v.IsNil())
}
