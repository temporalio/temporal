// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sdk

import (
	"encoding/base64"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"

	"go.temporal.io/server/common/utf8validator"
)

var (
	// PreferProtoDataConverter is like the default data converter defined in the SDK, except
	// that it prefers encoding proto messages with the binary encoding instead of json.
	PreferProtoDataConverter = converter.NewCompositeDataConverter(
		converter.NewNilPayloadConverter(),
		converter.NewByteSlicePayloadConverter(),
		// TODO: We have a local copy of ProtoPayloadConverter to add explicit utf8 validation
		// checks. After we remove this explicit validation, we can switch back to the sdk version.
		// converter.NewProtoPayloadConverter(),
		newProtoPayloadConverter(),
		converter.NewProtoJSONPayloadConverter(),
		converter.NewJSONPayloadConverter(),
	)
)

// Local copy of go.temporal.io/sdk/converter.ProtoPayloadConverter with utf8 validation checks
// and without various options:

type protoPayloadConverter struct {
}

// NewProtoPayloadConverter creates new instance of `ProtoPayloadConverter“.
func newProtoPayloadConverter() *protoPayloadConverter {
	return &protoPayloadConverter{}
}

func (c *protoPayloadConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	// This implementation only supports protobuf api v2.

	builtPointer := false
	for {
		if valueProto, ok := value.(proto.Message); ok {
			err := utf8validator.Validate(valueProto, utf8validator.SourcePersistence)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", converter.ErrUnableToEncode, err)
			}
			byteSlice, err := proto.Marshal(valueProto)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", converter.ErrUnableToEncode, err)
			}
			return &commonpb.Payload{
				Metadata: map[string][]byte{
					converter.MetadataEncoding:    []byte(c.Encoding()),
					converter.MetadataMessageType: []byte(valueProto.ProtoReflect().Descriptor().FullName()),
				},
				Data: byteSlice,
			}, nil
		}
		if builtPointer {
			break
		}
		value = pointerTo(value).Interface()
		builtPointer = true
	}

	return nil, nil
}

func (c *protoPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	// This implementation only supports protobuf api v2.

	originalValue := reflect.ValueOf(valuePtr)
	if originalValue.Kind() != reflect.Ptr {
		return fmt.Errorf("type: %T: %w", valuePtr, converter.ErrValuePtrIsNotPointer)
	}

	originalValue = originalValue.Elem()
	if !originalValue.CanSet() {
		return fmt.Errorf("type: %T: %w", valuePtr, converter.ErrUnableToSetValue)
	}

	if originalValue.Kind() == reflect.Interface {
		return fmt.Errorf("value type: %s: %w", originalValue.Type().String(), converter.ErrValuePtrMustConcreteType)
	}

	value := originalValue
	// If original value is of value type (i.e. commonpb.WorkflowType), create a pointer to it.
	if originalValue.Kind() != reflect.Ptr {
		value = pointerTo(originalValue.Interface())
	}

	protoValue := value.Interface() // protoValue is for sure of pointer type (i.e. *commonpb.WorkflowType).
	protoMessage, isProtoMessage := protoValue.(proto.Message)
	if !isProtoMessage {
		return fmt.Errorf("type: %T: %w", protoValue, converter.ErrTypeNotImplementProtoMessage)
	}

	// If original value is nil, create new instance.
	if originalValue.Kind() == reflect.Ptr && originalValue.IsNil() {
		value = newOfSameType(originalValue)
		protoValue = value.Interface()
		protoMessage = protoValue.(proto.Message) // type assertion must always succeed
	}

	err := proto.Unmarshal(payload.GetData(), protoMessage)
	if err == nil {
		err = utf8validator.Validate(protoMessage, utf8validator.SourcePersistence)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", converter.ErrUnableToDecode, err)
	}

	// If original value wasn't a pointer then set value back to where valuePtr points to.
	if originalValue.Kind() != reflect.Ptr {
		originalValue.Set(value.Elem())
	}

	return nil
}

func (c *protoPayloadConverter) ToString(payload *commonpb.Payload) string {
	return base64.RawStdEncoding.EncodeToString(payload.GetData())
}

func (c *protoPayloadConverter) Encoding() string {
	return converter.MetadataEncodingProto
}

func pointerTo(val interface{}) reflect.Value {
	valPtr := reflect.New(reflect.TypeOf(val))
	valPtr.Elem().Set(reflect.ValueOf(val))
	return valPtr
}

func newOfSameType(val reflect.Value) reflect.Value {
	valType := val.Type().Elem()     // is value type (i.e. commonpb.WorkflowType)
	newValue := reflect.New(valType) // is of pointer type (i.e. *commonpb.WorkflowType)
	val.Set(newValue)                // set newly created value back to passed value
	return newValue
}
