// Package rpcgen provides descriptor-backed generators for unary RPC test
// boundaries. It deliberately provides structure, not service semantics.
package rpcgen

import (
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"pgregory.net/rapid"
)

// UnaryMethod binds generated request and response Go types to a unary method
// descriptor. The caller supplies typed constructors because descriptors alone
// cannot construct generated Go message types.
type UnaryMethod[Request, Response proto.Message] struct {
	descriptor  protoreflect.MethodDescriptor
	newRequest  func() Request
	newResponse func() Response
}

// InboundRequestClass records how a generated inbound request should be used
// by the receiving property. Descriptor structure alone cannot infer semantic
// validity, so intentionally invalid values are supplied by the use case.
type InboundRequestClass string

const (
	InboundRequestValid    InboundRequestClass = "valid"
	InboundRequestBoundary InboundRequestClass = "boundary"
	InboundRequestInvalid  InboundRequestClass = "invalid"
)

// InboundRequest is a transport-representable generated inbound request with
// its declared test domain.
type InboundRequest[Request proto.Message] struct {
	Class InboundRequestClass
	Value Request
}

// InboundRequestProfile supplies the semantic domains that descriptors cannot
// determine. Every generated value must remain representable on the wire.
type InboundRequestProfile[Request proto.Message] struct {
	Valid    *rapid.Generator[Request]
	Boundary *rapid.Generator[Request]
	Invalid  *rapid.Generator[Request]
}

// NewUnaryMethod validates a typed unary method binding.
func NewUnaryMethod[Request, Response proto.Message](
	descriptor protoreflect.MethodDescriptor,
	newRequest func() Request,
	newResponse func() Response,
) (UnaryMethod[Request, Response], error) {
	if descriptor == nil || descriptor.IsStreamingClient() || descriptor.IsStreamingServer() {
		return UnaryMethod[Request, Response]{}, errors.New("rpcgen: method must be unary")
	}
	if newRequest == nil || newResponse == nil {
		return UnaryMethod[Request, Response]{}, errors.New("rpcgen: typed constructors are required")
	}
	request := newRequest()
	response := newResponse()
	if isNil(request) || request.ProtoReflect().Descriptor().FullName() != descriptor.Input().FullName() {
		return UnaryMethod[Request, Response]{}, fmt.Errorf("rpcgen: request type does not match %s", descriptor.FullName())
	}
	if isNil(response) || response.ProtoReflect().Descriptor().FullName() != descriptor.Output().FullName() {
		return UnaryMethod[Request, Response]{}, fmt.Errorf("rpcgen: response type does not match %s", descriptor.FullName())
	}
	return UnaryMethod[Request, Response]{descriptor: descriptor, newRequest: newRequest, newResponse: newResponse}, nil
}

func isNil[Message proto.Message](message Message) bool {
	value := reflect.ValueOf(message)
	return !value.IsValid() || value.Kind() == reflect.Pointer && value.IsNil()
}

// FullMethodName is the stable gRPC method identifier used in diagnostics.
func (m UnaryMethod[Request, Response]) FullMethodName() string {
	return "/" + string(m.descriptor.Parent().FullName()) + "/" + string(m.descriptor.Name())
}

// RequestGenerator returns transport-representable typed request messages.
func (m UnaryMethod[Request, Response]) RequestGenerator() *rapid.Generator[Request] {
	return messageGenerator(m.newRequest)
}

// ResponseGenerator returns transport-representable typed response messages.
func (m UnaryMethod[Request, Response]) ResponseGenerator() *rapid.Generator[Response] {
	return messageGenerator(m.newResponse)
}

// InboundRequestGenerator returns classified, transport-representable inbound
// requests. Valid and boundary generators default to descriptor-backed values;
// an invalid generator is opt-in because its semantic meaning belongs to the
// use case rather than the protobuf descriptor.
func (m UnaryMethod[Request, Response]) InboundRequestGenerator(
	profile InboundRequestProfile[Request],
) *rapid.Generator[InboundRequest[Request]] {
	if profile.Valid == nil {
		profile.Valid = m.RequestGenerator()
	}
	if profile.Boundary == nil {
		profile.Boundary = rapid.Just(m.newRequest())
	}
	return rapid.Custom(func(t *rapid.T) InboundRequest[Request] {
		classes := []InboundRequestClass{InboundRequestValid, InboundRequestBoundary}
		if profile.Invalid != nil {
			classes = append(classes, InboundRequestInvalid)
		}
		class := classes[rapid.IntRange(0, len(classes)-1).Draw(t, m.FullMethodName()+" inbound class")]
		var value Request
		switch class {
		case InboundRequestValid:
			value = profile.Valid.Draw(t, m.FullMethodName()+" valid request")
		case InboundRequestBoundary:
			value = profile.Boundary.Draw(t, m.FullMethodName()+" boundary request")
		case InboundRequestInvalid:
			value = profile.Invalid.Draw(t, m.FullMethodName()+" invalid request")
		default:
			t.Fatalf("rpcgen: unsupported inbound request class %q", class)
		}
		if isNil(value) {
			t.Fatalf("rpcgen: %s inbound request generator returned nil", class)
		}
		if _, err := proto.Marshal(value); err != nil {
			t.Fatalf("rpcgen: %s inbound request is not transport-representable: %v", class, err)
		}
		return InboundRequest[Request]{Class: class, Value: value}
	})
}

func messageGenerator[Message proto.Message](newMessage func() Message) *rapid.Generator[Message] {
	return rapid.Custom(func(t *rapid.T) Message {
		message := newMessage()
		populate(t, message.ProtoReflect(), 0)
		return message
	})
}

func populate(t *rapid.T, message protoreflect.Message, depth int) {
	if depth == 2 {
		return
	}
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if !rapid.Bool().Draw(t, string(field.Name())+"-present") {
			continue
		}
		if field.IsList() {
			list := message.Mutable(field).List()
			value := list.NewElement()
			if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
				populate(t, value.Message(), depth+1)
			} else {
				value = scalar(t, field)
			}
			list.Append(value)
			continue
		}
		if field.IsMap() {
			entries := message.Mutable(field).Map()
			value := entries.NewValue()
			if field.MapValue().Kind() == protoreflect.MessageKind || field.MapValue().Kind() == protoreflect.GroupKind {
				populate(t, value.Message(), depth+1)
			} else {
				value = scalar(t, field.MapValue())
			}
			entries.Set(mapKey(t, field.MapKey()), value)
			continue
		}
		if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			child := message.Mutable(field).Message()
			populate(t, child, depth+1)
			continue
		}
		message.Set(field, scalar(t, field))
	}
}

func mapKey(t *rapid.T, field protoreflect.FieldDescriptor) protoreflect.MapKey {
	return scalar(t, field).MapKey()
}

func scalar(t *rapid.T, field protoreflect.FieldDescriptor) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(rapid.Bool().Draw(t, string(field.Name())))
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(rapid.StringN(0, 4, 16).Draw(t, string(field.Name())))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(rapid.StringN(0, 3, 8).Draw(t, string(field.Name()))))
	case protoreflect.EnumKind:
		values := field.Enum().Values()
		return protoreflect.ValueOfEnum(values.Get(rapid.IntRange(0, values.Len()-1).Draw(t, string(field.Name()))).Number())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(rapid.Int32().Draw(t, string(field.Name())))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(rapid.Int64().Draw(t, string(field.Name())))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(rapid.Uint32().Draw(t, string(field.Name())))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(rapid.Uint64().Draw(t, string(field.Name())))
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(rapid.Float32().Draw(t, string(field.Name())))
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(rapid.Float64().Draw(t, string(field.Name())))
	default:
		t.Fatalf("rpcgen: unsupported scalar kind %s", field.Kind())
		return protoreflect.Value{}
	}
}
