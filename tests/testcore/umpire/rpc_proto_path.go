package umpire

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func protoMessageName(message proto.Message) protoreflect.FullName {
	if message == nil || isNilProto(message) {
		return ""
	}
	return message.ProtoReflect().Descriptor().FullName()
}

func rpcValueMessageName(value any) (protoreflect.FullName, bool) {
	message, ok := value.(proto.Message)
	if !ok || isNilProto(message) {
		return "", false
	}
	return message.ProtoReflect().Descriptor().FullName(), true
}

func rpcMessageDescriptor(name protoreflect.FullName) (protoreflect.MessageDescriptor, error) {
	message, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, fmt.Errorf("message %s is not registered: %w", name, err)
	}
	return message.Descriptor(), nil
}

func resolveRPCFieldPath(
	message protoreflect.FullName,
	field RPCFieldSpec,
) (protoMutationPath, error) {
	if field.Proto != nil {
		return append(protoMutationPath(nil), field.Proto...), nil
	}
	descriptor, err := rpcMessageDescriptor(message)
	if err != nil {
		return nil, err
	}
	return resolveProtoMutationPath(descriptor, field.Path)
}

func resolveRPCFieldPathForDescriptor(
	descriptor protoreflect.MessageDescriptor,
	field RPCFieldSpec,
) (protoMutationPath, error) {
	if field.Proto != nil {
		return append(protoMutationPath(nil), field.Proto...), nil
	}
	return resolveProtoMutationPath(descriptor, field.Path)
}

func resolveProtoMutationPath(
	descriptor protoreflect.MessageDescriptor,
	fieldPath string,
) (protoMutationPath, error) {
	parts := strings.Split(fieldPath, ".")
	path := make(protoMutationPath, 0, len(parts))
	for i, part := range parts {
		field := descriptor.Fields().ByName(protoreflect.Name(part))
		if field == nil {
			return nil, errors.New("field not found")
		}
		path = append(path, field)
		if i == len(parts)-1 {
			return path, nil
		}
		if field.IsList() || field.IsMap() {
			return nil, errors.New("field is repeated or map")
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return nil, errors.New("field is not a message")
		}
		descriptor = field.Message()
	}
	return nil, errors.New("field path is empty")
}

func rpcFieldValue(
	message protoreflect.Message,
	path protoMutationPath,
) (protoreflect.FieldDescriptor, protoreflect.Value, error) {
	if len(path) == 0 {
		return nil, protoreflect.Value{}, errors.New("field path is empty")
	}
	for _, field := range path[:len(path)-1] {
		if field.IsList() || field.IsMap() {
			return nil, protoreflect.Value{}, fmt.Errorf("field %s is repeated or map", field.FullName())
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return nil, protoreflect.Value{}, fmt.Errorf("field %s is not a message", field.FullName())
		}
		message = message.Get(field).Message()
	}
	field := path[len(path)-1]
	return field, message.Get(field), nil
}

func validateRPCMutationPath(path protoMutationPath) error {
	if len(path) == 0 {
		return errors.New("field path is empty")
	}
	for _, field := range path[:len(path)-1] {
		if field.IsList() || field.IsMap() {
			return fmt.Errorf("field %s is repeated or map", field.FullName())
		}
		if field.ContainingOneof() != nil {
			return fmt.Errorf("field %s is in a oneof", field.FullName())
		}
	}
	field := path[len(path)-1]
	if field.IsList() || field.IsMap() {
		return fmt.Errorf("field %s is repeated or map", field.FullName())
	}
	if (field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind) && !isDurationField(field) {
		return fmt.Errorf("field %s is a message", field.FullName())
	}
	return nil
}

func isDurationField(field protoreflect.FieldDescriptor) bool {
	return field.Kind() == protoreflect.MessageKind &&
		field.Message() != nil &&
		field.Message().FullName() == durationProtoFullName
}
