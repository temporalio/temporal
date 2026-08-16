package kitchensink

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	kitchenSinkProtoPackage        = "temporal.omes.kitchen_sink"
	umpire2KitchenSinkProtoPackage = "temporal.omes.umpire2.kitchen_sink"
)

// namespaceKitchenSinkDescriptor lets the copied Umpire 2 support package coexist with v1 in
// compatibility-test binaries; protobuf's global registry rejects duplicate file and type names.
func namespaceKitchenSinkDescriptor(rawDescriptor []byte) []byte {
	file := &descriptorpb.FileDescriptorProto{}
	if err := proto.Unmarshal(rawDescriptor, file); err != nil {
		panic(fmt.Errorf("umpire2 kitchensink: decode protobuf descriptor: %w", err))
	}

	file.Name = proto.String("umpire2/kitchen_sink.proto")
	file.Package = proto.String(umpire2KitchenSinkProtoPackage)
	for _, message := range file.MessageType {
		namespaceMessageDescriptor(message)
	}
	for _, extension := range file.Extension {
		namespaceFieldDescriptor(extension)
	}
	for _, service := range file.Service {
		for _, method := range service.Method {
			method.InputType = namespaceTypeName(method.InputType)
			method.OutputType = namespaceTypeName(method.OutputType)
		}
	}

	namespaced, err := proto.Marshal(file)
	if err != nil {
		panic(fmt.Errorf("umpire2 kitchensink: encode protobuf descriptor: %w", err))
	}
	return namespaced
}

func namespaceMessageDescriptor(message *descriptorpb.DescriptorProto) {
	for _, field := range message.Field {
		namespaceFieldDescriptor(field)
	}
	for _, extension := range message.Extension {
		namespaceFieldDescriptor(extension)
	}
	for _, nested := range message.NestedType {
		namespaceMessageDescriptor(nested)
	}
}

func namespaceFieldDescriptor(field *descriptorpb.FieldDescriptorProto) {
	field.TypeName = namespaceTypeName(field.TypeName)
	field.Extendee = namespaceTypeName(field.Extendee)
}

func namespaceTypeName(typeName *string) *string {
	if typeName == nil {
		return nil
	}
	oldPrefix := "." + kitchenSinkProtoPackage + "."
	if !strings.HasPrefix(*typeName, oldPrefix) {
		return typeName
	}
	namespaced := "." + umpire2KitchenSinkProtoPackage + "." + strings.TrimPrefix(*typeName, oldPrefix)
	return &namespaced
}
