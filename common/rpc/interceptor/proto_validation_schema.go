package interceptor

import (
	"errors"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newProtoValidator(messages ...proto.Message) (protovalidate.Validator, error) {
	var descriptors []protoreflect.MessageDescriptor
	visited := make(map[protoreflect.MessageDescriptor]struct{})
	var visit func(protoreflect.MessageDescriptor)
	visit = func(descriptor protoreflect.MessageDescriptor) {
		if _, ok := visited[descriptor]; ok {
			return
		}
		visited[descriptor] = struct{}{}
		if !descriptor.IsMapEntry() {
			descriptors = append(descriptors, descriptor)
		}
		for index := range descriptor.Fields().Len() {
			if child := descriptor.Fields().Get(index).Message(); child != nil {
				visit(child)
			}
		}
	}
	for _, message := range messages {
		visit(message.ProtoReflect().Descriptor())
	}
	validator, err := protovalidate.New(protovalidate.WithMessageDescriptors(descriptors...), protovalidate.WithDisableLazy())
	if err != nil {
		return nil, err
	}
	// Buf caches compilation errors. Probe each scope so an absent child or
	// an unrelated value error cannot hide an invalid rule at startup.
	for _, descriptor := range descriptors {
		message := dynamicpb.NewMessage(descriptor)
		probe := func(target protoreflect.Descriptor) error {
			err := validator.Validate(message, protovalidate.WithFilter(protovalidate.FilterFunc(func(_ protoreflect.Message, scope protoreflect.Descriptor) bool {
				return scope == target
			})))
			if _, ok := errors.AsType[*protovalidate.CompilationError](err); ok {
				return fmt.Errorf("compile protobuf validation for %s: %w", target.FullName(), err)
			}
			return nil
		}
		if err := probe(descriptor); err != nil {
			return nil, err
		}
		for index := range descriptor.Fields().Len() {
			if err := probe(descriptor.Fields().Get(index)); err != nil {
				return nil, err
			}
		}
	}
	return validator, nil
}
