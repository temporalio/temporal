package protoutils

import "google.golang.org/protobuf/reflect/protoreflect"

type ProtoEnum interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}

func EnumValues[T ProtoEnum]() []T {
	var zero T
	values := zero.Descriptor().Values()
	enums := make([]T, values.Len())
	for i := range enums {
		enums[i] = T(values.Get(i).Number())
	}
	return enums
}
