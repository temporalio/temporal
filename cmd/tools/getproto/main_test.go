package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestDescriptorSetOrder(t *testing.T) {
	files := make(map[string]protoreflect.FileDescriptor)
	for _, name := range []string{"z.proto", "a.proto", "m.proto"} {
		descriptor, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String(name)}, nil)
		require.NoError(t, err)
		files[name] = descriptor
	}

	var names []string
	for _, file := range descriptorSet(files).GetFile() {
		names = append(names, file.GetName())
	}
	require.Equal(t, []string{"a.proto", "m.proto", "z.proto"}, names)
}
