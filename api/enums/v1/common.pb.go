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

// Copyright (c) 2020 Temporal Technologies, Inc.
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// source: temporal/server/api/enums/v1/common.proto

package enums

import (
	reflect "reflect"
	"strconv"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DeadLetterQueueType int32

const (
	DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED DeadLetterQueueType = 0
	DEAD_LETTER_QUEUE_TYPE_REPLICATION DeadLetterQueueType = 1
	DEAD_LETTER_QUEUE_TYPE_NAMESPACE   DeadLetterQueueType = 2
)

// Enum value maps for DeadLetterQueueType.
var (
	DeadLetterQueueType_name = map[int32]string{
		0: "DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED",
		1: "DEAD_LETTER_QUEUE_TYPE_REPLICATION",
		2: "DEAD_LETTER_QUEUE_TYPE_NAMESPACE",
	}
	DeadLetterQueueType_value = map[string]int32{
		"DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED": 0,
		"DEAD_LETTER_QUEUE_TYPE_REPLICATION": 1,
		"DEAD_LETTER_QUEUE_TYPE_NAMESPACE":   2,
	}
)

func (x DeadLetterQueueType) Enum() *DeadLetterQueueType {
	p := new(DeadLetterQueueType)
	*p = x
	return p
}

func (x DeadLetterQueueType) String() string {
	switch x {
	case DEAD_LETTER_QUEUE_TYPE_UNSPECIFIED:
		return "Unspecified"
	case DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		return "Replication"
	case DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		return "Namespace"
	default:
		return strconv.Itoa(int(x))
	}

}

func (DeadLetterQueueType) Descriptor() protoreflect.EnumDescriptor {
	return file_temporal_server_api_enums_v1_common_proto_enumTypes[0].Descriptor()
}

func (DeadLetterQueueType) Type() protoreflect.EnumType {
	return &file_temporal_server_api_enums_v1_common_proto_enumTypes[0]
}

func (x DeadLetterQueueType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DeadLetterQueueType.Descriptor instead.
func (DeadLetterQueueType) EnumDescriptor() ([]byte, []int) {
	return file_temporal_server_api_enums_v1_common_proto_rawDescGZIP(), []int{0}
}

type ChecksumFlavor int32

const (
	CHECKSUM_FLAVOR_UNSPECIFIED                   ChecksumFlavor = 0
	CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY ChecksumFlavor = 1
)

// Enum value maps for ChecksumFlavor.
var (
	ChecksumFlavor_name = map[int32]string{
		0: "CHECKSUM_FLAVOR_UNSPECIFIED",
		1: "CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY",
	}
	ChecksumFlavor_value = map[string]int32{
		"CHECKSUM_FLAVOR_UNSPECIFIED":                   0,
		"CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY": 1,
	}
)

func (x ChecksumFlavor) Enum() *ChecksumFlavor {
	p := new(ChecksumFlavor)
	*p = x
	return p
}

func (x ChecksumFlavor) String() string {
	switch x {
	case CHECKSUM_FLAVOR_UNSPECIFIED:
		return "Unspecified"
	case CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY:
		return "IeeeCrc32OverProto3Binary"
	default:
		return strconv.Itoa(int(x))
	}

}

func (ChecksumFlavor) Descriptor() protoreflect.EnumDescriptor {
	return file_temporal_server_api_enums_v1_common_proto_enumTypes[1].Descriptor()
}

func (ChecksumFlavor) Type() protoreflect.EnumType {
	return &file_temporal_server_api_enums_v1_common_proto_enumTypes[1]
}

func (x ChecksumFlavor) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ChecksumFlavor.Descriptor instead.
func (ChecksumFlavor) EnumDescriptor() ([]byte, []int) {
	return file_temporal_server_api_enums_v1_common_proto_rawDescGZIP(), []int{1}
}

var File_temporal_server_api_enums_v1_common_proto protoreflect.FileDescriptor

var file_temporal_server_api_enums_v1_common_proto_rawDesc = []byte{
	0x0a, 0x29, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x63,
	0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1c, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2a, 0x8b, 0x01, 0x0a, 0x13, 0x44, 0x65,
	0x61, 0x64, 0x4c, 0x65, 0x74, 0x74, 0x65, 0x72, 0x51, 0x75, 0x65, 0x75, 0x65, 0x54, 0x79, 0x70,
	0x65, 0x12, 0x26, 0x0a, 0x22, 0x44, 0x45, 0x41, 0x44, 0x5f, 0x4c, 0x45, 0x54, 0x54, 0x45, 0x52,
	0x5f, 0x51, 0x55, 0x45, 0x55, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50,
	0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x26, 0x0a, 0x22, 0x44, 0x45, 0x41,
	0x44, 0x5f, 0x4c, 0x45, 0x54, 0x54, 0x45, 0x52, 0x5f, 0x51, 0x55, 0x45, 0x55, 0x45, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x50, 0x4c, 0x49, 0x43, 0x41, 0x54, 0x49, 0x4f, 0x4e, 0x10,
	0x01, 0x12, 0x24, 0x0a, 0x20, 0x44, 0x45, 0x41, 0x44, 0x5f, 0x4c, 0x45, 0x54, 0x54, 0x45, 0x52,
	0x5f, 0x51, 0x55, 0x45, 0x55, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4e, 0x41, 0x4d, 0x45,
	0x53, 0x50, 0x41, 0x43, 0x45, 0x10, 0x02, 0x2a, 0x64, 0x0a, 0x0e, 0x43, 0x68, 0x65, 0x63, 0x6b,
	0x73, 0x75, 0x6d, 0x46, 0x6c, 0x61, 0x76, 0x6f, 0x72, 0x12, 0x1f, 0x0a, 0x1b, 0x43, 0x48, 0x45,
	0x43, 0x4b, 0x53, 0x55, 0x4d, 0x5f, 0x46, 0x4c, 0x41, 0x56, 0x4f, 0x52, 0x5f, 0x55, 0x4e, 0x53,
	0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x31, 0x0a, 0x2d, 0x43, 0x48,
	0x45, 0x43, 0x4b, 0x53, 0x55, 0x4d, 0x5f, 0x46, 0x4c, 0x41, 0x56, 0x4f, 0x52, 0x5f, 0x49, 0x45,
	0x45, 0x45, 0x5f, 0x43, 0x52, 0x43, 0x33, 0x32, 0x5f, 0x4f, 0x56, 0x45, 0x52, 0x5f, 0x50, 0x52,
	0x4f, 0x54, 0x4f, 0x33, 0x5f, 0x42, 0x49, 0x4e, 0x41, 0x52, 0x59, 0x10, 0x01, 0x42, 0x2a, 0x5a,
	0x28, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73,
	0x2f, 0x76, 0x31, 0x3b, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_temporal_server_api_enums_v1_common_proto_rawDescOnce sync.Once
	file_temporal_server_api_enums_v1_common_proto_rawDescData = file_temporal_server_api_enums_v1_common_proto_rawDesc
)

func file_temporal_server_api_enums_v1_common_proto_rawDescGZIP() []byte {
	file_temporal_server_api_enums_v1_common_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_enums_v1_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_enums_v1_common_proto_rawDescData)
	})
	return file_temporal_server_api_enums_v1_common_proto_rawDescData
}

var file_temporal_server_api_enums_v1_common_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_temporal_server_api_enums_v1_common_proto_goTypes = []interface{}{
	(DeadLetterQueueType)(0), // 0: temporal.server.api.enums.v1.DeadLetterQueueType
	(ChecksumFlavor)(0),      // 1: temporal.server.api.enums.v1.ChecksumFlavor
}
var file_temporal_server_api_enums_v1_common_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_enums_v1_common_proto_init() }
func file_temporal_server_api_enums_v1_common_proto_init() {
	if File_temporal_server_api_enums_v1_common_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_enums_v1_common_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_enums_v1_common_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_enums_v1_common_proto_depIdxs,
		EnumInfos:         file_temporal_server_api_enums_v1_common_proto_enumTypes,
	}.Build()
	File_temporal_server_api_enums_v1_common_proto = out.File
	file_temporal_server_api_enums_v1_common_proto_rawDesc = nil
	file_temporal_server_api_enums_v1_common_proto_goTypes = nil
	file_temporal_server_api_enums_v1_common_proto_depIdxs = nil
}
