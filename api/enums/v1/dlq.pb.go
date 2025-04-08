// The MIT License
//
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
// plugins:
// 	protoc-gen-go
// 	protoc
// source: temporal/server/api/enums/v1/dlq.proto

package enums

import (
	reflect "reflect"
	"strconv"
	sync "sync"
	unsafe "unsafe"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DLQOperationType int32

const (
	DLQ_OPERATION_TYPE_UNSPECIFIED DLQOperationType = 0
	DLQ_OPERATION_TYPE_MERGE       DLQOperationType = 1
	DLQ_OPERATION_TYPE_PURGE       DLQOperationType = 2
)

// Enum value maps for DLQOperationType.
var (
	DLQOperationType_name = map[int32]string{
		0: "DLQ_OPERATION_TYPE_UNSPECIFIED",
		1: "DLQ_OPERATION_TYPE_MERGE",
		2: "DLQ_OPERATION_TYPE_PURGE",
	}
	DLQOperationType_value = map[string]int32{
		"DLQ_OPERATION_TYPE_UNSPECIFIED": 0,
		"DLQ_OPERATION_TYPE_MERGE":       1,
		"DLQ_OPERATION_TYPE_PURGE":       2,
	}
)

func (x DLQOperationType) Enum() *DLQOperationType {
	p := new(DLQOperationType)
	*p = x
	return p
}

func (x DLQOperationType) String() string {
	switch x {
	case DLQ_OPERATION_TYPE_UNSPECIFIED:
		return "DlqOperationTypeUnspecified"
	case DLQ_OPERATION_TYPE_MERGE:
		return "DlqOperationTypeMerge"
	case DLQ_OPERATION_TYPE_PURGE:
		return "DlqOperationTypePurge"
	default:
		return strconv.Itoa(int(x))
	}

}

func (DLQOperationType) Descriptor() protoreflect.EnumDescriptor {
	return file_temporal_server_api_enums_v1_dlq_proto_enumTypes[0].Descriptor()
}

func (DLQOperationType) Type() protoreflect.EnumType {
	return &file_temporal_server_api_enums_v1_dlq_proto_enumTypes[0]
}

func (x DLQOperationType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DLQOperationType.Descriptor instead.
func (DLQOperationType) EnumDescriptor() ([]byte, []int) {
	return file_temporal_server_api_enums_v1_dlq_proto_rawDescGZIP(), []int{0}
}

type DLQOperationState int32

const (
	DLQ_OPERATION_STATE_UNSPECIFIED DLQOperationState = 0
	DLQ_OPERATION_STATE_RUNNING     DLQOperationState = 1
	DLQ_OPERATION_STATE_COMPLETED   DLQOperationState = 2
	DLQ_OPERATION_STATE_FAILED      DLQOperationState = 3
)

// Enum value maps for DLQOperationState.
var (
	DLQOperationState_name = map[int32]string{
		0: "DLQ_OPERATION_STATE_UNSPECIFIED",
		1: "DLQ_OPERATION_STATE_RUNNING",
		2: "DLQ_OPERATION_STATE_COMPLETED",
		3: "DLQ_OPERATION_STATE_FAILED",
	}
	DLQOperationState_value = map[string]int32{
		"DLQ_OPERATION_STATE_UNSPECIFIED": 0,
		"DLQ_OPERATION_STATE_RUNNING":     1,
		"DLQ_OPERATION_STATE_COMPLETED":   2,
		"DLQ_OPERATION_STATE_FAILED":      3,
	}
)

func (x DLQOperationState) Enum() *DLQOperationState {
	p := new(DLQOperationState)
	*p = x
	return p
}

func (x DLQOperationState) String() string {
	switch x {
	case DLQ_OPERATION_STATE_UNSPECIFIED:
		return "DlqOperationStateUnspecified"
	case DLQ_OPERATION_STATE_RUNNING:
		return "DlqOperationStateRunning"
	case DLQ_OPERATION_STATE_COMPLETED:
		return "DlqOperationStateCompleted"
	case DLQ_OPERATION_STATE_FAILED:
		return "DlqOperationStateFailed"
	default:
		return strconv.Itoa(int(x))
	}

}

func (DLQOperationState) Descriptor() protoreflect.EnumDescriptor {
	return file_temporal_server_api_enums_v1_dlq_proto_enumTypes[1].Descriptor()
}

func (DLQOperationState) Type() protoreflect.EnumType {
	return &file_temporal_server_api_enums_v1_dlq_proto_enumTypes[1]
}

func (x DLQOperationState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DLQOperationState.Descriptor instead.
func (DLQOperationState) EnumDescriptor() ([]byte, []int) {
	return file_temporal_server_api_enums_v1_dlq_proto_rawDescGZIP(), []int{1}
}

var File_temporal_server_api_enums_v1_dlq_proto protoreflect.FileDescriptor

const file_temporal_server_api_enums_v1_dlq_proto_rawDesc = "" +
	"\n" +
	"&temporal/server/api/enums/v1/dlq.proto\x12\x1ctemporal.server.api.enums.v1*r\n" +
	"\x10DLQOperationType\x12\"\n" +
	"\x1eDLQ_OPERATION_TYPE_UNSPECIFIED\x10\x00\x12\x1c\n" +
	"\x18DLQ_OPERATION_TYPE_MERGE\x10\x01\x12\x1c\n" +
	"\x18DLQ_OPERATION_TYPE_PURGE\x10\x02*\x9c\x01\n" +
	"\x11DLQOperationState\x12#\n" +
	"\x1fDLQ_OPERATION_STATE_UNSPECIFIED\x10\x00\x12\x1f\n" +
	"\x1bDLQ_OPERATION_STATE_RUNNING\x10\x01\x12!\n" +
	"\x1dDLQ_OPERATION_STATE_COMPLETED\x10\x02\x12\x1e\n" +
	"\x1aDLQ_OPERATION_STATE_FAILED\x10\x03B*Z(go.temporal.io/server/api/enums/v1;enumsb\x06proto3"

var (
	file_temporal_server_api_enums_v1_dlq_proto_rawDescOnce sync.Once
	file_temporal_server_api_enums_v1_dlq_proto_rawDescData []byte
)

func file_temporal_server_api_enums_v1_dlq_proto_rawDescGZIP() []byte {
	file_temporal_server_api_enums_v1_dlq_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_enums_v1_dlq_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_enums_v1_dlq_proto_rawDesc), len(file_temporal_server_api_enums_v1_dlq_proto_rawDesc)))
	})
	return file_temporal_server_api_enums_v1_dlq_proto_rawDescData
}

var file_temporal_server_api_enums_v1_dlq_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_temporal_server_api_enums_v1_dlq_proto_goTypes = []any{
	(DLQOperationType)(0),  // 0: temporal.server.api.enums.v1.DLQOperationType
	(DLQOperationState)(0), // 1: temporal.server.api.enums.v1.DLQOperationState
}
var file_temporal_server_api_enums_v1_dlq_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_enums_v1_dlq_proto_init() }
func file_temporal_server_api_enums_v1_dlq_proto_init() {
	if File_temporal_server_api_enums_v1_dlq_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_enums_v1_dlq_proto_rawDesc), len(file_temporal_server_api_enums_v1_dlq_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_enums_v1_dlq_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_enums_v1_dlq_proto_depIdxs,
		EnumInfos:         file_temporal_server_api_enums_v1_dlq_proto_enumTypes,
	}.Build()
	File_temporal_server_api_enums_v1_dlq_proto = out.File
	file_temporal_server_api_enums_v1_dlq_proto_goTypes = nil
	file_temporal_server_api_enums_v1_dlq_proto_depIdxs = nil
}
