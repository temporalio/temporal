// The MIT License
//
// Copyright (c) 2021 Temporal Technologies, Inc.
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
// source: temporal/server/api/enums/v1/workflow_task_type.proto

package enums

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	"strconv"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type WorkflowTaskType int32

const (
	WORKFLOW_TASK_TYPE_UNSPECIFIED WorkflowTaskType = 0
	WORKFLOW_TASK_TYPE_NORMAL      WorkflowTaskType = 1
	// TODO (alex): TRANSIENT is not current used. Needs to be set when Attempt>1.
	WORKFLOW_TASK_TYPE_TRANSIENT   WorkflowTaskType = 2
	WORKFLOW_TASK_TYPE_SPECULATIVE WorkflowTaskType = 3
)

// Enum value maps for WorkflowTaskType.
var (
	WorkflowTaskType_name = map[int32]string{
		0: "WORKFLOW_TASK_TYPE_UNSPECIFIED",
		1: "WORKFLOW_TASK_TYPE_NORMAL",
		2: "WORKFLOW_TASK_TYPE_TRANSIENT",
		3: "WORKFLOW_TASK_TYPE_SPECULATIVE",
	}
	WorkflowTaskType_value = map[string]int32{
		"WORKFLOW_TASK_TYPE_UNSPECIFIED": 0,
		"WORKFLOW_TASK_TYPE_NORMAL":      1,
		"WORKFLOW_TASK_TYPE_TRANSIENT":   2,
		"WORKFLOW_TASK_TYPE_SPECULATIVE": 3,
	}
)

func (x WorkflowTaskType) Enum() *WorkflowTaskType {
	p := new(WorkflowTaskType)
	*p = x
	return p
}

func (x WorkflowTaskType) String() string {
	switch x {
	case WORKFLOW_TASK_TYPE_UNSPECIFIED:
		return "Unspecified"
	case WORKFLOW_TASK_TYPE_NORMAL:
		return "Normal"
	case WORKFLOW_TASK_TYPE_TRANSIENT:
		return "Transient"
	case WORKFLOW_TASK_TYPE_SPECULATIVE:
		return "Speculative"
	default:
		return strconv.Itoa(int(x))
	}

}

func (WorkflowTaskType) Descriptor() protoreflect.EnumDescriptor {
	return file_temporal_server_api_enums_v1_workflow_task_type_proto_enumTypes[0].Descriptor()
}

func (WorkflowTaskType) Type() protoreflect.EnumType {
	return &file_temporal_server_api_enums_v1_workflow_task_type_proto_enumTypes[0]
}

func (x WorkflowTaskType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use WorkflowTaskType.Descriptor instead.
func (WorkflowTaskType) EnumDescriptor() ([]byte, []int) {
	return file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescGZIP(), []int{0}
}

var File_temporal_server_api_enums_v1_workflow_task_type_proto protoreflect.FileDescriptor

var file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDesc = []byte{
	0x0a, 0x35, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x77,
	0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x5f, 0x74, 0x61, 0x73, 0x6b, 0x5f, 0x74, 0x79, 0x70,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1c, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x65, 0x6e, 0x75,
	0x6d, 0x73, 0x2e, 0x76, 0x31, 0x2a, 0x9b, 0x01, 0x0a, 0x10, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c,
	0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x54, 0x79, 0x70, 0x65, 0x12, 0x22, 0x0a, 0x1e, 0x57, 0x4f,
	0x52, 0x4b, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x54, 0x41, 0x53, 0x4b, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x1d,
	0x0a, 0x19, 0x57, 0x4f, 0x52, 0x4b, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x54, 0x41, 0x53, 0x4b, 0x5f,
	0x54, 0x59, 0x50, 0x45, 0x5f, 0x4e, 0x4f, 0x52, 0x4d, 0x41, 0x4c, 0x10, 0x01, 0x12, 0x20, 0x0a,
	0x1c, 0x57, 0x4f, 0x52, 0x4b, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x54, 0x41, 0x53, 0x4b, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x54, 0x52, 0x41, 0x4e, 0x53, 0x49, 0x45, 0x4e, 0x54, 0x10, 0x02, 0x12,
	0x22, 0x0a, 0x1e, 0x57, 0x4f, 0x52, 0x4b, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x54, 0x41, 0x53, 0x4b,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x50, 0x45, 0x43, 0x55, 0x4c, 0x41, 0x54, 0x49, 0x56,
	0x45, 0x10, 0x03, 0x42, 0x2a, 0x5a, 0x28, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x2f, 0x76, 0x31, 0x3b, 0x65, 0x6e, 0x75, 0x6d, 0x73, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescOnce sync.Once
	file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescData = file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDesc
)

func file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescGZIP() []byte {
	file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescData)
	})
	return file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDescData
}

var file_temporal_server_api_enums_v1_workflow_task_type_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_temporal_server_api_enums_v1_workflow_task_type_proto_goTypes = []interface{}{
	(WorkflowTaskType)(0), // 0: temporal.server.api.enums.v1.WorkflowTaskType
}
var file_temporal_server_api_enums_v1_workflow_task_type_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_enums_v1_workflow_task_type_proto_init() }
func file_temporal_server_api_enums_v1_workflow_task_type_proto_init() {
	if File_temporal_server_api_enums_v1_workflow_task_type_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_enums_v1_workflow_task_type_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_enums_v1_workflow_task_type_proto_depIdxs,
		EnumInfos:         file_temporal_server_api_enums_v1_workflow_task_type_proto_enumTypes,
	}.Build()
	File_temporal_server_api_enums_v1_workflow_task_type_proto = out.File
	file_temporal_server_api_enums_v1_workflow_task_type_proto_rawDesc = nil
	file_temporal_server_api_enums_v1_workflow_task_type_proto_goTypes = nil
	file_temporal_server_api_enums_v1_workflow_task_type_proto_depIdxs = nil
}
