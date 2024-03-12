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
// source: temporal/server/api/namespace/v1/message.proto

package namespace

import (
	reflect "reflect"
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

type NamespaceCacheInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// (-- api-linter: core::0140::prepositions=disabled
	//
	//	aip.dev/not-precedent: "in" and "by" are needed here. --)
	ItemsInCacheByIdCount int64 `protobuf:"varint,1,opt,name=items_in_cache_by_id_count,json=itemsInCacheByIdCount,proto3" json:"items_in_cache_by_id_count,omitempty"`
	// (-- api-linter: core::0140::prepositions=disabled
	//
	//	aip.dev/not-precedent: "in" and "by" are needed here. --)
	ItemsInCacheByNameCount int64 `protobuf:"varint,2,opt,name=items_in_cache_by_name_count,json=itemsInCacheByNameCount,proto3" json:"items_in_cache_by_name_count,omitempty"`
}

func (x *NamespaceCacheInfo) Reset() {
	*x = NamespaceCacheInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_namespace_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NamespaceCacheInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NamespaceCacheInfo) ProtoMessage() {}

func (x *NamespaceCacheInfo) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_namespace_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NamespaceCacheInfo.ProtoReflect.Descriptor instead.
func (*NamespaceCacheInfo) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_namespace_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *NamespaceCacheInfo) GetItemsInCacheByIdCount() int64 {
	if x != nil {
		return x.ItemsInCacheByIdCount
	}
	return 0
}

func (x *NamespaceCacheInfo) GetItemsInCacheByNameCount() int64 {
	if x != nil {
		return x.ItemsInCacheByNameCount
	}
	return 0
}

var File_temporal_server_api_namespace_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_namespace_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x20,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x22,
	0x96, 0x01, 0x0a, 0x12, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x43, 0x61, 0x63,
	0x68, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x3d, 0x0a, 0x1a, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x5f, 0x69,
	0x6e, 0x5f, 0x63, 0x61, 0x63, 0x68, 0x65, 0x5f, 0x62, 0x79, 0x5f, 0x69, 0x64, 0x5f, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x15, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x49,
	0x6e, 0x43, 0x61, 0x63, 0x68, 0x65, 0x42, 0x79, 0x49, 0x64, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x41, 0x0a, 0x1c, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x5f, 0x69, 0x6e, 0x5f,
	0x63, 0x61, 0x63, 0x68, 0x65, 0x5f, 0x62, 0x79, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x63, 0x6f, 0x75,
	0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x17, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x49,
	0x6e, 0x43, 0x61, 0x63, 0x68, 0x65, 0x42, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x43, 0x6f, 0x75, 0x6e, 0x74,
	0x42, 0x02, 0x68, 0x00, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2f, 0x76, 0x31, 0x3b, 0x6e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_namespace_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_namespace_v1_message_proto_rawDescData = file_temporal_server_api_namespace_v1_message_proto_rawDesc
)

func file_temporal_server_api_namespace_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_namespace_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_namespace_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_namespace_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_namespace_v1_message_proto_rawDescData
}

var file_temporal_server_api_namespace_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_temporal_server_api_namespace_v1_message_proto_goTypes = []interface{}{
	(*NamespaceCacheInfo)(nil), // 0: temporal.server.api.namespace.v1.NamespaceCacheInfo
}
var file_temporal_server_api_namespace_v1_message_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_namespace_v1_message_proto_init() }
func file_temporal_server_api_namespace_v1_message_proto_init() {
	if File_temporal_server_api_namespace_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_namespace_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NamespaceCacheInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_namespace_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_namespace_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_namespace_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_namespace_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_namespace_v1_message_proto = out.File
	file_temporal_server_api_namespace_v1_message_proto_rawDesc = nil
	file_temporal_server_api_namespace_v1_message_proto_goTypes = nil
	file_temporal_server_api_namespace_v1_message_proto_depIdxs = nil
}
