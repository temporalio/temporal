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
// plugins:
// 	protoc-gen-go
// 	protoc
// source: temporal/server/api/metrics/v1/message.proto

package metrics

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

type Baggage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CountersInt map[string]int64 `protobuf:"bytes,1,rep,name=counters_int,json=countersInt,proto3" json:"counters_int,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"varint,2,opt,name=value,proto3"`
}

func (x *Baggage) Reset() {
	*x = Baggage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_metrics_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Baggage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Baggage) ProtoMessage() {}

func (x *Baggage) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_metrics_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Baggage.ProtoReflect.Descriptor instead.
func (*Baggage) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_metrics_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *Baggage) GetCountersInt() map[string]int64 {
	if x != nil {
		return x.CountersInt
	}
	return nil
}

var File_temporal_server_api_metrics_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_metrics_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2c, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x2f, 0x76, 0x31,
	0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1e,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x2e, 0x76, 0x31, 0x22, 0xa6,
	0x01, 0x0a, 0x07, 0x42, 0x61, 0x67, 0x67, 0x61, 0x67, 0x65, 0x12, 0x5b, 0x0a, 0x0c, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x65, 0x72, 0x73, 0x5f, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x38, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x2e, 0x76,
	0x31, 0x2e, 0x42, 0x61, 0x67, 0x67, 0x61, 0x67, 0x65, 0x2e, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x65,
	0x72, 0x73, 0x49, 0x6e, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0b, 0x63, 0x6f, 0x75, 0x6e,
	0x74, 0x65, 0x72, 0x73, 0x49, 0x6e, 0x74, 0x1a, 0x3e, 0x0a, 0x10, 0x43, 0x6f, 0x75, 0x6e, 0x74,
	0x65, 0x72, 0x73, 0x49, 0x6e, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x2e, 0x5a, 0x2c, 0x67, 0x6f, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x2f, 0x76, 0x31, 0x3b,
	0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_metrics_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_metrics_v1_message_proto_rawDescData = file_temporal_server_api_metrics_v1_message_proto_rawDesc
)

func file_temporal_server_api_metrics_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_metrics_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_metrics_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_metrics_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_metrics_v1_message_proto_rawDescData
}

var file_temporal_server_api_metrics_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_temporal_server_api_metrics_v1_message_proto_goTypes = []interface{}{
	(*Baggage)(nil), // 0: temporal.server.api.metrics.v1.Baggage
	nil,             // 1: temporal.server.api.metrics.v1.Baggage.CountersIntEntry
}
var file_temporal_server_api_metrics_v1_message_proto_depIdxs = []int32{
	1, // 0: temporal.server.api.metrics.v1.Baggage.counters_int:type_name -> temporal.server.api.metrics.v1.Baggage.CountersIntEntry
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_temporal_server_api_metrics_v1_message_proto_init() }
func file_temporal_server_api_metrics_v1_message_proto_init() {
	if File_temporal_server_api_metrics_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_metrics_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Baggage); i {
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
			RawDescriptor: file_temporal_server_api_metrics_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_metrics_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_metrics_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_metrics_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_metrics_v1_message_proto = out.File
	file_temporal_server_api_metrics_v1_message_proto_rawDesc = nil
	file_temporal_server_api_metrics_v1_message_proto_goTypes = nil
	file_temporal_server_api_metrics_v1_message_proto_depIdxs = nil
}
