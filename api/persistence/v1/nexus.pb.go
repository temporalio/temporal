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

// Copyright (c) 2019 Temporal Technologies, Inc.
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
// source: temporal/server/api/persistence/v1/nexus.proto

package persistence

import (
	reflect "reflect"
	sync "sync"

	v11 "go.temporal.io/api/nexus/v1"
	v1 "go.temporal.io/server/api/clock/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type NexusEndpoint struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The last recorded cluster-local Hybrid Logical Clock timestamp for _this_ endpoint.
	// Updated whenever the endpoint is directly updated due to a user action but not when applying replication events.
	// The clock is referenced when new timestamps are generated to ensure it produces monotonically increasing
	// timestamps.
	Clock *v1.HybridLogicalClock `protobuf:"bytes,1,opt,name=clock,proto3" json:"clock,omitempty"`
	// Endpoint specification. This is a mirror of the public API and is intended to be mutable.
	Spec *v11.EndpointSpec `protobuf:"bytes,2,opt,name=spec,proto3" json:"spec,omitempty"`
	// The date and time when the endpoint was created.
	// (-- api-linter: core::0142::time-field-names=disabled
	//
	//	aip.dev/not-precedent: Not following linter rules. --)
	CreatedTime *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=created_time,json=createdTime,proto3" json:"created_time,omitempty"`
}

func (x *NexusEndpoint) Reset() {
	*x = NexusEndpoint{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NexusEndpoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpoint) ProtoMessage() {}

func (x *NexusEndpoint) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpoint.ProtoReflect.Descriptor instead.
func (*NexusEndpoint) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{0}
}

func (x *NexusEndpoint) GetClock() *v1.HybridLogicalClock {
	if x != nil {
		return x.Clock
	}
	return nil
}

func (x *NexusEndpoint) GetSpec() *v11.EndpointSpec {
	if x != nil {
		return x.Spec
	}
	return nil
}

func (x *NexusEndpoint) GetCreatedTime() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedTime
	}
	return nil
}

// Container for a version, a UUID, and a NexusEndpoint.
type NexusEndpointEntry struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Version  int64          `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	Id       string         `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Endpoint *NexusEndpoint `protobuf:"bytes,3,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
}

func (x *NexusEndpointEntry) Reset() {
	*x = NexusEndpointEntry{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NexusEndpointEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointEntry) ProtoMessage() {}

func (x *NexusEndpointEntry) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpointEntry.ProtoReflect.Descriptor instead.
func (*NexusEndpointEntry) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{1}
}

func (x *NexusEndpointEntry) GetVersion() int64 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *NexusEndpointEntry) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *NexusEndpointEntry) GetEndpoint() *NexusEndpoint {
	if x != nil {
		return x.Endpoint
	}
	return nil
}

var File_temporal_server_api_persistence_v1_nexus_proto protoreflect.FileDescriptor

var file_temporal_server_api_persistence_v1_nexus_proto_rawDesc = []byte{
	0x0a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2f,
	0x76, 0x31, 0x2f, 0x6e, 0x65, 0x78, 0x75, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x22,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x23, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6e,
	0x65, 0x78, 0x75, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2a, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2f, 0x76,
	0x31, 0x2f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xdb,
	0x01, 0x0a, 0x0d, 0x4e, 0x65, 0x78, 0x75, 0x73, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12,
	0x4a, 0x0a, 0x05, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x30,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x79, 0x62,
	0x72, 0x69, 0x64, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x52, 0x05,
	0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x42, 0x02, 0x68, 0x00, 0x12, 0x3b, 0x0a, 0x04, 0x73, 0x70, 0x65, 0x63,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6e, 0x65, 0x78, 0x75, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x53, 0x70, 0x65, 0x63, 0x52, 0x04, 0x73, 0x70, 0x65, 0x63, 0x42,
	0x02, 0x68, 0x00, 0x12, 0x41, 0x0a, 0x0c, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x52, 0x0b, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x54, 0x69, 0x6d, 0x65,
	0x42, 0x02, 0x68, 0x00, 0x22, 0x99, 0x01, 0x0a, 0x12, 0x4e, 0x65, 0x78, 0x75, 0x73, 0x45, 0x6e, 0x64,
	0x70, 0x6f, 0x69, 0x6e, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x1c, 0x0a, 0x07, 0x76, 0x65,
	0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x42, 0x02, 0x68, 0x00, 0x12, 0x12, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x42, 0x02, 0x68, 0x00, 0x12, 0x51, 0x0a, 0x08, 0x65, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x31, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x4e, 0x65, 0x78, 0x75, 0x73, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x08, 0x65, 0x6e,
	0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x42, 0x02, 0x68, 0x00, 0x42, 0x36, 0x5a, 0x34, 0x67, 0x6f,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63,
	0x65, 0x2f, 0x76, 0x31, 0x3b, 0x70, 0x65, 0x72, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x65,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescData = file_temporal_server_api_persistence_v1_nexus_proto_rawDesc
)

func file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_nexus_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_persistence_v1_nexus_proto_rawDescData)
	})
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_nexus_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_temporal_server_api_persistence_v1_nexus_proto_goTypes = []interface{}{
	(*NexusEndpoint)(nil),         // 0: temporal.server.api.persistence.v1.NexusEndpoint
	(*NexusEndpointEntry)(nil),    // 1: temporal.server.api.persistence.v1.NexusEndpointEntry
	(*v1.HybridLogicalClock)(nil), // 2: temporal.server.api.clock.v1.HybridLogicalClock
	(*v11.EndpointSpec)(nil),      // 3: temporal.api.nexus.v1.EndpointSpec
	(*timestamppb.Timestamp)(nil), // 4: google.protobuf.Timestamp
}
var file_temporal_server_api_persistence_v1_nexus_proto_depIdxs = []int32{
	2, // 0: temporal.server.api.persistence.v1.NexusEndpoint.clock:type_name -> temporal.server.api.clock.v1.HybridLogicalClock
	3, // 1: temporal.server.api.persistence.v1.NexusEndpoint.spec:type_name -> temporal.api.nexus.v1.EndpointSpec
	4, // 2: temporal.server.api.persistence.v1.NexusEndpoint.created_time:type_name -> google.protobuf.Timestamp
	0, // 3: temporal.server.api.persistence.v1.NexusEndpointEntry.endpoint:type_name -> temporal.server.api.persistence.v1.NexusEndpoint
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_nexus_proto_init() }
func file_temporal_server_api_persistence_v1_nexus_proto_init() {
	if File_temporal_server_api_persistence_v1_nexus_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NexusEndpoint); i {
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
		file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NexusEndpointEntry); i {
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
			RawDescriptor: file_temporal_server_api_persistence_v1_nexus_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_nexus_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_nexus_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_nexus_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_nexus_proto = out.File
	file_temporal_server_api_persistence_v1_nexus_proto_rawDesc = nil
	file_temporal_server_api_persistence_v1_nexus_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_nexus_proto_depIdxs = nil
}
