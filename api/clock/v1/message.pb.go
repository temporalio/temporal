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
// source: temporal/server/api/clock/v1/message.proto

package clock

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

type VectorClock struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ShardId   int32 `protobuf:"varint,1,opt,name=shard_id,json=shardId,proto3" json:"shard_id,omitempty"`
	Clock     int64 `protobuf:"varint,2,opt,name=clock,proto3" json:"clock,omitempty"`
	ClusterId int64 `protobuf:"varint,3,opt,name=cluster_id,json=clusterId,proto3" json:"cluster_id,omitempty"`
}

func (x *VectorClock) Reset() {
	*x = VectorClock{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_clock_v1_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VectorClock) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VectorClock) ProtoMessage() {}

func (x *VectorClock) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_clock_v1_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VectorClock.ProtoReflect.Descriptor instead.
func (*VectorClock) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_clock_v1_message_proto_rawDescGZIP(), []int{0}
}

func (x *VectorClock) GetShardId() int32 {
	if x != nil {
		return x.ShardId
	}
	return 0
}

func (x *VectorClock) GetClock() int64 {
	if x != nil {
		return x.Clock
	}
	return 0
}

func (x *VectorClock) GetClusterId() int64 {
	if x != nil {
		return x.ClusterId
	}
	return 0
}

// A Hybrid Logical Clock timestamp.
// Guarantees strict total ordering for conflict resolution purposes.
type HybridLogicalClock struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Wall clock - A single time source MUST guarantee that 2 consecutive timestamps are monotonically non-decreasing.
	// e.g. by storing the last wall clock and returning max(gettimeofday(), lastWallClock).
	WallClock int64 `protobuf:"varint,1,opt,name=wall_clock,json=wallClock,proto3" json:"wall_clock,omitempty"`
	// Incremental sequence that is reset every time the system's wallclock moves forward.
	// Ensures the clock generates monotonically increasing timestamps.
	Version int32 `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
	// The cluster version ID as described in the XDC docs - used as a tie breaker.
	// See: https://github.com/uber/cadence/blob/master/docs/design/2290-cadence-ndc.md
	ClusterId int64 `protobuf:"varint,3,opt,name=cluster_id,json=clusterId,proto3" json:"cluster_id,omitempty"`
}

func (x *HybridLogicalClock) Reset() {
	*x = HybridLogicalClock{}
	if protoimpl.UnsafeEnabled {
		mi := &file_temporal_server_api_clock_v1_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HybridLogicalClock) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HybridLogicalClock) ProtoMessage() {}

func (x *HybridLogicalClock) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_clock_v1_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HybridLogicalClock.ProtoReflect.Descriptor instead.
func (*HybridLogicalClock) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_clock_v1_message_proto_rawDescGZIP(), []int{1}
}

func (x *HybridLogicalClock) GetWallClock() int64 {
	if x != nil {
		return x.WallClock
	}
	return 0
}

func (x *HybridLogicalClock) GetVersion() int32 {
	if x != nil {
		return x.Version
	}
	return 0
}

func (x *HybridLogicalClock) GetClusterId() int64 {
	if x != nil {
		return x.ClusterId
	}
	return 0
}

var File_temporal_server_api_clock_v1_message_proto protoreflect.FileDescriptor

var file_temporal_server_api_clock_v1_message_proto_rawDesc = []byte{
	0x0a, 0x2a, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2f, 0x76, 0x31, 0x2f, 0x6d,
	0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1c, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2e, 0x76, 0x31, 0x22, 0x5d, 0x0a, 0x0b, 0x56, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x12, 0x19, 0x0a, 0x08, 0x73, 0x68, 0x61,
	0x72, 0x64, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x07, 0x73, 0x68, 0x61,
	0x72, 0x64, 0x49, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x05, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09,
	0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x49, 0x64, 0x22, 0x6c, 0x0a, 0x12, 0x48, 0x79, 0x62,
	0x72, 0x69, 0x64, 0x4c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x12,
	0x1d, 0x0a, 0x0a, 0x77, 0x61, 0x6c, 0x6c, 0x5f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x09, 0x77, 0x61, 0x6c, 0x6c, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x12, 0x18,
	0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c, 0x75, 0x73,
	0x74, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x63, 0x6c,
	0x75, 0x73, 0x74, 0x65, 0x72, 0x49, 0x64, 0x42, 0x2a, 0x5a, 0x28, 0x67, 0x6f, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x2f, 0x76, 0x31, 0x3b, 0x63, 0x6c,
	0x6f, 0x63, 0x6b, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_temporal_server_api_clock_v1_message_proto_rawDescOnce sync.Once
	file_temporal_server_api_clock_v1_message_proto_rawDescData = file_temporal_server_api_clock_v1_message_proto_rawDesc
)

func file_temporal_server_api_clock_v1_message_proto_rawDescGZIP() []byte {
	file_temporal_server_api_clock_v1_message_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_clock_v1_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_temporal_server_api_clock_v1_message_proto_rawDescData)
	})
	return file_temporal_server_api_clock_v1_message_proto_rawDescData
}

var file_temporal_server_api_clock_v1_message_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_temporal_server_api_clock_v1_message_proto_goTypes = []interface{}{
	(*VectorClock)(nil),        // 0: temporal.server.api.clock.v1.VectorClock
	(*HybridLogicalClock)(nil), // 1: temporal.server.api.clock.v1.HybridLogicalClock
}
var file_temporal_server_api_clock_v1_message_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_clock_v1_message_proto_init() }
func file_temporal_server_api_clock_v1_message_proto_init() {
	if File_temporal_server_api_clock_v1_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_temporal_server_api_clock_v1_message_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VectorClock); i {
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
		file_temporal_server_api_clock_v1_message_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HybridLogicalClock); i {
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
			RawDescriptor: file_temporal_server_api_clock_v1_message_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_clock_v1_message_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_clock_v1_message_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_clock_v1_message_proto_msgTypes,
	}.Build()
	File_temporal_server_api_clock_v1_message_proto = out.File
	file_temporal_server_api_clock_v1_message_proto_rawDesc = nil
	file_temporal_server_api_clock_v1_message_proto_goTypes = nil
	file_temporal_server_api_clock_v1_message_proto_depIdxs = nil
}
