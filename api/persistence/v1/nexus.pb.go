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
	unsafe "unsafe"

	v1 "go.temporal.io/api/common/v1"
	v11 "go.temporal.io/server/api/clock/v1"
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

// Contains mutable fields for an Endpoint. Duplicated from the public API's temporal.api.nexus.v1.EndpointSpec where
// the worker target has a namespace name.
// We store an ID in persistence to prevent namespace renames from breaking references.
type NexusEndpointSpec struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Endpoint name, unique for this cluster. Must match `[a-zA-Z_][a-zA-Z0-9_]*`.
	// Renaming an endpoint breaks all workflow callers that reference this endpoint, causing operations to fail.
	Name        string      `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Description *v1.Payload `protobuf:"bytes,2,opt,name=description,proto3" json:"description,omitempty"`
	// Target to route requests to.
	Target        *NexusEndpointTarget `protobuf:"bytes,3,opt,name=target,proto3" json:"target,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpointSpec) Reset() {
	*x = NexusEndpointSpec{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpointSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointSpec) ProtoMessage() {}

func (x *NexusEndpointSpec) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpointSpec.ProtoReflect.Descriptor instead.
func (*NexusEndpointSpec) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{0}
}

func (x *NexusEndpointSpec) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *NexusEndpointSpec) GetDescription() *v1.Payload {
	if x != nil {
		return x.Description
	}
	return nil
}

func (x *NexusEndpointSpec) GetTarget() *NexusEndpointTarget {
	if x != nil {
		return x.Target
	}
	return nil
}

// Target to route requests to.
// Duplicated from the public API's temporal.api.nexus.v1.EndpointTarget where the worker target has a namespace name.
// We store an ID in persistence to prevent namespace renames from breaking references.
type NexusEndpointTarget struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Variant:
	//
	//	*NexusEndpointTarget_Worker_
	//	*NexusEndpointTarget_External_
	Variant       isNexusEndpointTarget_Variant `protobuf_oneof:"variant"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpointTarget) Reset() {
	*x = NexusEndpointTarget{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpointTarget) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointTarget) ProtoMessage() {}

func (x *NexusEndpointTarget) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpointTarget.ProtoReflect.Descriptor instead.
func (*NexusEndpointTarget) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{1}
}

func (x *NexusEndpointTarget) GetVariant() isNexusEndpointTarget_Variant {
	if x != nil {
		return x.Variant
	}
	return nil
}

func (x *NexusEndpointTarget) GetWorker() *NexusEndpointTarget_Worker {
	if x != nil {
		if x, ok := x.Variant.(*NexusEndpointTarget_Worker_); ok {
			return x.Worker
		}
	}
	return nil
}

func (x *NexusEndpointTarget) GetExternal() *NexusEndpointTarget_External {
	if x != nil {
		if x, ok := x.Variant.(*NexusEndpointTarget_External_); ok {
			return x.External
		}
	}
	return nil
}

type isNexusEndpointTarget_Variant interface {
	isNexusEndpointTarget_Variant()
}

type NexusEndpointTarget_Worker_ struct {
	Worker *NexusEndpointTarget_Worker `protobuf:"bytes,1,opt,name=worker,proto3,oneof"`
}

type NexusEndpointTarget_External_ struct {
	External *NexusEndpointTarget_External `protobuf:"bytes,2,opt,name=external,proto3,oneof"`
}

func (*NexusEndpointTarget_Worker_) isNexusEndpointTarget_Variant() {}

func (*NexusEndpointTarget_External_) isNexusEndpointTarget_Variant() {}

type NexusEndpoint struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The last recorded cluster-local Hybrid Logical Clock timestamp for _this_ endpoint.
	// Updated whenever the endpoint is directly updated due to a user action but not when applying replication events.
	// The clock is referenced when new timestamps are generated to ensure it produces monotonically increasing
	// timestamps.
	Clock *v11.HybridLogicalClock `protobuf:"bytes,1,opt,name=clock,proto3" json:"clock,omitempty"`
	// Endpoint specification. This is a mirror of the public API and is intended to be mutable.
	Spec *NexusEndpointSpec `protobuf:"bytes,2,opt,name=spec,proto3" json:"spec,omitempty"`
	// The date and time when the endpoint was created.
	// (-- api-linter: core::0142::time-field-names=disabled
	//
	//	aip.dev/not-precedent: Not following linter rules. --)
	CreatedTime   *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=created_time,json=createdTime,proto3" json:"created_time,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpoint) Reset() {
	*x = NexusEndpoint{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpoint) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpoint) ProtoMessage() {}

func (x *NexusEndpoint) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[2]
	if x != nil {
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
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{2}
}

func (x *NexusEndpoint) GetClock() *v11.HybridLogicalClock {
	if x != nil {
		return x.Clock
	}
	return nil
}

func (x *NexusEndpoint) GetSpec() *NexusEndpointSpec {
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
	state         protoimpl.MessageState `protogen:"open.v1"`
	Version       int64                  `protobuf:"varint,1,opt,name=version,proto3" json:"version,omitempty"`
	Id            string                 `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Endpoint      *NexusEndpoint         `protobuf:"bytes,3,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpointEntry) Reset() {
	*x = NexusEndpointEntry{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpointEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointEntry) ProtoMessage() {}

func (x *NexusEndpointEntry) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[3]
	if x != nil {
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
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{3}
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

// Target a worker polling on a Nexus task queue in a specific namespace.
type NexusEndpointTarget_Worker struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Namespace ID to route requests to.
	NamespaceId string `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	// Nexus task queue to route requests to.
	TaskQueue     string `protobuf:"bytes,2,opt,name=task_queue,json=taskQueue,proto3" json:"task_queue,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpointTarget_Worker) Reset() {
	*x = NexusEndpointTarget_Worker{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpointTarget_Worker) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointTarget_Worker) ProtoMessage() {}

func (x *NexusEndpointTarget_Worker) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpointTarget_Worker.ProtoReflect.Descriptor instead.
func (*NexusEndpointTarget_Worker) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{1, 0}
}

func (x *NexusEndpointTarget_Worker) GetNamespaceId() string {
	if x != nil {
		return x.NamespaceId
	}
	return ""
}

func (x *NexusEndpointTarget_Worker) GetTaskQueue() string {
	if x != nil {
		return x.TaskQueue
	}
	return ""
}

// Target an external server by URL.
// At a later point, this will support providing credentials, in the meantime, an http.RoundTripper can be injected
// into the server to modify the request.
type NexusEndpointTarget_External struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// URL to call.
	// (-- api-linter: core::0140::uri=disabled
	//
	//	aip.dev/not-precedent: Not following linter rules. --)
	Url           string `protobuf:"bytes,1,opt,name=url,proto3" json:"url,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NexusEndpointTarget_External) Reset() {
	*x = NexusEndpointTarget_External{}
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NexusEndpointTarget_External) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NexusEndpointTarget_External) ProtoMessage() {}

func (x *NexusEndpointTarget_External) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NexusEndpointTarget_External.ProtoReflect.Descriptor instead.
func (*NexusEndpointTarget_External) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP(), []int{1, 1}
}

func (x *NexusEndpointTarget_External) GetUrl() string {
	if x != nil {
		return x.Url
	}
	return ""
}

var File_temporal_server_api_persistence_v1_nexus_proto protoreflect.FileDescriptor

const file_temporal_server_api_persistence_v1_nexus_proto_rawDesc = "" +
	"\n" +
	".temporal/server/api/persistence/v1/nexus.proto\x12\"temporal.server.api.persistence.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a$temporal/api/common/v1/message.proto\x1a*temporal/server/api/clock/v1/message.proto\"\xbb\x01\n" +
	"\x11NexusEndpointSpec\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12A\n" +
	"\vdescription\x18\x02 \x01(\v2\x1f.temporal.api.common.v1.PayloadR\vdescription\x12O\n" +
	"\x06target\x18\x03 \x01(\v27.temporal.server.api.persistence.v1.NexusEndpointTargetR\x06target\"\xc4\x02\n" +
	"\x13NexusEndpointTarget\x12X\n" +
	"\x06worker\x18\x01 \x01(\v2>.temporal.server.api.persistence.v1.NexusEndpointTarget.WorkerH\x00R\x06worker\x12^\n" +
	"\bexternal\x18\x02 \x01(\v2@.temporal.server.api.persistence.v1.NexusEndpointTarget.ExternalH\x00R\bexternal\x1aJ\n" +
	"\x06Worker\x12!\n" +
	"\fnamespace_id\x18\x01 \x01(\tR\vnamespaceId\x12\x1d\n" +
	"\n" +
	"task_queue\x18\x02 \x01(\tR\ttaskQueue\x1a\x1c\n" +
	"\bExternal\x12\x10\n" +
	"\x03url\x18\x01 \x01(\tR\x03urlB\t\n" +
	"\avariant\"\xe1\x01\n" +
	"\rNexusEndpoint\x12F\n" +
	"\x05clock\x18\x01 \x01(\v20.temporal.server.api.clock.v1.HybridLogicalClockR\x05clock\x12I\n" +
	"\x04spec\x18\x02 \x01(\v25.temporal.server.api.persistence.v1.NexusEndpointSpecR\x04spec\x12=\n" +
	"\fcreated_time\x18\x03 \x01(\v2\x1a.google.protobuf.TimestampR\vcreatedTime\"\x8d\x01\n" +
	"\x12NexusEndpointEntry\x12\x18\n" +
	"\aversion\x18\x01 \x01(\x03R\aversion\x12\x0e\n" +
	"\x02id\x18\x02 \x01(\tR\x02id\x12M\n" +
	"\bendpoint\x18\x03 \x01(\v21.temporal.server.api.persistence.v1.NexusEndpointR\bendpointB6Z4go.temporal.io/server/api/persistence/v1;persistenceb\x06proto3"

var (
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescOnce sync.Once
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescData []byte
)

func file_temporal_server_api_persistence_v1_nexus_proto_rawDescGZIP() []byte {
	file_temporal_server_api_persistence_v1_nexus_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_persistence_v1_nexus_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_nexus_proto_rawDesc), len(file_temporal_server_api_persistence_v1_nexus_proto_rawDesc)))
	})
	return file_temporal_server_api_persistence_v1_nexus_proto_rawDescData
}

var file_temporal_server_api_persistence_v1_nexus_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_temporal_server_api_persistence_v1_nexus_proto_goTypes = []any{
	(*NexusEndpointSpec)(nil),            // 0: temporal.server.api.persistence.v1.NexusEndpointSpec
	(*NexusEndpointTarget)(nil),          // 1: temporal.server.api.persistence.v1.NexusEndpointTarget
	(*NexusEndpoint)(nil),                // 2: temporal.server.api.persistence.v1.NexusEndpoint
	(*NexusEndpointEntry)(nil),           // 3: temporal.server.api.persistence.v1.NexusEndpointEntry
	(*NexusEndpointTarget_Worker)(nil),   // 4: temporal.server.api.persistence.v1.NexusEndpointTarget.Worker
	(*NexusEndpointTarget_External)(nil), // 5: temporal.server.api.persistence.v1.NexusEndpointTarget.External
	(*v1.Payload)(nil),                   // 6: temporal.api.common.v1.Payload
	(*v11.HybridLogicalClock)(nil),       // 7: temporal.server.api.clock.v1.HybridLogicalClock
	(*timestamppb.Timestamp)(nil),        // 8: google.protobuf.Timestamp
}
var file_temporal_server_api_persistence_v1_nexus_proto_depIdxs = []int32{
	6, // 0: temporal.server.api.persistence.v1.NexusEndpointSpec.description:type_name -> temporal.api.common.v1.Payload
	1, // 1: temporal.server.api.persistence.v1.NexusEndpointSpec.target:type_name -> temporal.server.api.persistence.v1.NexusEndpointTarget
	4, // 2: temporal.server.api.persistence.v1.NexusEndpointTarget.worker:type_name -> temporal.server.api.persistence.v1.NexusEndpointTarget.Worker
	5, // 3: temporal.server.api.persistence.v1.NexusEndpointTarget.external:type_name -> temporal.server.api.persistence.v1.NexusEndpointTarget.External
	7, // 4: temporal.server.api.persistence.v1.NexusEndpoint.clock:type_name -> temporal.server.api.clock.v1.HybridLogicalClock
	0, // 5: temporal.server.api.persistence.v1.NexusEndpoint.spec:type_name -> temporal.server.api.persistence.v1.NexusEndpointSpec
	8, // 6: temporal.server.api.persistence.v1.NexusEndpoint.created_time:type_name -> google.protobuf.Timestamp
	2, // 7: temporal.server.api.persistence.v1.NexusEndpointEntry.endpoint:type_name -> temporal.server.api.persistence.v1.NexusEndpoint
	8, // [8:8] is the sub-list for method output_type
	8, // [8:8] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_temporal_server_api_persistence_v1_nexus_proto_init() }
func file_temporal_server_api_persistence_v1_nexus_proto_init() {
	if File_temporal_server_api_persistence_v1_nexus_proto != nil {
		return
	}
	file_temporal_server_api_persistence_v1_nexus_proto_msgTypes[1].OneofWrappers = []any{
		(*NexusEndpointTarget_Worker_)(nil),
		(*NexusEndpointTarget_External_)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_persistence_v1_nexus_proto_rawDesc), len(file_temporal_server_api_persistence_v1_nexus_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_persistence_v1_nexus_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_persistence_v1_nexus_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_persistence_v1_nexus_proto_msgTypes,
	}.Build()
	File_temporal_server_api_persistence_v1_nexus_proto = out.File
	file_temporal_server_api_persistence_v1_nexus_proto_goTypes = nil
	file_temporal_server_api_persistence_v1_nexus_proto_depIdxs = nil
}
