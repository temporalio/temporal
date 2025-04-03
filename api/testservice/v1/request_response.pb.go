// The MIT License
//
// Copyright (c) 2025 Temporal Technologies, Inc.
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
// source: temporal/server/api/testservice/v1/request_response.proto

package testservice

import (
	reflect "reflect"
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

type SendHelloRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SendHelloRequest) Reset() {
	*x = SendHelloRequest{}
	mi := &file_temporal_server_api_testservice_v1_request_response_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SendHelloRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendHelloRequest) ProtoMessage() {}

func (x *SendHelloRequest) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_testservice_v1_request_response_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SendHelloRequest.ProtoReflect.Descriptor instead.
func (*SendHelloRequest) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_testservice_v1_request_response_proto_rawDescGZIP(), []int{0}
}

func (x *SendHelloRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

type SendHelloResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Message       string                 `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SendHelloResponse) Reset() {
	*x = SendHelloResponse{}
	mi := &file_temporal_server_api_testservice_v1_request_response_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SendHelloResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendHelloResponse) ProtoMessage() {}

func (x *SendHelloResponse) ProtoReflect() protoreflect.Message {
	mi := &file_temporal_server_api_testservice_v1_request_response_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SendHelloResponse.ProtoReflect.Descriptor instead.
func (*SendHelloResponse) Descriptor() ([]byte, []int) {
	return file_temporal_server_api_testservice_v1_request_response_proto_rawDescGZIP(), []int{1}
}

func (x *SendHelloResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

var File_temporal_server_api_testservice_v1_request_response_proto protoreflect.FileDescriptor

const file_temporal_server_api_testservice_v1_request_response_proto_rawDesc = "" +
	"\n" +
	"9temporal/server/api/testservice/v1/request_response.proto\x12\"temporal.server.api.testservice.v1\"&\n" +
	"\x10SendHelloRequest\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\"-\n" +
	"\x11SendHelloResponse\x12\x18\n" +
	"\amessage\x18\x01 \x01(\tR\amessageB6Z4go.temporal.io/server/api/testservice/v1;testserviceb\x06proto3"

var (
	file_temporal_server_api_testservice_v1_request_response_proto_rawDescOnce sync.Once
	file_temporal_server_api_testservice_v1_request_response_proto_rawDescData []byte
)

func file_temporal_server_api_testservice_v1_request_response_proto_rawDescGZIP() []byte {
	file_temporal_server_api_testservice_v1_request_response_proto_rawDescOnce.Do(func() {
		file_temporal_server_api_testservice_v1_request_response_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_temporal_server_api_testservice_v1_request_response_proto_rawDesc), len(file_temporal_server_api_testservice_v1_request_response_proto_rawDesc)))
	})
	return file_temporal_server_api_testservice_v1_request_response_proto_rawDescData
}

var file_temporal_server_api_testservice_v1_request_response_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_temporal_server_api_testservice_v1_request_response_proto_goTypes = []any{
	(*SendHelloRequest)(nil),  // 0: temporal.server.api.testservice.v1.SendHelloRequest
	(*SendHelloResponse)(nil), // 1: temporal.server.api.testservice.v1.SendHelloResponse
}
var file_temporal_server_api_testservice_v1_request_response_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_testservice_v1_request_response_proto_init() }
func file_temporal_server_api_testservice_v1_request_response_proto_init() {
	if File_temporal_server_api_testservice_v1_request_response_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_temporal_server_api_testservice_v1_request_response_proto_rawDesc), len(file_temporal_server_api_testservice_v1_request_response_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_temporal_server_api_testservice_v1_request_response_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_testservice_v1_request_response_proto_depIdxs,
		MessageInfos:      file_temporal_server_api_testservice_v1_request_response_proto_msgTypes,
	}.Build()
	File_temporal_server_api_testservice_v1_request_response_proto = out.File
	file_temporal_server_api_testservice_v1_request_response_proto_goTypes = nil
	file_temporal_server_api_testservice_v1_request_response_proto_depIdxs = nil
}
