// The MIT License
//
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
// source: temporal/server/api/matchingservice/v1/service.proto

package matchingservice

import (
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_temporal_server_api_matchingservice_v1_service_proto protoreflect.FileDescriptor

var file_temporal_server_api_matchingservice_v1_service_proto_rawDesc = []byte{
	0x0a, 0x34, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2f, 0x76, 0x31, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x26, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x1a, 0x1c,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x3d, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70,
	0x69, 0x2f, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x2f, 0x76, 0x31, 0x2f, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x5f, 0x72, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x32, 0xe2, 0x1a, 0x0a, 0x0f,
	0x4d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12,
	0xa6, 0x01, 0x0a, 0x15, 0x50, 0x6f, 0x6c, 0x6c, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77,
	0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x44, 0x2e, 0x74, 0x65, 0x6d, 0x70,
	0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x50, 0x6f, 0x6c, 0x6c, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54,
	0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x45, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x6f, 0x6c, 0x6c, 0x57, 0x6f, 0x72,
	0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xa6, 0x01, 0x0a, 0x15, 0x50, 0x6f, 0x6c,
	0x6c, 0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x12, 0x44, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e,
	0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x6f, 0x6c, 0x6c,
	0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x45, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d,
	0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x50, 0x6f, 0x6c, 0x6c, 0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x61,
	0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x00, 0x12, 0x94, 0x01, 0x0a, 0x0f, 0x41, 0x64, 0x64, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x3e, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x41,
	0x64, 0x64, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x3f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x41,
	0x64, 0x64, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x94, 0x01, 0x0a, 0x0f, 0x41, 0x64, 0x64,
	0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x12, 0x3e, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x64, 0x64, 0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74,
	0x79, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x3f, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x64, 0x64, 0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74,
	0x79, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x8e, 0x01, 0x0a, 0x0d, 0x51, 0x75, 0x65, 0x72, 0x79, 0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f,
	0x77, 0x12, 0x3c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79,
	0x57, 0x6f, 0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x3d, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x57, 0x6f,
	0x72, 0x6b, 0x66, 0x6c, 0x6f, 0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x12, 0xb2, 0x01, 0x0a, 0x19, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x64, 0x51, 0x75, 0x65, 0x72,
	0x79, 0x54, 0x61, 0x73, 0x6b, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x12, 0x48,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x64, 0x51,
	0x75, 0x65, 0x72, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65,
	0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x49, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f,
	0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d,
	0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x64, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x61,
	0x73, 0x6b, 0x43, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xa6, 0x01, 0x0a, 0x15, 0x43, 0x61, 0x6e, 0x63, 0x65, 0x6c,
	0x4f, 0x75, 0x74, 0x73, 0x74, 0x61, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x50, 0x6f, 0x6c, 0x6c, 0x12,
	0x44, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x4f,
	0x75, 0x74, 0x73, 0x74, 0x61, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x50, 0x6f, 0x6c, 0x6c, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x45, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43,
	0x61, 0x6e, 0x63, 0x65, 0x6c, 0x4f, 0x75, 0x74, 0x73, 0x74, 0x61, 0x6e, 0x64, 0x69, 0x6e, 0x67,
	0x50, 0x6f, 0x6c, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x9a,
	0x01, 0x0a, 0x11, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x12, 0x40, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68,
	0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x65,
	0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x41, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74,
	0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xac, 0x01, 0x0a, 0x17,
	0x4c, 0x69, 0x73, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x46, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61,
	0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31,
	0x2e, 0x4c, 0x69, 0x73, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x47, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xc7, 0x01, 0x0a, 0x20, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64,
	0x49, 0x64, 0x43, 0x6f, 0x6d, 0x70, 0x61, 0x74, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x12,
	0x4f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x57,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x43, 0x6f, 0x6d, 0x70,
	0x61, 0x74, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x50, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x43, 0x6f, 0x6d,
	0x70, 0x61, 0x74, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x00, 0x12, 0xbe, 0x01, 0x0a, 0x1d, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b,
	0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x43, 0x6f, 0x6d, 0x70, 0x61, 0x74, 0x69,
	0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x12, 0x4c, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61,
	0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74,
	0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64,
	0x43, 0x6f, 0x6d, 0x70, 0x61, 0x74, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x4d, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68,
	0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65,
	0x74, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x43, 0x6f,
	0x6d, 0x70, 0x61, 0x74, 0x69, 0x62, 0x69, 0x6c, 0x69, 0x74, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xa3, 0x01, 0x0a, 0x14, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73,
	0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x12, 0x43,
	0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x51,
	0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x44, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69,
	0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74,
	0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74,
	0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xb8, 0x01, 0x0a, 0x1b,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x52, 0x75, 0x6c, 0x65, 0x73, 0x12, 0x4a, 0x2e, 0x74, 0x65,
	0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x52, 0x75, 0x6c, 0x65, 0x73,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x4b, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61,
	0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31,
	0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x52, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x86, 0x02, 0x0a, 0x18, 0x47, 0x65, 0x74, 0x57, 0x6f,
	0x72, 0x6b, 0x65, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x52, 0x75,
	0x6c, 0x65, 0x73, 0x12, 0x47, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73,
	0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69,
	0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74,
	0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67,
	0x52, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x48, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x57, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x56,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x52, 0x75, 0x6c, 0x65, 0x73, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x57, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x51, 0x12, 0x4f,
	0x2f, 0x61, 0x70, 0x69, 0x2f, 0x76, 0x31, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x73, 0x2f, 0x7b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x7d, 0x2f, 0x74,
	0x61, 0x73, 0x6b, 0x2d, 0x71, 0x75, 0x65, 0x75, 0x65, 0x73, 0x2f, 0x7b, 0x74, 0x61, 0x73, 0x6b,
	0x5f, 0x71, 0x75, 0x65, 0x75, 0x65, 0x7d, 0x2f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2d, 0x76,
	0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x69, 0x6e, 0x67, 0x2d, 0x72, 0x75, 0x6c, 0x65, 0x73, 0x12,
	0xd9, 0x01, 0x0a, 0x26, 0x41, 0x70, 0x70, 0x6c, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x55, 0x2e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x2e, 0x76, 0x31, 0x2e, 0x41, 0x70, 0x70, 0x6c, 0x79, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x56, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x70, 0x70, 0x6c, 0x79,
	0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74,
	0x61, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x45, 0x76, 0x65, 0x6e,
	0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xb5, 0x01, 0x0a, 0x1a,
	0x47, 0x65, 0x74, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x4d, 0x61, 0x70, 0x70, 0x69, 0x6e, 0x67, 0x12, 0x49, 0x2e, 0x74, 0x65, 0x6d,
	0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x2e, 0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x54, 0x61,
	0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x4d, 0x61, 0x70, 0x70, 0x69, 0x6e, 0x67, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x4a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x47,
	0x65, 0x74, 0x42, 0x75, 0x69, 0x6c, 0x64, 0x49, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65,
	0x75, 0x65, 0x4d, 0x61, 0x70, 0x70, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x00, 0x12, 0xa3, 0x01, 0x0a, 0x14, 0x46, 0x6f, 0x72, 0x63, 0x65, 0x55, 0x6e, 0x6c,
	0x6f, 0x61, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x12, 0x43, 0x2e, 0x74,
	0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61,
	0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x6f, 0x72, 0x63, 0x65, 0x55, 0x6e, 0x6c, 0x6f, 0x61,
	0x64, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x44, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72,
	0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x6f, 0x72, 0x63, 0x65,
	0x55, 0x6e, 0x6c, 0x6f, 0x61, 0x64, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xac, 0x01, 0x0a, 0x17, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65,
	0x72, 0x44, 0x61, 0x74, 0x61, 0x12, 0x46, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c,
	0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x55,
	0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73,
	0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x47, 0x2e,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73,
	0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xb5, 0x01, 0x0a, 0x1a, 0x52, 0x65, 0x70,
	0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55,
	0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x12, 0x49, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72,
	0x61, 0x6c, 0x2e, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61,
	0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75,
	0x65, 0x75, 0x65, 0x55, 0x73, 0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x4a, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e,
	0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x70, 0x6c,
	0x69, 0x63, 0x61, 0x74, 0x65, 0x54, 0x61, 0x73, 0x6b, 0x51, 0x75, 0x65, 0x75, 0x65, 0x55, 0x73,
	0x65, 0x72, 0x44, 0x61, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x42, 0x3e, 0x5a, 0x3c, 0x67, 0x6f, 0x2e, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x6c, 0x2e,
	0x69, 0x6f, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x6d, 0x61,
	0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2f, 0x76, 0x31,
	0x3b, 0x6d, 0x61, 0x74, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_temporal_server_api_matchingservice_v1_service_proto_goTypes = []interface{}{
	(*PollWorkflowTaskQueueRequest)(nil),                   // 0: temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueRequest
	(*PollActivityTaskQueueRequest)(nil),                   // 1: temporal.server.api.matchingservice.v1.PollActivityTaskQueueRequest
	(*AddWorkflowTaskRequest)(nil),                         // 2: temporal.server.api.matchingservice.v1.AddWorkflowTaskRequest
	(*AddActivityTaskRequest)(nil),                         // 3: temporal.server.api.matchingservice.v1.AddActivityTaskRequest
	(*QueryWorkflowRequest)(nil),                           // 4: temporal.server.api.matchingservice.v1.QueryWorkflowRequest
	(*RespondQueryTaskCompletedRequest)(nil),               // 5: temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedRequest
	(*CancelOutstandingPollRequest)(nil),                   // 6: temporal.server.api.matchingservice.v1.CancelOutstandingPollRequest
	(*DescribeTaskQueueRequest)(nil),                       // 7: temporal.server.api.matchingservice.v1.DescribeTaskQueueRequest
	(*ListTaskQueuePartitionsRequest)(nil),                 // 8: temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsRequest
	(*UpdateWorkerBuildIdCompatibilityRequest)(nil),        // 9: temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityRequest
	(*GetWorkerBuildIdCompatibilityRequest)(nil),           // 10: temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityRequest
	(*GetTaskQueueUserDataRequest)(nil),                    // 11: temporal.server.api.matchingservice.v1.GetTaskQueueUserDataRequest
	(*UpdateWorkerVersioningRulesRequest)(nil),             // 12: temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesRequest
	(*GetWorkerVersioningRulesRequest)(nil),                // 13: temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesRequest
	(*ApplyTaskQueueUserDataReplicationEventRequest)(nil),  // 14: temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventRequest
	(*GetBuildIdTaskQueueMappingRequest)(nil),              // 15: temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingRequest
	(*ForceUnloadTaskQueueRequest)(nil),                    // 16: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueRequest
	(*UpdateTaskQueueUserDataRequest)(nil),                 // 17: temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataRequest
	(*ReplicateTaskQueueUserDataRequest)(nil),              // 18: temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataRequest
	(*PollWorkflowTaskQueueResponse)(nil),                  // 19: temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueResponse
	(*PollActivityTaskQueueResponse)(nil),                  // 20: temporal.server.api.matchingservice.v1.PollActivityTaskQueueResponse
	(*AddWorkflowTaskResponse)(nil),                        // 21: temporal.server.api.matchingservice.v1.AddWorkflowTaskResponse
	(*AddActivityTaskResponse)(nil),                        // 22: temporal.server.api.matchingservice.v1.AddActivityTaskResponse
	(*QueryWorkflowResponse)(nil),                          // 23: temporal.server.api.matchingservice.v1.QueryWorkflowResponse
	(*RespondQueryTaskCompletedResponse)(nil),              // 24: temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedResponse
	(*CancelOutstandingPollResponse)(nil),                  // 25: temporal.server.api.matchingservice.v1.CancelOutstandingPollResponse
	(*DescribeTaskQueueResponse)(nil),                      // 26: temporal.server.api.matchingservice.v1.DescribeTaskQueueResponse
	(*ListTaskQueuePartitionsResponse)(nil),                // 27: temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsResponse
	(*UpdateWorkerBuildIdCompatibilityResponse)(nil),       // 28: temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityResponse
	(*GetWorkerBuildIdCompatibilityResponse)(nil),          // 29: temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityResponse
	(*GetTaskQueueUserDataResponse)(nil),                   // 30: temporal.server.api.matchingservice.v1.GetTaskQueueUserDataResponse
	(*UpdateWorkerVersioningRulesResponse)(nil),            // 31: temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesResponse
	(*GetWorkerVersioningRulesResponse)(nil),               // 32: temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesResponse
	(*ApplyTaskQueueUserDataReplicationEventResponse)(nil), // 33: temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventResponse
	(*GetBuildIdTaskQueueMappingResponse)(nil),             // 34: temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingResponse
	(*ForceUnloadTaskQueueResponse)(nil),                   // 35: temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueResponse
	(*UpdateTaskQueueUserDataResponse)(nil),                // 36: temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataResponse
	(*ReplicateTaskQueueUserDataResponse)(nil),             // 37: temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataResponse
}
var file_temporal_server_api_matchingservice_v1_service_proto_depIdxs = []int32{
	0,  // 0: temporal.server.api.matchingservice.v1.MatchingService.PollWorkflowTaskQueue:input_type -> temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueRequest
	1,  // 1: temporal.server.api.matchingservice.v1.MatchingService.PollActivityTaskQueue:input_type -> temporal.server.api.matchingservice.v1.PollActivityTaskQueueRequest
	2,  // 2: temporal.server.api.matchingservice.v1.MatchingService.AddWorkflowTask:input_type -> temporal.server.api.matchingservice.v1.AddWorkflowTaskRequest
	3,  // 3: temporal.server.api.matchingservice.v1.MatchingService.AddActivityTask:input_type -> temporal.server.api.matchingservice.v1.AddActivityTaskRequest
	4,  // 4: temporal.server.api.matchingservice.v1.MatchingService.QueryWorkflow:input_type -> temporal.server.api.matchingservice.v1.QueryWorkflowRequest
	5,  // 5: temporal.server.api.matchingservice.v1.MatchingService.RespondQueryTaskCompleted:input_type -> temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedRequest
	6,  // 6: temporal.server.api.matchingservice.v1.MatchingService.CancelOutstandingPoll:input_type -> temporal.server.api.matchingservice.v1.CancelOutstandingPollRequest
	7,  // 7: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueue:input_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueueRequest
	8,  // 8: temporal.server.api.matchingservice.v1.MatchingService.ListTaskQueuePartitions:input_type -> temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsRequest
	9,  // 9: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerBuildIdCompatibility:input_type -> temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityRequest
	10, // 10: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerBuildIdCompatibility:input_type -> temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityRequest
	11, // 11: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.GetTaskQueueUserDataRequest
	12, // 12: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerVersioningRules:input_type -> temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesRequest
	13, // 13: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerVersioningRules:input_type -> temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesRequest
	14, // 14: temporal.server.api.matchingservice.v1.MatchingService.ApplyTaskQueueUserDataReplicationEvent:input_type -> temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventRequest
	15, // 15: temporal.server.api.matchingservice.v1.MatchingService.GetBuildIdTaskQueueMapping:input_type -> temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingRequest
	16, // 16: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueue:input_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueRequest
	17, // 17: temporal.server.api.matchingservice.v1.MatchingService.UpdateTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataRequest
	18, // 18: temporal.server.api.matchingservice.v1.MatchingService.ReplicateTaskQueueUserData:input_type -> temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataRequest
	19, // 19: temporal.server.api.matchingservice.v1.MatchingService.PollWorkflowTaskQueue:output_type -> temporal.server.api.matchingservice.v1.PollWorkflowTaskQueueResponse
	20, // 20: temporal.server.api.matchingservice.v1.MatchingService.PollActivityTaskQueue:output_type -> temporal.server.api.matchingservice.v1.PollActivityTaskQueueResponse
	21, // 21: temporal.server.api.matchingservice.v1.MatchingService.AddWorkflowTask:output_type -> temporal.server.api.matchingservice.v1.AddWorkflowTaskResponse
	22, // 22: temporal.server.api.matchingservice.v1.MatchingService.AddActivityTask:output_type -> temporal.server.api.matchingservice.v1.AddActivityTaskResponse
	23, // 23: temporal.server.api.matchingservice.v1.MatchingService.QueryWorkflow:output_type -> temporal.server.api.matchingservice.v1.QueryWorkflowResponse
	24, // 24: temporal.server.api.matchingservice.v1.MatchingService.RespondQueryTaskCompleted:output_type -> temporal.server.api.matchingservice.v1.RespondQueryTaskCompletedResponse
	25, // 25: temporal.server.api.matchingservice.v1.MatchingService.CancelOutstandingPoll:output_type -> temporal.server.api.matchingservice.v1.CancelOutstandingPollResponse
	26, // 26: temporal.server.api.matchingservice.v1.MatchingService.DescribeTaskQueue:output_type -> temporal.server.api.matchingservice.v1.DescribeTaskQueueResponse
	27, // 27: temporal.server.api.matchingservice.v1.MatchingService.ListTaskQueuePartitions:output_type -> temporal.server.api.matchingservice.v1.ListTaskQueuePartitionsResponse
	28, // 28: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerBuildIdCompatibility:output_type -> temporal.server.api.matchingservice.v1.UpdateWorkerBuildIdCompatibilityResponse
	29, // 29: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerBuildIdCompatibility:output_type -> temporal.server.api.matchingservice.v1.GetWorkerBuildIdCompatibilityResponse
	30, // 30: temporal.server.api.matchingservice.v1.MatchingService.GetTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.GetTaskQueueUserDataResponse
	31, // 31: temporal.server.api.matchingservice.v1.MatchingService.UpdateWorkerVersioningRules:output_type -> temporal.server.api.matchingservice.v1.UpdateWorkerVersioningRulesResponse
	32, // 32: temporal.server.api.matchingservice.v1.MatchingService.GetWorkerVersioningRules:output_type -> temporal.server.api.matchingservice.v1.GetWorkerVersioningRulesResponse
	33, // 33: temporal.server.api.matchingservice.v1.MatchingService.ApplyTaskQueueUserDataReplicationEvent:output_type -> temporal.server.api.matchingservice.v1.ApplyTaskQueueUserDataReplicationEventResponse
	34, // 34: temporal.server.api.matchingservice.v1.MatchingService.GetBuildIdTaskQueueMapping:output_type -> temporal.server.api.matchingservice.v1.GetBuildIdTaskQueueMappingResponse
	35, // 35: temporal.server.api.matchingservice.v1.MatchingService.ForceUnloadTaskQueue:output_type -> temporal.server.api.matchingservice.v1.ForceUnloadTaskQueueResponse
	36, // 36: temporal.server.api.matchingservice.v1.MatchingService.UpdateTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.UpdateTaskQueueUserDataResponse
	37, // 37: temporal.server.api.matchingservice.v1.MatchingService.ReplicateTaskQueueUserData:output_type -> temporal.server.api.matchingservice.v1.ReplicateTaskQueueUserDataResponse
	19, // [19:38] is the sub-list for method output_type
	0,  // [0:19] is the sub-list for method input_type
	0,  // [0:0] is the sub-list for extension type_name
	0,  // [0:0] is the sub-list for extension extendee
	0,  // [0:0] is the sub-list for field type_name
}

func init() { file_temporal_server_api_matchingservice_v1_service_proto_init() }
func file_temporal_server_api_matchingservice_v1_service_proto_init() {
	if File_temporal_server_api_matchingservice_v1_service_proto != nil {
		return
	}
	file_temporal_server_api_matchingservice_v1_request_response_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_temporal_server_api_matchingservice_v1_service_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_temporal_server_api_matchingservice_v1_service_proto_goTypes,
		DependencyIndexes: file_temporal_server_api_matchingservice_v1_service_proto_depIdxs,
	}.Build()
	File_temporal_server_api_matchingservice_v1_service_proto = out.File
	file_temporal_server_api_matchingservice_v1_service_proto_rawDesc = nil
	file_temporal_server_api_matchingservice_v1_service_proto_goTypes = nil
	file_temporal_server_api_matchingservice_v1_service_proto_depIdxs = nil
}
