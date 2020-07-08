// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/enums/v1/indexer.proto

package enums

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	math "math"
	strconv "strconv"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type MessageType int32

const (
	MESSAGE_TYPE_UNSPECIFIED MessageType = 0
	MESSAGE_TYPE_INDEX       MessageType = 1
	MESSAGE_TYPE_DELETE      MessageType = 2
)

var MessageType_name = map[int32]string{
	0: "Unspecified",
	1: "Index",
	2: "Delete",
}

var MessageType_value = map[string]int32{
	"Unspecified": 0,
	"Index":       1,
	"Delete":      2,
}

func (MessageType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_5f4b8ce5d6f2b050, []int{0}
}

type FieldType int32

const (
	FIELD_TYPE_UNSPECIFIED FieldType = 0
	FIELD_TYPE_STRING      FieldType = 1
	FIELD_TYPE_INT         FieldType = 2
	FIELD_TYPE_BOOL        FieldType = 3
	FIELD_TYPE_BINARY      FieldType = 4
)

var FieldType_name = map[int32]string{
	0: "Unspecified",
	1: "String",
	2: "Int",
	3: "Bool",
	4: "Binary",
}

var FieldType_value = map[string]int32{
	"Unspecified": 0,
	"String":      1,
	"Int":         2,
	"Bool":        3,
	"Binary":      4,
}

func (FieldType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_5f4b8ce5d6f2b050, []int{1}
}

func init() {
	proto.RegisterEnum("temporal.server.api.enums.v1.MessageType", MessageType_name, MessageType_value)
	proto.RegisterEnum("temporal.server.api.enums.v1.FieldType", FieldType_name, FieldType_value)
}

func init() {
	proto.RegisterFile("temporal/server/api/enums/v1/indexer.proto", fileDescriptor_5f4b8ce5d6f2b050)
}

var fileDescriptor_5f4b8ce5d6f2b050 = []byte{
	// 302 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xd2, 0x2a, 0x49, 0xcd, 0x2d,
	0xc8, 0x2f, 0x4a, 0xcc, 0xd1, 0x2f, 0x4e, 0x2d, 0x2a, 0x4b, 0x2d, 0xd2, 0x4f, 0x2c, 0xc8, 0xd4,
	0x4f, 0xcd, 0x2b, 0xcd, 0x2d, 0xd6, 0x2f, 0x33, 0xd4, 0xcf, 0xcc, 0x4b, 0x49, 0xad, 0x48, 0x2d,
	0xd2, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x92, 0x81, 0xa9, 0xd5, 0x83, 0xa8, 0xd5, 0x4b, 0x2c,
	0xc8, 0xd4, 0x03, 0xab, 0xd5, 0x2b, 0x33, 0xd4, 0x8a, 0xe1, 0xe2, 0xf6, 0x4d, 0x2d, 0x2e, 0x4e,
	0x4c, 0x4f, 0x0d, 0xa9, 0x2c, 0x48, 0x15, 0x92, 0xe1, 0x92, 0xf0, 0x75, 0x0d, 0x0e, 0x76, 0x74,
	0x77, 0x8d, 0x0f, 0x89, 0x0c, 0x70, 0x8d, 0x0f, 0xf5, 0x0b, 0x0e, 0x70, 0x75, 0xf6, 0x74, 0xf3,
	0x74, 0x75, 0x11, 0x60, 0x10, 0x12, 0xe3, 0x12, 0x42, 0x91, 0xf5, 0xf4, 0x73, 0x71, 0x8d, 0x10,
	0x60, 0x14, 0x12, 0xe7, 0x12, 0x46, 0x11, 0x77, 0x71, 0xf5, 0x71, 0x0d, 0x71, 0x15, 0x60, 0xd2,
	0xaa, 0xe3, 0xe2, 0x74, 0xcb, 0x4c, 0xcd, 0x49, 0x01, 0x9b, 0x2d, 0xc5, 0x25, 0xe6, 0xe6, 0xe9,
	0xea, 0xe3, 0x82, 0xcd, 0x64, 0x51, 0x2e, 0x41, 0x24, 0xb9, 0xe0, 0x90, 0x20, 0x4f, 0x3f, 0x77,
	0x01, 0x46, 0x21, 0x21, 0x2e, 0x3e, 0x24, 0x61, 0x4f, 0xbf, 0x10, 0x01, 0x26, 0x21, 0x61, 0x2e,
	0x7e, 0x24, 0x31, 0x27, 0x7f, 0x7f, 0x1f, 0x01, 0x66, 0x34, 0xfd, 0x4e, 0x9e, 0x7e, 0x8e, 0x41,
	0x91, 0x02, 0x2c, 0x4e, 0x71, 0x17, 0x1e, 0xca, 0x31, 0xdc, 0x78, 0x28, 0xc7, 0xf0, 0xe1, 0xa1,
	0x1c, 0x63, 0xc3, 0x23, 0x39, 0xc6, 0x15, 0x8f, 0xe4, 0x18, 0x4f, 0x3c, 0x92, 0x63, 0xbc, 0xf0,
	0x48, 0x8e, 0xf1, 0xc1, 0x23, 0x39, 0xc6, 0x17, 0x8f, 0xe4, 0x18, 0x3e, 0x3c, 0x92, 0x63, 0x9c,
	0xf0, 0x58, 0x8e, 0xe1, 0xc2, 0x63, 0x39, 0x86, 0x1b, 0x8f, 0xe5, 0x18, 0xa2, 0x34, 0xd2, 0xf3,
	0xf5, 0xe0, 0x81, 0x96, 0x99, 0x8f, 0x2d, 0x8c, 0xad, 0xc1, 0x8c, 0x24, 0x36, 0x70, 0x10, 0x1b,
	0x03, 0x02, 0x00, 0x00, 0xff, 0xff, 0x4c, 0xe4, 0x91, 0x22, 0x90, 0x01, 0x00, 0x00,
}

func (x MessageType) String() string {
	s, ok := MessageType_name[int32(x)]
	if ok {
		return s
	}
	return strconv.Itoa(int(x))
}
func (x FieldType) String() string {
	s, ok := FieldType_name[int32(x)]
	if ok {
		return s
	}
	return strconv.Itoa(int(x))
}
