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

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/workflow/v1/message.proto

package workflow

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"

	proto "github.com/gogo/protobuf/proto"
	v1 "go.temporal.io/api/common/v1"
	v11 "go.temporal.io/server/api/clock/v1"
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

type ParentExecutionInfo struct {
	NamespaceId      string                `protobuf:"bytes,1,opt,name=namespace_id,json=namespaceId,proto3" json:"namespace_id,omitempty"`
	Namespace        string                `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
	Execution        *v1.WorkflowExecution `protobuf:"bytes,3,opt,name=execution,proto3" json:"execution,omitempty"`
	InitiatedId      int64                 `protobuf:"varint,4,opt,name=initiated_id,json=initiatedId,proto3" json:"initiated_id,omitempty"`
	Clock            *v11.VectorClock      `protobuf:"bytes,5,opt,name=clock,proto3" json:"clock,omitempty"`
	InitiatedVersion int64                 `protobuf:"varint,6,opt,name=initiated_version,json=initiatedVersion,proto3" json:"initiated_version,omitempty"`
}

func (m *ParentExecutionInfo) Reset()      { *m = ParentExecutionInfo{} }
func (*ParentExecutionInfo) ProtoMessage() {}
func (*ParentExecutionInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_c4f1ca48d03c9ded, []int{0}
}
func (m *ParentExecutionInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ParentExecutionInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ParentExecutionInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ParentExecutionInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ParentExecutionInfo.Merge(m, src)
}
func (m *ParentExecutionInfo) XXX_Size() int {
	return m.Size()
}
func (m *ParentExecutionInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_ParentExecutionInfo.DiscardUnknown(m)
}

var xxx_messageInfo_ParentExecutionInfo proto.InternalMessageInfo

func (m *ParentExecutionInfo) GetNamespaceId() string {
	if m != nil {
		return m.NamespaceId
	}
	return ""
}

func (m *ParentExecutionInfo) GetNamespace() string {
	if m != nil {
		return m.Namespace
	}
	return ""
}

func (m *ParentExecutionInfo) GetExecution() *v1.WorkflowExecution {
	if m != nil {
		return m.Execution
	}
	return nil
}

func (m *ParentExecutionInfo) GetInitiatedId() int64 {
	if m != nil {
		return m.InitiatedId
	}
	return 0
}

func (m *ParentExecutionInfo) GetClock() *v11.VectorClock {
	if m != nil {
		return m.Clock
	}
	return nil
}

func (m *ParentExecutionInfo) GetInitiatedVersion() int64 {
	if m != nil {
		return m.InitiatedVersion
	}
	return 0
}

type BaseExecutionInfo struct {
	RunId                            string `protobuf:"bytes,1,opt,name=run_id,json=runId,proto3" json:"run_id,omitempty"`
	LowestCommonAncestorEventId      int64  `protobuf:"varint,2,opt,name=lowest_common_ancestor_event_id,json=lowestCommonAncestorEventId,proto3" json:"lowest_common_ancestor_event_id,omitempty"`
	LowestCommonAncestorEventVersion int64  `protobuf:"varint,3,opt,name=lowest_common_ancestor_event_version,json=lowestCommonAncestorEventVersion,proto3" json:"lowest_common_ancestor_event_version,omitempty"`
}

func (m *BaseExecutionInfo) Reset()      { *m = BaseExecutionInfo{} }
func (*BaseExecutionInfo) ProtoMessage() {}
func (*BaseExecutionInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_c4f1ca48d03c9ded, []int{1}
}
func (m *BaseExecutionInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *BaseExecutionInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_BaseExecutionInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *BaseExecutionInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BaseExecutionInfo.Merge(m, src)
}
func (m *BaseExecutionInfo) XXX_Size() int {
	return m.Size()
}
func (m *BaseExecutionInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_BaseExecutionInfo.DiscardUnknown(m)
}

var xxx_messageInfo_BaseExecutionInfo proto.InternalMessageInfo

func (m *BaseExecutionInfo) GetRunId() string {
	if m != nil {
		return m.RunId
	}
	return ""
}

func (m *BaseExecutionInfo) GetLowestCommonAncestorEventId() int64 {
	if m != nil {
		return m.LowestCommonAncestorEventId
	}
	return 0
}

func (m *BaseExecutionInfo) GetLowestCommonAncestorEventVersion() int64 {
	if m != nil {
		return m.LowestCommonAncestorEventVersion
	}
	return 0
}

func init() {
	proto.RegisterType((*ParentExecutionInfo)(nil), "temporal.server.api.workflow.v1.ParentExecutionInfo")
	proto.RegisterType((*BaseExecutionInfo)(nil), "temporal.server.api.workflow.v1.BaseExecutionInfo")
}

func init() {
	proto.RegisterFile("temporal/server/api/workflow/v1/message.proto", fileDescriptor_c4f1ca48d03c9ded)
}

var fileDescriptor_c4f1ca48d03c9ded = []byte{
	// 439 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x92, 0xbf, 0x6e, 0x13, 0x31,
	0x1c, 0xc7, 0xcf, 0x09, 0x89, 0x14, 0x87, 0x81, 0x1a, 0x21, 0x45, 0x05, 0xb9, 0xa1, 0xea, 0x10,
	0x40, 0xf8, 0x14, 0x18, 0x19, 0x10, 0x2d, 0x15, 0xba, 0x05, 0xa1, 0x0c, 0x45, 0x62, 0x89, 0xcc,
	0xdd, 0xaf, 0x95, 0xd5, 0x3b, 0xfb, 0x64, 0x3b, 0x17, 0x46, 0x1e, 0x81, 0xc7, 0x60, 0xe4, 0x11,
	0x18, 0x19, 0x33, 0x76, 0x24, 0x97, 0x85, 0xb1, 0x8f, 0x80, 0xec, 0xcb, 0xdd, 0x01, 0xa5, 0xdd,
	0xee, 0x7e, 0xfe, 0xf8, 0xfb, 0xe7, 0x27, 0xe3, 0xa7, 0x16, 0xb2, 0x5c, 0x69, 0x9e, 0x86, 0x06,
	0x74, 0x01, 0x3a, 0xe4, 0xb9, 0x08, 0x97, 0x4a, 0x9f, 0x9f, 0xa6, 0x6a, 0x19, 0x16, 0xd3, 0x30,
	0x03, 0x63, 0xf8, 0x19, 0xb0, 0x5c, 0x2b, 0xab, 0xc8, 0x5e, 0x8d, 0xb3, 0x0a, 0x67, 0x3c, 0x17,
	0xac, 0xc6, 0x59, 0x31, 0xdd, 0x3d, 0x68, 0xf4, 0x9c, 0x50, 0xac, 0xb2, 0x4c, 0xc9, 0x2b, 0x32,
	0xbb, 0x8f, 0xff, 0xe7, 0x1a, 0xa7, 0x2a, 0x3e, 0xbf, 0xc2, 0xee, 0x7f, 0xeb, 0xe0, 0xbb, 0xef,
	0xb8, 0x06, 0x69, 0x8f, 0x3f, 0x41, 0xbc, 0xb0, 0x42, 0xc9, 0x48, 0x9e, 0x2a, 0xf2, 0x10, 0xdf,
	0x96, 0x3c, 0x03, 0x93, 0xf3, 0x18, 0xe6, 0x22, 0x19, 0xa1, 0x31, 0x9a, 0x0c, 0x66, 0xc3, 0x66,
	0x16, 0x25, 0xe4, 0x01, 0x1e, 0x34, 0xbf, 0xa3, 0x8e, 0x3f, 0x6f, 0x07, 0xe4, 0x0d, 0x1e, 0x40,
	0xad, 0x38, 0xea, 0x8e, 0xd1, 0x64, 0xf8, 0xec, 0x11, 0x6b, 0xfa, 0xb9, 0x62, 0x55, 0x7c, 0x56,
	0x4c, 0xd9, 0xfb, 0x6d, 0xc5, 0x26, 0xc2, 0xac, 0xbd, 0xeb, 0x92, 0x08, 0x29, 0xac, 0xe0, 0x16,
	0x12, 0x97, 0xe4, 0xd6, 0x18, 0x4d, 0xba, 0xb3, 0x61, 0x33, 0x8b, 0x12, 0xf2, 0x12, 0xf7, 0x7c,
	0xbd, 0x51, 0xef, 0x5f, 0x9f, 0x3f, 0xf6, 0xe8, 0x09, 0xe7, 0x76, 0x02, 0xb1, 0x55, 0xfa, 0xc8,
	0xfd, 0xce, 0xaa, 0x7b, 0xe4, 0x09, 0xde, 0x69, 0x3d, 0x0a, 0xd0, 0xc6, 0x85, 0xee, 0x7b, 0xa3,
	0x3b, 0xcd, 0xc1, 0x49, 0x35, 0xdf, 0xff, 0x8e, 0xf0, 0xce, 0x21, 0x37, 0xf0, 0xf7, 0xc2, 0xee,
	0xe1, 0xbe, 0x5e, 0xc8, 0x76, 0x55, 0x3d, 0xbd, 0x90, 0x51, 0x42, 0x5e, 0xe3, 0xbd, 0x54, 0x2d,
	0xc1, 0xd8, 0x79, 0x55, 0x77, 0xce, 0x65, 0x0c, 0xc6, 0x2a, 0x3d, 0x87, 0x02, 0xa4, 0x75, 0x7c,
	0xc7, 0xfb, 0xdc, 0xaf, 0xb0, 0x23, 0x4f, 0xbd, 0xda, 0x42, 0xc7, 0x8e, 0x89, 0x12, 0xf2, 0x16,
	0x1f, 0xdc, 0xa8, 0x52, 0x47, 0xee, 0x7a, 0xa9, 0xf1, 0xb5, 0x52, 0xdb, 0x0a, 0x87, 0xc9, 0x6a,
	0x4d, 0x83, 0x8b, 0x35, 0x0d, 0x2e, 0xd7, 0x14, 0x7d, 0x2e, 0x29, 0xfa, 0x5a, 0x52, 0xf4, 0xa3,
	0xa4, 0x68, 0x55, 0x52, 0xf4, 0xb3, 0xa4, 0xe8, 0x57, 0x49, 0x83, 0xcb, 0x92, 0xa2, 0x2f, 0x1b,
	0x1a, 0xac, 0x36, 0x34, 0xb8, 0xd8, 0xd0, 0xe0, 0x03, 0x3b, 0x53, 0xed, 0x66, 0x85, 0xba, 0xe6,
	0x4d, 0xbf, 0xa8, 0xbf, 0x3f, 0xf6, 0xfd, 0x13, 0x7b, 0xfe, 0x3b, 0x00, 0x00, 0xff, 0xff, 0xc9,
	0xc9, 0x7e, 0x2f, 0x06, 0x03, 0x00, 0x00,
}

func (this *ParentExecutionInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*ParentExecutionInfo)
	if !ok {
		that2, ok := that.(ParentExecutionInfo)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.NamespaceId != that1.NamespaceId {
		return false
	}
	if this.Namespace != that1.Namespace {
		return false
	}
	if !this.Execution.Equal(that1.Execution) {
		return false
	}
	if this.InitiatedId != that1.InitiatedId {
		return false
	}
	if !this.Clock.Equal(that1.Clock) {
		return false
	}
	if this.InitiatedVersion != that1.InitiatedVersion {
		return false
	}
	return true
}
func (this *BaseExecutionInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*BaseExecutionInfo)
	if !ok {
		that2, ok := that.(BaseExecutionInfo)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.RunId != that1.RunId {
		return false
	}
	if this.LowestCommonAncestorEventId != that1.LowestCommonAncestorEventId {
		return false
	}
	if this.LowestCommonAncestorEventVersion != that1.LowestCommonAncestorEventVersion {
		return false
	}
	return true
}
func (this *ParentExecutionInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 10)
	s = append(s, "&workflow.ParentExecutionInfo{")
	s = append(s, "NamespaceId: "+fmt.Sprintf("%#v", this.NamespaceId)+",\n")
	s = append(s, "Namespace: "+fmt.Sprintf("%#v", this.Namespace)+",\n")
	if this.Execution != nil {
		s = append(s, "Execution: "+fmt.Sprintf("%#v", this.Execution)+",\n")
	}
	s = append(s, "InitiatedId: "+fmt.Sprintf("%#v", this.InitiatedId)+",\n")
	if this.Clock != nil {
		s = append(s, "Clock: "+fmt.Sprintf("%#v", this.Clock)+",\n")
	}
	s = append(s, "InitiatedVersion: "+fmt.Sprintf("%#v", this.InitiatedVersion)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *BaseExecutionInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 7)
	s = append(s, "&workflow.BaseExecutionInfo{")
	s = append(s, "RunId: "+fmt.Sprintf("%#v", this.RunId)+",\n")
	s = append(s, "LowestCommonAncestorEventId: "+fmt.Sprintf("%#v", this.LowestCommonAncestorEventId)+",\n")
	s = append(s, "LowestCommonAncestorEventVersion: "+fmt.Sprintf("%#v", this.LowestCommonAncestorEventVersion)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func valueToGoStringMessage(v interface{}, typ string) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}
func (m *ParentExecutionInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ParentExecutionInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ParentExecutionInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.InitiatedVersion != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.InitiatedVersion))
		i--
		dAtA[i] = 0x30
	}
	if m.Clock != nil {
		{
			size, err := m.Clock.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMessage(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x2a
	}
	if m.InitiatedId != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.InitiatedId))
		i--
		dAtA[i] = 0x20
	}
	if m.Execution != nil {
		{
			size, err := m.Execution.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMessage(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x1a
	}
	if len(m.Namespace) > 0 {
		i -= len(m.Namespace)
		copy(dAtA[i:], m.Namespace)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.Namespace)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.NamespaceId) > 0 {
		i -= len(m.NamespaceId)
		copy(dAtA[i:], m.NamespaceId)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.NamespaceId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *BaseExecutionInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *BaseExecutionInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *BaseExecutionInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.LowestCommonAncestorEventVersion != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.LowestCommonAncestorEventVersion))
		i--
		dAtA[i] = 0x18
	}
	if m.LowestCommonAncestorEventId != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.LowestCommonAncestorEventId))
		i--
		dAtA[i] = 0x10
	}
	if len(m.RunId) > 0 {
		i -= len(m.RunId)
		copy(dAtA[i:], m.RunId)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.RunId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintMessage(dAtA []byte, offset int, v uint64) int {
	offset -= sovMessage(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *ParentExecutionInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.NamespaceId)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	l = len(m.Namespace)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.Execution != nil {
		l = m.Execution.Size()
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.InitiatedId != 0 {
		n += 1 + sovMessage(uint64(m.InitiatedId))
	}
	if m.Clock != nil {
		l = m.Clock.Size()
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.InitiatedVersion != 0 {
		n += 1 + sovMessage(uint64(m.InitiatedVersion))
	}
	return n
}

func (m *BaseExecutionInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.RunId)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.LowestCommonAncestorEventId != 0 {
		n += 1 + sovMessage(uint64(m.LowestCommonAncestorEventId))
	}
	if m.LowestCommonAncestorEventVersion != 0 {
		n += 1 + sovMessage(uint64(m.LowestCommonAncestorEventVersion))
	}
	return n
}

func sovMessage(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozMessage(x uint64) (n int) {
	return sovMessage(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *ParentExecutionInfo) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&ParentExecutionInfo{`,
		`NamespaceId:` + fmt.Sprintf("%v", this.NamespaceId) + `,`,
		`Namespace:` + fmt.Sprintf("%v", this.Namespace) + `,`,
		`Execution:` + strings.Replace(fmt.Sprintf("%v", this.Execution), "WorkflowExecution", "v1.WorkflowExecution", 1) + `,`,
		`InitiatedId:` + fmt.Sprintf("%v", this.InitiatedId) + `,`,
		`Clock:` + strings.Replace(fmt.Sprintf("%v", this.Clock), "VectorClock", "v11.VectorClock", 1) + `,`,
		`InitiatedVersion:` + fmt.Sprintf("%v", this.InitiatedVersion) + `,`,
		`}`,
	}, "")
	return s
}
func (this *BaseExecutionInfo) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&BaseExecutionInfo{`,
		`RunId:` + fmt.Sprintf("%v", this.RunId) + `,`,
		`LowestCommonAncestorEventId:` + fmt.Sprintf("%v", this.LowestCommonAncestorEventId) + `,`,
		`LowestCommonAncestorEventVersion:` + fmt.Sprintf("%v", this.LowestCommonAncestorEventVersion) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringMessage(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *ParentExecutionInfo) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMessage
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ParentExecutionInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ParentExecutionInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field NamespaceId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.NamespaceId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Namespace", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Namespace = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Execution", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Execution == nil {
				m.Execution = &v1.WorkflowExecution{}
			}
			if err := m.Execution.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field InitiatedId", wireType)
			}
			m.InitiatedId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.InitiatedId |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Clock", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Clock == nil {
				m.Clock = &v11.VectorClock{}
			}
			if err := m.Clock.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field InitiatedVersion", wireType)
			}
			m.InitiatedVersion = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.InitiatedVersion |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMessage(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMessage
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMessage
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *BaseExecutionInfo) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMessage
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: BaseExecutionInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: BaseExecutionInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RunId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RunId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field LowestCommonAncestorEventId", wireType)
			}
			m.LowestCommonAncestorEventId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.LowestCommonAncestorEventId |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field LowestCommonAncestorEventVersion", wireType)
			}
			m.LowestCommonAncestorEventVersion = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.LowestCommonAncestorEventVersion |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMessage(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMessage
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMessage
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMessage(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMessage
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMessage
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupMessage
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthMessage
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthMessage        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMessage          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupMessage = fmt.Errorf("proto: unexpected end of group")
)
