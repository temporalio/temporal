// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/cluster/v1/message.proto

package cluster

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"

	proto "github.com/gogo/protobuf/proto"
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

type HostInfo struct {
	Identity string `protobuf:"bytes,1,opt,name=identity,proto3" json:"identity,omitempty"`
}

func (m *HostInfo) Reset()      { *m = HostInfo{} }
func (*HostInfo) ProtoMessage() {}
func (*HostInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_fcc65697c8eece3a, []int{0}
}
func (m *HostInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *HostInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_HostInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *HostInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HostInfo.Merge(m, src)
}
func (m *HostInfo) XXX_Size() int {
	return m.Size()
}
func (m *HostInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_HostInfo.DiscardUnknown(m)
}

var xxx_messageInfo_HostInfo proto.InternalMessageInfo

func (m *HostInfo) GetIdentity() string {
	if m != nil {
		return m.Identity
	}
	return ""
}

type RingInfo struct {
	Role        string      `protobuf:"bytes,1,opt,name=role,proto3" json:"role,omitempty"`
	MemberCount int32       `protobuf:"varint,2,opt,name=member_count,json=memberCount,proto3" json:"member_count,omitempty"`
	Members     []*HostInfo `protobuf:"bytes,3,rep,name=members,proto3" json:"members,omitempty"`
}

func (m *RingInfo) Reset()      { *m = RingInfo{} }
func (*RingInfo) ProtoMessage() {}
func (*RingInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_fcc65697c8eece3a, []int{1}
}
func (m *RingInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *RingInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_RingInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *RingInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_RingInfo.Merge(m, src)
}
func (m *RingInfo) XXX_Size() int {
	return m.Size()
}
func (m *RingInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_RingInfo.DiscardUnknown(m)
}

var xxx_messageInfo_RingInfo proto.InternalMessageInfo

func (m *RingInfo) GetRole() string {
	if m != nil {
		return m.Role
	}
	return ""
}

func (m *RingInfo) GetMemberCount() int32 {
	if m != nil {
		return m.MemberCount
	}
	return 0
}

func (m *RingInfo) GetMembers() []*HostInfo {
	if m != nil {
		return m.Members
	}
	return nil
}

type MembershipInfo struct {
	CurrentHost      *HostInfo   `protobuf:"bytes,1,opt,name=current_host,json=currentHost,proto3" json:"current_host,omitempty"`
	ReachableMembers []string    `protobuf:"bytes,2,rep,name=reachable_members,json=reachableMembers,proto3" json:"reachable_members,omitempty"`
	Rings            []*RingInfo `protobuf:"bytes,3,rep,name=rings,proto3" json:"rings,omitempty"`
}

func (m *MembershipInfo) Reset()      { *m = MembershipInfo{} }
func (*MembershipInfo) ProtoMessage() {}
func (*MembershipInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_fcc65697c8eece3a, []int{2}
}
func (m *MembershipInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *MembershipInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_MembershipInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *MembershipInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_MembershipInfo.Merge(m, src)
}
func (m *MembershipInfo) XXX_Size() int {
	return m.Size()
}
func (m *MembershipInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_MembershipInfo.DiscardUnknown(m)
}

var xxx_messageInfo_MembershipInfo proto.InternalMessageInfo

func (m *MembershipInfo) GetCurrentHost() *HostInfo {
	if m != nil {
		return m.CurrentHost
	}
	return nil
}

func (m *MembershipInfo) GetReachableMembers() []string {
	if m != nil {
		return m.ReachableMembers
	}
	return nil
}

func (m *MembershipInfo) GetRings() []*RingInfo {
	if m != nil {
		return m.Rings
	}
	return nil
}

func init() {
	proto.RegisterType((*HostInfo)(nil), "temporal.server.api.cluster.v1.HostInfo")
	proto.RegisterType((*RingInfo)(nil), "temporal.server.api.cluster.v1.RingInfo")
	proto.RegisterType((*MembershipInfo)(nil), "temporal.server.api.cluster.v1.MembershipInfo")
}

func init() {
	proto.RegisterFile("temporal/server/api/cluster/v1/message.proto", fileDescriptor_fcc65697c8eece3a)
}

var fileDescriptor_fcc65697c8eece3a = []byte{
	// 345 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0xbf, 0x4e, 0xf3, 0x30,
	0x14, 0xc5, 0xe3, 0xf6, 0xeb, 0x47, 0xeb, 0x56, 0x08, 0x3c, 0x55, 0x0c, 0x57, 0xa1, 0x03, 0x8a,
	0x44, 0xe5, 0xa8, 0x30, 0x22, 0x31, 0x94, 0x05, 0x84, 0x58, 0x32, 0xb2, 0x54, 0x49, 0x30, 0xa9,
	0xa5, 0x24, 0x8e, 0x6c, 0xb7, 0x12, 0x1b, 0x0b, 0x3b, 0x8f, 0xc1, 0xa3, 0x20, 0xa6, 0x8e, 0x1d,
	0xa9, 0xb3, 0x30, 0xf6, 0x11, 0x50, 0xf3, 0xa7, 0x2c, 0x08, 0xc1, 0x76, 0xee, 0xc9, 0x39, 0xf1,
	0xcf, 0xd6, 0xc5, 0x43, 0xcd, 0x92, 0x4c, 0x48, 0x3f, 0x76, 0x15, 0x93, 0x73, 0x26, 0x5d, 0x3f,
	0xe3, 0x6e, 0x18, 0xcf, 0x94, 0x66, 0xd2, 0x9d, 0x8f, 0xdc, 0x84, 0x29, 0xe5, 0x47, 0x8c, 0x66,
	0x52, 0x68, 0x41, 0xa0, 0x4e, 0xd3, 0x32, 0x4d, 0xfd, 0x8c, 0xd3, 0x2a, 0x4d, 0xe7, 0xa3, 0xc1,
	0x11, 0x6e, 0x5f, 0x0a, 0xa5, 0xaf, 0xd2, 0x7b, 0x41, 0x0e, 0x70, 0x9b, 0xdf, 0xb1, 0x54, 0x73,
	0xfd, 0xd0, 0x47, 0x36, 0x72, 0x3a, 0xde, 0x76, 0x1e, 0x3c, 0x21, 0xdc, 0xf6, 0x78, 0x1a, 0x15,
	0x41, 0x82, 0xff, 0x49, 0x11, 0xb3, 0x2a, 0x54, 0x68, 0x72, 0x88, 0x7b, 0x09, 0x4b, 0x02, 0x26,
	0x27, 0xa1, 0x98, 0xa5, 0xba, 0xdf, 0xb0, 0x91, 0xd3, 0xf2, 0xba, 0xa5, 0x77, 0xb1, 0xb1, 0xc8,
	0x18, 0xef, 0x94, 0xa3, 0xea, 0x37, 0xed, 0xa6, 0xd3, 0x3d, 0x71, 0xe8, 0xcf, 0x74, 0xb4, 0x46,
	0xf3, 0xea, 0xe2, 0xe0, 0x0d, 0xe1, 0xdd, 0x9b, 0x52, 0x4f, 0x79, 0x56, 0xd0, 0x5c, 0xe3, 0x5e,
	0x38, 0x93, 0x92, 0xa5, 0x7a, 0x32, 0x15, 0x4a, 0x17, 0x54, 0x7f, 0xf9, 0x77, 0xb7, 0x6a, 0x6f,
	0x0c, 0x72, 0x8c, 0xf7, 0x25, 0xf3, 0xc3, 0xa9, 0x1f, 0xc4, 0x6c, 0x52, 0xd3, 0x36, 0xec, 0xa6,
	0xd3, 0xf1, 0xf6, 0xb6, 0x1f, 0x2a, 0x00, 0x72, 0x8e, 0x5b, 0x92, 0xa7, 0xd1, 0xaf, 0xaf, 0x53,
	0x3f, 0xa0, 0x57, 0xd6, 0xc6, 0xc1, 0x62, 0x05, 0xd6, 0x72, 0x05, 0xd6, 0x7a, 0x05, 0xe8, 0xd1,
	0x00, 0x7a, 0x31, 0x80, 0x5e, 0x0d, 0xa0, 0x85, 0x01, 0xf4, 0x6e, 0x00, 0x7d, 0x18, 0xb0, 0xd6,
	0x06, 0xd0, 0x73, 0x0e, 0xd6, 0x22, 0x07, 0x6b, 0x99, 0x83, 0x75, 0x3b, 0x8c, 0xc4, 0xd7, 0x41,
	0x5c, 0x7c, 0xbf, 0x06, 0x67, 0x95, 0x0c, 0xfe, 0x17, 0x7b, 0x70, 0xfa, 0x19, 0x00, 0x00, 0xff,
	0xff, 0x4e, 0xa0, 0x30, 0xe6, 0x37, 0x02, 0x00, 0x00,
}

func (this *HostInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*HostInfo)
	if !ok {
		that2, ok := that.(HostInfo)
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
	if this.Identity != that1.Identity {
		return false
	}
	return true
}
func (this *RingInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*RingInfo)
	if !ok {
		that2, ok := that.(RingInfo)
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
	if this.Role != that1.Role {
		return false
	}
	if this.MemberCount != that1.MemberCount {
		return false
	}
	if len(this.Members) != len(that1.Members) {
		return false
	}
	for i := range this.Members {
		if !this.Members[i].Equal(that1.Members[i]) {
			return false
		}
	}
	return true
}
func (this *MembershipInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*MembershipInfo)
	if !ok {
		that2, ok := that.(MembershipInfo)
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
	if !this.CurrentHost.Equal(that1.CurrentHost) {
		return false
	}
	if len(this.ReachableMembers) != len(that1.ReachableMembers) {
		return false
	}
	for i := range this.ReachableMembers {
		if this.ReachableMembers[i] != that1.ReachableMembers[i] {
			return false
		}
	}
	if len(this.Rings) != len(that1.Rings) {
		return false
	}
	for i := range this.Rings {
		if !this.Rings[i].Equal(that1.Rings[i]) {
			return false
		}
	}
	return true
}
func (this *HostInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 5)
	s = append(s, "&cluster.HostInfo{")
	s = append(s, "Identity: "+fmt.Sprintf("%#v", this.Identity)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *RingInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 7)
	s = append(s, "&cluster.RingInfo{")
	s = append(s, "Role: "+fmt.Sprintf("%#v", this.Role)+",\n")
	s = append(s, "MemberCount: "+fmt.Sprintf("%#v", this.MemberCount)+",\n")
	if this.Members != nil {
		s = append(s, "Members: "+fmt.Sprintf("%#v", this.Members)+",\n")
	}
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *MembershipInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 7)
	s = append(s, "&cluster.MembershipInfo{")
	if this.CurrentHost != nil {
		s = append(s, "CurrentHost: "+fmt.Sprintf("%#v", this.CurrentHost)+",\n")
	}
	s = append(s, "ReachableMembers: "+fmt.Sprintf("%#v", this.ReachableMembers)+",\n")
	if this.Rings != nil {
		s = append(s, "Rings: "+fmt.Sprintf("%#v", this.Rings)+",\n")
	}
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
func (m *HostInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *HostInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *HostInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Identity) > 0 {
		i -= len(m.Identity)
		copy(dAtA[i:], m.Identity)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.Identity)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *RingInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *RingInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *RingInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Members) > 0 {
		for iNdEx := len(m.Members) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Members[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMessage(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if m.MemberCount != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.MemberCount))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Role) > 0 {
		i -= len(m.Role)
		copy(dAtA[i:], m.Role)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.Role)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *MembershipInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *MembershipInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *MembershipInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Rings) > 0 {
		for iNdEx := len(m.Rings) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Rings[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMessage(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.ReachableMembers) > 0 {
		for iNdEx := len(m.ReachableMembers) - 1; iNdEx >= 0; iNdEx-- {
			i -= len(m.ReachableMembers[iNdEx])
			copy(dAtA[i:], m.ReachableMembers[iNdEx])
			i = encodeVarintMessage(dAtA, i, uint64(len(m.ReachableMembers[iNdEx])))
			i--
			dAtA[i] = 0x12
		}
	}
	if m.CurrentHost != nil {
		{
			size, err := m.CurrentHost.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMessage(dAtA, i, uint64(size))
		}
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
func (m *HostInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Identity)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	return n
}

func (m *RingInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Role)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.MemberCount != 0 {
		n += 1 + sovMessage(uint64(m.MemberCount))
	}
	if len(m.Members) > 0 {
		for _, e := range m.Members {
			l = e.Size()
			n += 1 + l + sovMessage(uint64(l))
		}
	}
	return n
}

func (m *MembershipInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.CurrentHost != nil {
		l = m.CurrentHost.Size()
		n += 1 + l + sovMessage(uint64(l))
	}
	if len(m.ReachableMembers) > 0 {
		for _, s := range m.ReachableMembers {
			l = len(s)
			n += 1 + l + sovMessage(uint64(l))
		}
	}
	if len(m.Rings) > 0 {
		for _, e := range m.Rings {
			l = e.Size()
			n += 1 + l + sovMessage(uint64(l))
		}
	}
	return n
}

func sovMessage(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozMessage(x uint64) (n int) {
	return sovMessage(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *HostInfo) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&HostInfo{`,
		`Identity:` + fmt.Sprintf("%v", this.Identity) + `,`,
		`}`,
	}, "")
	return s
}
func (this *RingInfo) String() string {
	if this == nil {
		return "nil"
	}
	repeatedStringForMembers := "[]*HostInfo{"
	for _, f := range this.Members {
		repeatedStringForMembers += strings.Replace(f.String(), "HostInfo", "HostInfo", 1) + ","
	}
	repeatedStringForMembers += "}"
	s := strings.Join([]string{`&RingInfo{`,
		`Role:` + fmt.Sprintf("%v", this.Role) + `,`,
		`MemberCount:` + fmt.Sprintf("%v", this.MemberCount) + `,`,
		`Members:` + repeatedStringForMembers + `,`,
		`}`,
	}, "")
	return s
}
func (this *MembershipInfo) String() string {
	if this == nil {
		return "nil"
	}
	repeatedStringForRings := "[]*RingInfo{"
	for _, f := range this.Rings {
		repeatedStringForRings += strings.Replace(f.String(), "RingInfo", "RingInfo", 1) + ","
	}
	repeatedStringForRings += "}"
	s := strings.Join([]string{`&MembershipInfo{`,
		`CurrentHost:` + strings.Replace(this.CurrentHost.String(), "HostInfo", "HostInfo", 1) + `,`,
		`ReachableMembers:` + fmt.Sprintf("%v", this.ReachableMembers) + `,`,
		`Rings:` + repeatedStringForRings + `,`,
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
func (m *HostInfo) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: HostInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: HostInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Identity", wireType)
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
			m.Identity = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
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
func (m *RingInfo) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: RingInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RingInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Role", wireType)
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
			m.Role = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MemberCount", wireType)
			}
			m.MemberCount = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MemberCount |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Members", wireType)
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
			m.Members = append(m.Members, &HostInfo{})
			if err := m.Members[len(m.Members)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
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
func (m *MembershipInfo) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: MembershipInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: MembershipInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentHost", wireType)
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
			if m.CurrentHost == nil {
				m.CurrentHost = &HostInfo{}
			}
			if err := m.CurrentHost.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ReachableMembers", wireType)
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
			m.ReachableMembers = append(m.ReachableMembers, string(dAtA[iNdEx:postIndex]))
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Rings", wireType)
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
			m.Rings = append(m.Rings, &RingInfo{})
			if err := m.Rings[len(m.Rings)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
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
