// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/history/v1/message.proto

package history

import (
	bytes "bytes"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	v1 "go.temporal.io/temporal-proto/history/v1"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"
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

type TransientDecisionInfo struct {
	ScheduledEvent *v1.HistoryEvent `protobuf:"bytes,1,opt,name=scheduled_event,json=scheduledEvent,proto3" json:"scheduled_event,omitempty"`
	StartedEvent   *v1.HistoryEvent `protobuf:"bytes,2,opt,name=started_event,json=startedEvent,proto3" json:"started_event,omitempty"`
}

func (m *TransientDecisionInfo) Reset()      { *m = TransientDecisionInfo{} }
func (*TransientDecisionInfo) ProtoMessage() {}
func (*TransientDecisionInfo) Descriptor() ([]byte, []int) {
	return fileDescriptor_670cd05c700ece14, []int{0}
}
func (m *TransientDecisionInfo) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TransientDecisionInfo) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TransientDecisionInfo.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TransientDecisionInfo) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TransientDecisionInfo.Merge(m, src)
}
func (m *TransientDecisionInfo) XXX_Size() int {
	return m.Size()
}
func (m *TransientDecisionInfo) XXX_DiscardUnknown() {
	xxx_messageInfo_TransientDecisionInfo.DiscardUnknown(m)
}

var xxx_messageInfo_TransientDecisionInfo proto.InternalMessageInfo

func (m *TransientDecisionInfo) GetScheduledEvent() *v1.HistoryEvent {
	if m != nil {
		return m.ScheduledEvent
	}
	return nil
}

func (m *TransientDecisionInfo) GetStartedEvent() *v1.HistoryEvent {
	if m != nil {
		return m.StartedEvent
	}
	return nil
}

// VersionHistoryItem contains signal eventId and the corresponding version.
type VersionHistoryItem struct {
	EventId int64 `protobuf:"varint,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	Version int64 `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
}

func (m *VersionHistoryItem) Reset()      { *m = VersionHistoryItem{} }
func (*VersionHistoryItem) ProtoMessage() {}
func (*VersionHistoryItem) Descriptor() ([]byte, []int) {
	return fileDescriptor_670cd05c700ece14, []int{1}
}
func (m *VersionHistoryItem) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *VersionHistoryItem) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_VersionHistoryItem.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *VersionHistoryItem) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VersionHistoryItem.Merge(m, src)
}
func (m *VersionHistoryItem) XXX_Size() int {
	return m.Size()
}
func (m *VersionHistoryItem) XXX_DiscardUnknown() {
	xxx_messageInfo_VersionHistoryItem.DiscardUnknown(m)
}

var xxx_messageInfo_VersionHistoryItem proto.InternalMessageInfo

func (m *VersionHistoryItem) GetEventId() int64 {
	if m != nil {
		return m.EventId
	}
	return 0
}

func (m *VersionHistoryItem) GetVersion() int64 {
	if m != nil {
		return m.Version
	}
	return 0
}

// VersionHistory contains the version history of a branch.
type VersionHistory struct {
	BranchToken []byte                `protobuf:"bytes,1,opt,name=branch_token,json=branchToken,proto3" json:"branch_token,omitempty"`
	Items       []*VersionHistoryItem `protobuf:"bytes,2,rep,name=items,proto3" json:"items,omitempty"`
}

func (m *VersionHistory) Reset()      { *m = VersionHistory{} }
func (*VersionHistory) ProtoMessage() {}
func (*VersionHistory) Descriptor() ([]byte, []int) {
	return fileDescriptor_670cd05c700ece14, []int{2}
}
func (m *VersionHistory) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *VersionHistory) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_VersionHistory.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *VersionHistory) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VersionHistory.Merge(m, src)
}
func (m *VersionHistory) XXX_Size() int {
	return m.Size()
}
func (m *VersionHistory) XXX_DiscardUnknown() {
	xxx_messageInfo_VersionHistory.DiscardUnknown(m)
}

var xxx_messageInfo_VersionHistory proto.InternalMessageInfo

func (m *VersionHistory) GetBranchToken() []byte {
	if m != nil {
		return m.BranchToken
	}
	return nil
}

func (m *VersionHistory) GetItems() []*VersionHistoryItem {
	if m != nil {
		return m.Items
	}
	return nil
}

// VersionHistories contains all version histories from all branches.
type VersionHistories struct {
	CurrentVersionHistoryIndex int32             `protobuf:"varint,1,opt,name=current_version_history_index,json=currentVersionHistoryIndex,proto3" json:"current_version_history_index,omitempty"`
	Histories                  []*VersionHistory `protobuf:"bytes,2,rep,name=histories,proto3" json:"histories,omitempty"`
}

func (m *VersionHistories) Reset()      { *m = VersionHistories{} }
func (*VersionHistories) ProtoMessage() {}
func (*VersionHistories) Descriptor() ([]byte, []int) {
	return fileDescriptor_670cd05c700ece14, []int{3}
}
func (m *VersionHistories) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *VersionHistories) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_VersionHistories.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *VersionHistories) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VersionHistories.Merge(m, src)
}
func (m *VersionHistories) XXX_Size() int {
	return m.Size()
}
func (m *VersionHistories) XXX_DiscardUnknown() {
	xxx_messageInfo_VersionHistories.DiscardUnknown(m)
}

var xxx_messageInfo_VersionHistories proto.InternalMessageInfo

func (m *VersionHistories) GetCurrentVersionHistoryIndex() int32 {
	if m != nil {
		return m.CurrentVersionHistoryIndex
	}
	return 0
}

func (m *VersionHistories) GetHistories() []*VersionHistory {
	if m != nil {
		return m.Histories
	}
	return nil
}

func init() {
	proto.RegisterType((*TransientDecisionInfo)(nil), "temporal.server.api.history.v1.TransientDecisionInfo")
	proto.RegisterType((*VersionHistoryItem)(nil), "temporal.server.api.history.v1.VersionHistoryItem")
	proto.RegisterType((*VersionHistory)(nil), "temporal.server.api.history.v1.VersionHistory")
	proto.RegisterType((*VersionHistories)(nil), "temporal.server.api.history.v1.VersionHistories")
}

func init() {
	proto.RegisterFile("temporal/server/api/history/v1/message.proto", fileDescriptor_670cd05c700ece14)
}

var fileDescriptor_670cd05c700ece14 = []byte{
	// 414 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0x3f, 0x8f, 0xd3, 0x30,
	0x18, 0xc6, 0xe3, 0xab, 0x8e, 0x03, 0xb7, 0x1c, 0xc8, 0x12, 0x52, 0x39, 0x09, 0xeb, 0x9a, 0xe9,
	0x86, 0x93, 0xa3, 0x2b, 0x23, 0x13, 0x08, 0xd0, 0x05, 0x31, 0x45, 0x27, 0x06, 0x96, 0x28, 0x4d,
	0x5e, 0x1a, 0x8b, 0xc6, 0x8e, 0x6c, 0x37, 0x82, 0x01, 0x89, 0x8f, 0xc0, 0x77, 0x60, 0x41, 0xe2,
	0x8b, 0x30, 0x76, 0xec, 0x48, 0xd3, 0x85, 0xb1, 0x1f, 0x01, 0xc5, 0x71, 0x53, 0x95, 0x7f, 0xe2,
	0x36, 0x3f, 0xce, 0xf3, 0xfe, 0x9e, 0x27, 0xf2, 0x8b, 0xcf, 0x0d, 0x14, 0xa5, 0x54, 0xc9, 0x2c,
	0xd0, 0xa0, 0x2a, 0x50, 0x41, 0x52, 0xf2, 0x20, 0xe7, 0xda, 0x48, 0xf5, 0x3e, 0xa8, 0x2e, 0x82,
	0x02, 0xb4, 0x4e, 0xa6, 0xc0, 0x4a, 0x25, 0x8d, 0x24, 0x74, 0xeb, 0x66, 0xad, 0x9b, 0x25, 0x25,
	0x67, 0xce, 0xcd, 0xaa, 0x8b, 0x93, 0x51, 0x47, 0xfb, 0x1b, 0xc2, 0xff, 0x8a, 0xf0, 0xbd, 0x2b,
	0x95, 0x08, 0xcd, 0x41, 0x98, 0xa7, 0x90, 0x72, 0xcd, 0xa5, 0x08, 0xc5, 0x1b, 0x49, 0x5e, 0xe0,
	0x3b, 0x3a, 0xcd, 0x21, 0x9b, 0xcf, 0x20, 0x8b, 0xa1, 0x02, 0x61, 0x86, 0xe8, 0x14, 0x9d, 0xf5,
	0xc7, 0x23, 0xd6, 0xc5, 0xee, 0xb2, 0xd8, 0x65, 0x7b, 0x7c, 0xd6, 0x18, 0xa3, 0xe3, 0x6e, 0xd2,
	0x6a, 0xf2, 0x1c, 0xdf, 0xd6, 0x26, 0x51, 0xa6, 0x23, 0x1d, 0xfc, 0x2f, 0x69, 0xe0, 0xe6, 0xac,
	0xf2, 0x43, 0x4c, 0x5e, 0x81, 0x6a, 0x2a, 0x3a, 0x53, 0x68, 0xa0, 0x20, 0xf7, 0xf1, 0x4d, 0x4b,
	0x8d, 0x79, 0x66, 0x2b, 0xf6, 0xa2, 0x23, 0xab, 0xc3, 0x8c, 0x0c, 0xf1, 0x51, 0xd5, 0x0e, 0xd8,
	0xc8, 0x5e, 0xb4, 0x95, 0xfe, 0x07, 0x7c, 0xbc, 0x8f, 0x22, 0x23, 0x3c, 0x98, 0xa8, 0x44, 0xa4,
	0x79, 0x6c, 0xe4, 0x5b, 0x10, 0x16, 0x35, 0x88, 0xfa, 0xed, 0xdd, 0x55, 0x73, 0x45, 0x2e, 0xf1,
	0x21, 0x37, 0x50, 0xe8, 0xe1, 0xc1, 0x69, 0xef, 0xac, 0x3f, 0x1e, 0xb3, 0x7f, 0x3f, 0x00, 0xfb,
	0xbd, 0x6c, 0xd4, 0x02, 0xfc, 0xcf, 0x08, 0xdf, 0xdd, 0xfb, 0xca, 0x41, 0x93, 0xc7, 0xf8, 0x41,
	0x3a, 0x57, 0xaa, 0xf9, 0x15, 0x57, 0x33, 0x76, 0xb0, 0x98, 0x8b, 0x0c, 0xde, 0xd9, 0x4a, 0x87,
	0xd1, 0x89, 0x33, 0xfd, 0x42, 0x6f, 0x1c, 0xe4, 0x25, 0xbe, 0x95, 0x6f, 0x79, 0xae, 0x25, 0xbb,
	0x5e, 0xcb, 0x68, 0x07, 0x78, 0x32, 0x59, 0xac, 0xa8, 0xb7, 0x5c, 0x51, 0x6f, 0xb3, 0xa2, 0xe8,
	0x63, 0x4d, 0xd1, 0x97, 0x9a, 0xa2, 0x6f, 0x35, 0x45, 0x8b, 0x9a, 0xa2, 0xef, 0x35, 0x45, 0x3f,
	0x6a, 0xea, 0x6d, 0x6a, 0x8a, 0x3e, 0xad, 0xa9, 0xb7, 0x58, 0x53, 0x6f, 0xb9, 0xa6, 0xde, 0xeb,
	0xf3, 0xa9, 0xdc, 0x45, 0x72, 0xf9, 0xe7, 0x55, 0x7e, 0xe4, 0x8e, 0x93, 0x1b, 0x76, 0x11, 0x1f,
	0xfe, 0x0c, 0x00, 0x00, 0xff, 0xff, 0xea, 0x6c, 0x51, 0xc1, 0xfb, 0x02, 0x00, 0x00,
}

func (this *TransientDecisionInfo) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*TransientDecisionInfo)
	if !ok {
		that2, ok := that.(TransientDecisionInfo)
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
	if !this.ScheduledEvent.Equal(that1.ScheduledEvent) {
		return false
	}
	if !this.StartedEvent.Equal(that1.StartedEvent) {
		return false
	}
	return true
}
func (this *VersionHistoryItem) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*VersionHistoryItem)
	if !ok {
		that2, ok := that.(VersionHistoryItem)
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
	if this.EventId != that1.EventId {
		return false
	}
	if this.Version != that1.Version {
		return false
	}
	return true
}
func (this *VersionHistory) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*VersionHistory)
	if !ok {
		that2, ok := that.(VersionHistory)
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
	if !bytes.Equal(this.BranchToken, that1.BranchToken) {
		return false
	}
	if len(this.Items) != len(that1.Items) {
		return false
	}
	for i := range this.Items {
		if !this.Items[i].Equal(that1.Items[i]) {
			return false
		}
	}
	return true
}
func (this *VersionHistories) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*VersionHistories)
	if !ok {
		that2, ok := that.(VersionHistories)
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
	if this.CurrentVersionHistoryIndex != that1.CurrentVersionHistoryIndex {
		return false
	}
	if len(this.Histories) != len(that1.Histories) {
		return false
	}
	for i := range this.Histories {
		if !this.Histories[i].Equal(that1.Histories[i]) {
			return false
		}
	}
	return true
}
func (this *TransientDecisionInfo) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 6)
	s = append(s, "&history.TransientDecisionInfo{")
	if this.ScheduledEvent != nil {
		s = append(s, "ScheduledEvent: "+fmt.Sprintf("%#v", this.ScheduledEvent)+",\n")
	}
	if this.StartedEvent != nil {
		s = append(s, "StartedEvent: "+fmt.Sprintf("%#v", this.StartedEvent)+",\n")
	}
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *VersionHistoryItem) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 6)
	s = append(s, "&history.VersionHistoryItem{")
	s = append(s, "EventId: "+fmt.Sprintf("%#v", this.EventId)+",\n")
	s = append(s, "Version: "+fmt.Sprintf("%#v", this.Version)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *VersionHistory) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 6)
	s = append(s, "&history.VersionHistory{")
	s = append(s, "BranchToken: "+fmt.Sprintf("%#v", this.BranchToken)+",\n")
	if this.Items != nil {
		s = append(s, "Items: "+fmt.Sprintf("%#v", this.Items)+",\n")
	}
	s = append(s, "}")
	return strings.Join(s, "")
}
func (this *VersionHistories) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 6)
	s = append(s, "&history.VersionHistories{")
	s = append(s, "CurrentVersionHistoryIndex: "+fmt.Sprintf("%#v", this.CurrentVersionHistoryIndex)+",\n")
	if this.Histories != nil {
		s = append(s, "Histories: "+fmt.Sprintf("%#v", this.Histories)+",\n")
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
func (m *TransientDecisionInfo) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TransientDecisionInfo) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TransientDecisionInfo) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.StartedEvent != nil {
		{
			size, err := m.StartedEvent.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintMessage(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if m.ScheduledEvent != nil {
		{
			size, err := m.ScheduledEvent.MarshalToSizedBuffer(dAtA[:i])
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

func (m *VersionHistoryItem) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *VersionHistoryItem) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *VersionHistoryItem) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Version != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.Version))
		i--
		dAtA[i] = 0x10
	}
	if m.EventId != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.EventId))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *VersionHistory) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *VersionHistory) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *VersionHistory) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Items) > 0 {
		for iNdEx := len(m.Items) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Items[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMessage(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.BranchToken) > 0 {
		i -= len(m.BranchToken)
		copy(dAtA[i:], m.BranchToken)
		i = encodeVarintMessage(dAtA, i, uint64(len(m.BranchToken)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *VersionHistories) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *VersionHistories) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *VersionHistories) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Histories) > 0 {
		for iNdEx := len(m.Histories) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Histories[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintMessage(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if m.CurrentVersionHistoryIndex != 0 {
		i = encodeVarintMessage(dAtA, i, uint64(m.CurrentVersionHistoryIndex))
		i--
		dAtA[i] = 0x8
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
func (m *TransientDecisionInfo) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.ScheduledEvent != nil {
		l = m.ScheduledEvent.Size()
		n += 1 + l + sovMessage(uint64(l))
	}
	if m.StartedEvent != nil {
		l = m.StartedEvent.Size()
		n += 1 + l + sovMessage(uint64(l))
	}
	return n
}

func (m *VersionHistoryItem) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.EventId != 0 {
		n += 1 + sovMessage(uint64(m.EventId))
	}
	if m.Version != 0 {
		n += 1 + sovMessage(uint64(m.Version))
	}
	return n
}

func (m *VersionHistory) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.BranchToken)
	if l > 0 {
		n += 1 + l + sovMessage(uint64(l))
	}
	if len(m.Items) > 0 {
		for _, e := range m.Items {
			l = e.Size()
			n += 1 + l + sovMessage(uint64(l))
		}
	}
	return n
}

func (m *VersionHistories) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.CurrentVersionHistoryIndex != 0 {
		n += 1 + sovMessage(uint64(m.CurrentVersionHistoryIndex))
	}
	if len(m.Histories) > 0 {
		for _, e := range m.Histories {
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
func (this *TransientDecisionInfo) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&TransientDecisionInfo{`,
		`ScheduledEvent:` + strings.Replace(fmt.Sprintf("%v", this.ScheduledEvent), "HistoryEvent", "v1.HistoryEvent", 1) + `,`,
		`StartedEvent:` + strings.Replace(fmt.Sprintf("%v", this.StartedEvent), "HistoryEvent", "v1.HistoryEvent", 1) + `,`,
		`}`,
	}, "")
	return s
}
func (this *VersionHistoryItem) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&VersionHistoryItem{`,
		`EventId:` + fmt.Sprintf("%v", this.EventId) + `,`,
		`Version:` + fmt.Sprintf("%v", this.Version) + `,`,
		`}`,
	}, "")
	return s
}
func (this *VersionHistory) String() string {
	if this == nil {
		return "nil"
	}
	repeatedStringForItems := "[]*VersionHistoryItem{"
	for _, f := range this.Items {
		repeatedStringForItems += strings.Replace(f.String(), "VersionHistoryItem", "VersionHistoryItem", 1) + ","
	}
	repeatedStringForItems += "}"
	s := strings.Join([]string{`&VersionHistory{`,
		`BranchToken:` + fmt.Sprintf("%v", this.BranchToken) + `,`,
		`Items:` + repeatedStringForItems + `,`,
		`}`,
	}, "")
	return s
}
func (this *VersionHistories) String() string {
	if this == nil {
		return "nil"
	}
	repeatedStringForHistories := "[]*VersionHistory{"
	for _, f := range this.Histories {
		repeatedStringForHistories += strings.Replace(f.String(), "VersionHistory", "VersionHistory", 1) + ","
	}
	repeatedStringForHistories += "}"
	s := strings.Join([]string{`&VersionHistories{`,
		`CurrentVersionHistoryIndex:` + fmt.Sprintf("%v", this.CurrentVersionHistoryIndex) + `,`,
		`Histories:` + repeatedStringForHistories + `,`,
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
func (m *TransientDecisionInfo) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: TransientDecisionInfo: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TransientDecisionInfo: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ScheduledEvent", wireType)
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
			if m.ScheduledEvent == nil {
				m.ScheduledEvent = &v1.HistoryEvent{}
			}
			if err := m.ScheduledEvent.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartedEvent", wireType)
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
			if m.StartedEvent == nil {
				m.StartedEvent = &v1.HistoryEvent{}
			}
			if err := m.StartedEvent.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
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
func (m *VersionHistoryItem) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: VersionHistoryItem: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: VersionHistoryItem: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field EventId", wireType)
			}
			m.EventId = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.EventId |= int64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= int64(b&0x7F) << shift
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
func (m *VersionHistory) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: VersionHistory: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: VersionHistory: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BranchToken", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthMessage
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthMessage
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BranchToken = append(m.BranchToken[:0], dAtA[iNdEx:postIndex]...)
			if m.BranchToken == nil {
				m.BranchToken = []byte{}
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Items", wireType)
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
			m.Items = append(m.Items, &VersionHistoryItem{})
			if err := m.Items[len(m.Items)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
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
func (m *VersionHistories) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: VersionHistories: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: VersionHistories: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentVersionHistoryIndex", wireType)
			}
			m.CurrentVersionHistoryIndex = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMessage
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.CurrentVersionHistoryIndex |= int32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Histories", wireType)
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
			m.Histories = append(m.Histories, &VersionHistory{})
			if err := m.Histories[len(m.Histories)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
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
