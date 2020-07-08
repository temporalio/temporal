// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/adminservice/v1/service.proto

package adminservice

import (
	context "context"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
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

func init() {
	proto.RegisterFile("temporal/server/api/adminservice/v1/service.proto", fileDescriptor_cf5ca5e0c737570d)
}

var fileDescriptor_cf5ca5e0c737570d = []byte{
	// 659 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x96, 0x3f, 0x6f, 0xd3, 0x4e,
	0x18, 0xc7, 0x7d, 0xcb, 0x6f, 0x38, 0xfd, 0x10, 0xe8, 0xf8, 0x23, 0xd1, 0xe1, 0x40, 0xb0, 0x3b,
	0x6a, 0x91, 0x40, 0xb4, 0x40, 0x9b, 0xb6, 0x21, 0x15, 0xa4, 0x88, 0xba, 0xa8, 0x48, 0x2c, 0xe8,
	0x62, 0x3f, 0x6d, 0xac, 0x3a, 0x39, 0x73, 0x77, 0x4e, 0xe9, 0x04, 0x23, 0x13, 0x82, 0x09, 0x09,
	0x89, 0x89, 0x05, 0x24, 0xde, 0x01, 0x12, 0x12, 0x1b, 0x63, 0xc7, 0x8e, 0xd4, 0x5d, 0x18, 0xfb,
	0x12, 0x50, 0x48, 0xee, 0xea, 0x36, 0x6e, 0x74, 0x76, 0xb3, 0xd9, 0xd2, 0xf3, 0xf9, 0x3e, 0x9f,
	0xf3, 0x25, 0xcf, 0x1d, 0x9e, 0x54, 0xd0, 0x8e, 0xb9, 0x60, 0x51, 0x45, 0x82, 0xe8, 0x82, 0xa8,
	0xb0, 0x38, 0xac, 0xb0, 0xa0, 0x1d, 0x76, 0x7a, 0xef, 0xa1, 0x0f, 0x95, 0xee, 0x64, 0x65, 0xf0,
	0xe8, 0xc6, 0x82, 0x2b, 0x4e, 0xae, 0x6b, 0xc4, 0xed, 0x23, 0x2e, 0x8b, 0x43, 0x37, 0x8b, 0xb8,
	0xdd, 0xc9, 0x89, 0x69, 0x9b, 0x5c, 0x01, 0x2f, 0x12, 0x90, 0xea, 0xb9, 0x00, 0x19, 0xf3, 0x8e,
	0x1c, 0x34, 0x98, 0xfa, 0x7a, 0x11, 0xff, 0x5f, 0xed, 0x95, 0xae, 0xf6, 0x4b, 0xc9, 0x37, 0x84,
	0x2f, 0x2f, 0x82, 0xf4, 0x45, 0xd8, 0x84, 0xa7, 0x5c, 0x6c, 0xae, 0x47, 0x7c, 0xab, 0xf6, 0x12,
	0xfc, 0x44, 0x85, 0xbc, 0x43, 0x6a, 0xae, 0x85, 0x90, 0x7b, 0x22, 0xef, 0xf5, 0x25, 0x26, 0xee,
	0x9f, 0x36, 0xa6, 0xbf, 0x86, 0x6b, 0x0e, 0xf9, 0x88, 0xf0, 0x79, 0x5d, 0xb7, 0x14, 0x4a, 0xc5,
	0xc5, 0xf6, 0x12, 0x97, 0x8a, 0xcc, 0x16, 0xea, 0x90, 0x21, 0xb5, 0xe2, 0x5c, 0xf9, 0x00, 0x23,
	0xf7, 0x0a, 0xe3, 0x85, 0x88, 0x4b, 0x58, 0x6d, 0x31, 0x11, 0x90, 0x9b, 0x56, 0x89, 0x87, 0x80,
	0x36, 0xb9, 0x55, 0x98, 0xcb, 0x0a, 0x78, 0xd0, 0xe6, 0x5d, 0x78, 0xc2, 0xe4, 0xa6, 0xa5, 0xc0,
	0x21, 0x50, 0x4c, 0x20, 0xcb, 0x19, 0x81, 0xef, 0x08, 0xd3, 0x3a, 0xa8, 0xe1, 0x1d, 0x64, 0x5b,
	0x83, 0x4f, 0x46, 0x1e, 0x58, 0xa5, 0x8f, 0x0e, 0xd1, 0xa6, 0x0f, 0xc7, 0x92, 0x65, 0xec, 0x7f,
	0x22, 0x7c, 0x75, 0x74, 0xf1, 0xda, 0x14, 0x69, 0x8c, 0xa1, 0xe7, 0xda, 0x94, 0x5e, 0xc1, 0xf2,
	0x98, 0xd2, 0xcc, 0x1a, 0x3e, 0x23, 0x7c, 0xa9, 0x0e, 0xca, 0x83, 0x38, 0x0a, 0x7d, 0xd6, 0x2b,
	0x5c, 0x06, 0x29, 0xd9, 0x06, 0x48, 0x32, 0x6f, 0xdb, 0x2b, 0x07, 0xd6, 0xbe, 0x0b, 0xa7, 0xca,
	0x30, 0x96, 0x3f, 0x10, 0xbe, 0x52, 0x07, 0xf5, 0x88, 0xb5, 0x41, 0xc6, 0xcc, 0x87, 0x3c, 0x5d,
	0xeb, 0xcd, 0x1d, 0x95, 0xa2, 0xbd, 0x1b, 0xe3, 0x09, 0x33, 0x0b, 0xe8, 0x8d, 0xcd, 0x3a, 0xa8,
	0xc5, 0xc6, 0x4a, 0x9e, 0x7a, 0xcd, 0xb6, 0x5b, 0x3e, 0x5f, 0x6c, 0x6c, 0x8e, 0x88, 0x31, 0xba,
	0x6f, 0x10, 0x3e, 0xe3, 0x01, 0x8b, 0xe3, 0x68, 0xbb, 0xd6, 0x85, 0x8e, 0x92, 0xe4, 0xb6, 0xe5,
	0x9f, 0x3c, 0xc3, 0x68, 0xad, 0xe9, 0x32, 0xa8, 0x51, 0xf9, 0x80, 0x30, 0xa9, 0x06, 0xc1, 0x2a,
	0x30, 0xe1, 0xb7, 0xaa, 0x4a, 0x89, 0xb0, 0x99, 0x28, 0x20, 0xf7, 0xac, 0x42, 0x87, 0x41, 0x2d,
	0x35, 0x5b, 0x9a, 0x37, 0x66, 0x6f, 0x11, 0x3e, 0xab, 0x07, 0xfc, 0x42, 0x94, 0x48, 0x05, 0x82,
	0xcc, 0x14, 0x3a, 0x16, 0x06, 0x94, 0x76, 0xba, 0x53, 0x0e, 0x3e, 0x22, 0xe4, 0x01, 0x0b, 0x16,
	0x1b, 0x2b, 0xe6, 0xa7, 0x35, 0x63, 0xfb, 0xf1, 0xb3, 0x54, 0x31, 0xa1, 0x21, 0xd8, 0x08, 0xbd,
	0x47, 0xf8, 0xdc, 0xe3, 0x44, 0x6c, 0x40, 0xd6, 0xc8, 0x2e, 0xf4, 0x38, 0xa6, 0x95, 0xee, 0x96,
	0xa4, 0x8f, 0x38, 0x2d, 0x43, 0x29, 0xa7, 0xe3, 0x58, 0x31, 0xa7, 0x61, 0xda, 0x38, 0x7d, 0x42,
	0xf8, 0x82, 0x07, 0xeb, 0x02, 0x64, 0x4b, 0xcf, 0xed, 0xde, 0x41, 0x29, 0xc9, 0x9c, 0xe5, 0x06,
	0x0c, 0xa3, 0xda, 0xad, 0x7a, 0x8a, 0x84, 0x23, 0x87, 0x84, 0x07, 0x12, 0x3a, 0x41, 0x66, 0x6c,
	0xf4, 0x0d, 0xe7, 0x2d, 0xf3, 0xf3, 0xe0, 0x62, 0x87, 0xc4, 0x49, 0x19, 0xda, 0x72, 0x3e, 0xd9,
	0xd9, 0xa3, 0xce, 0xee, 0x1e, 0x75, 0x0e, 0xf6, 0x28, 0x7a, 0x9d, 0x52, 0xf4, 0x25, 0xa5, 0xe8,
	0x57, 0x4a, 0xd1, 0x4e, 0x4a, 0xd1, 0xef, 0x94, 0xa2, 0x3f, 0x29, 0x75, 0x0e, 0x52, 0x8a, 0xde,
	0xed, 0x53, 0x67, 0x67, 0x9f, 0x3a, 0xbb, 0xfb, 0xd4, 0x79, 0x36, 0xbb, 0x11, 0xaa, 0x56, 0xd2,
	0x74, 0x7d, 0xde, 0xae, 0x68, 0x8d, 0x90, 0x9b, 0xc7, 0xbc, 0xcb, 0xf2, 0x4c, 0xf6, 0xbd, 0xf9,
	0xdf, 0xbf, 0x9b, 0xf2, 0x8d, 0xbf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x62, 0xee, 0x13, 0x21, 0xbf,
	0x0b, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// AdminServiceClient is the client API for AdminService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type AdminServiceClient interface {
	// DescribeWorkflowExecution returns information about the internal states of workflow execution.
	DescribeWorkflowExecution(ctx context.Context, in *DescribeWorkflowExecutionRequest, opts ...grpc.CallOption) (*DescribeWorkflowExecutionResponse, error)
	// DescribeHistoryHost returns information about the internal states of a history host
	DescribeHistoryHost(ctx context.Context, in *DescribeHistoryHostRequest, opts ...grpc.CallOption) (*DescribeHistoryHostResponse, error)
	CloseShard(ctx context.Context, in *CloseShardRequest, opts ...grpc.CallOption) (*CloseShardResponse, error)
	RemoveTask(ctx context.Context, in *RemoveTaskRequest, opts ...grpc.CallOption) (*RemoveTaskResponse, error)
	// Returns the raw history of specified workflow execution.  It fails with 'NotFound' if specified workflow
	// execution in unknown to the service.
	GetWorkflowExecutionRawHistory(ctx context.Context, in *GetWorkflowExecutionRawHistoryRequest, opts ...grpc.CallOption) (*GetWorkflowExecutionRawHistoryResponse, error)
	// Returns the raw history of specified workflow execution.  It fails with 'NotFound' if specified workflow
	// execution in unknown to the service.
	// StartEventId defines the beginning of the event to fetch. The first event is inclusive.
	// EndEventId and EndEventVersion defines the end of the event to fetch. The end event is exclusive.
	GetWorkflowExecutionRawHistoryV2(ctx context.Context, in *GetWorkflowExecutionRawHistoryV2Request, opts ...grpc.CallOption) (*GetWorkflowExecutionRawHistoryV2Response, error)
	// GetReplicationMessages returns new replication tasks since the read level provided in the token.
	GetReplicationMessages(ctx context.Context, in *GetReplicationMessagesRequest, opts ...grpc.CallOption) (*GetReplicationMessagesResponse, error)
	// GetNamespaceReplicationMessages returns new namespace replication tasks since last retrieved task Id.
	GetNamespaceReplicationMessages(ctx context.Context, in *GetNamespaceReplicationMessagesRequest, opts ...grpc.CallOption) (*GetNamespaceReplicationMessagesResponse, error)
	// GetDLQReplicationMessages return replication messages based on DLQ info.
	GetDLQReplicationMessages(ctx context.Context, in *GetDLQReplicationMessagesRequest, opts ...grpc.CallOption) (*GetDLQReplicationMessagesResponse, error)
	// ReapplyEvents applies stale events to the current workflow and current run.
	ReapplyEvents(ctx context.Context, in *ReapplyEventsRequest, opts ...grpc.CallOption) (*ReapplyEventsResponse, error)
	// AddSearchAttribute whitelist search attribute in request.
	AddSearchAttribute(ctx context.Context, in *AddSearchAttributeRequest, opts ...grpc.CallOption) (*AddSearchAttributeResponse, error)
	// DescribeCluster returns information about Temporal cluster.
	DescribeCluster(ctx context.Context, in *DescribeClusterRequest, opts ...grpc.CallOption) (*DescribeClusterResponse, error)
	// ReadDLQMessages returns messages from DLQ.
	ReadDLQMessages(ctx context.Context, in *ReadDLQMessagesRequest, opts ...grpc.CallOption) (*ReadDLQMessagesResponse, error)
	// PurgeDLQMessages purges messages from DLQ.
	PurgeDLQMessages(ctx context.Context, in *PurgeDLQMessagesRequest, opts ...grpc.CallOption) (*PurgeDLQMessagesResponse, error)
	// MergeDLQMessages merges messages from DLQ.
	MergeDLQMessages(ctx context.Context, in *MergeDLQMessagesRequest, opts ...grpc.CallOption) (*MergeDLQMessagesResponse, error)
	// RefreshWorkflowTasks refreshes all tasks of a workflow.
	RefreshWorkflowTasks(ctx context.Context, in *RefreshWorkflowTasksRequest, opts ...grpc.CallOption) (*RefreshWorkflowTasksResponse, error)
	// ResendReplicationTasks requests replication tasks from remote cluster and apply tasks to current cluster.
	ResendReplicationTasks(ctx context.Context, in *ResendReplicationTasksRequest, opts ...grpc.CallOption) (*ResendReplicationTasksResponse, error)
}

type adminServiceClient struct {
	cc *grpc.ClientConn
}

func NewAdminServiceClient(cc *grpc.ClientConn) AdminServiceClient {
	return &adminServiceClient{cc}
}

func (c *adminServiceClient) DescribeWorkflowExecution(ctx context.Context, in *DescribeWorkflowExecutionRequest, opts ...grpc.CallOption) (*DescribeWorkflowExecutionResponse, error) {
	out := new(DescribeWorkflowExecutionResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/DescribeWorkflowExecution", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) DescribeHistoryHost(ctx context.Context, in *DescribeHistoryHostRequest, opts ...grpc.CallOption) (*DescribeHistoryHostResponse, error) {
	out := new(DescribeHistoryHostResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/DescribeHistoryHost", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) CloseShard(ctx context.Context, in *CloseShardRequest, opts ...grpc.CallOption) (*CloseShardResponse, error) {
	out := new(CloseShardResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/CloseShard", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) RemoveTask(ctx context.Context, in *RemoveTaskRequest, opts ...grpc.CallOption) (*RemoveTaskResponse, error) {
	out := new(RemoveTaskResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/RemoveTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) GetWorkflowExecutionRawHistory(ctx context.Context, in *GetWorkflowExecutionRawHistoryRequest, opts ...grpc.CallOption) (*GetWorkflowExecutionRawHistoryResponse, error) {
	out := new(GetWorkflowExecutionRawHistoryResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistory", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) GetWorkflowExecutionRawHistoryV2(ctx context.Context, in *GetWorkflowExecutionRawHistoryV2Request, opts ...grpc.CallOption) (*GetWorkflowExecutionRawHistoryV2Response, error) {
	out := new(GetWorkflowExecutionRawHistoryV2Response)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistoryV2", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) GetReplicationMessages(ctx context.Context, in *GetReplicationMessagesRequest, opts ...grpc.CallOption) (*GetReplicationMessagesResponse, error) {
	out := new(GetReplicationMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/GetReplicationMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) GetNamespaceReplicationMessages(ctx context.Context, in *GetNamespaceReplicationMessagesRequest, opts ...grpc.CallOption) (*GetNamespaceReplicationMessagesResponse, error) {
	out := new(GetNamespaceReplicationMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/GetNamespaceReplicationMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) GetDLQReplicationMessages(ctx context.Context, in *GetDLQReplicationMessagesRequest, opts ...grpc.CallOption) (*GetDLQReplicationMessagesResponse, error) {
	out := new(GetDLQReplicationMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/GetDLQReplicationMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) ReapplyEvents(ctx context.Context, in *ReapplyEventsRequest, opts ...grpc.CallOption) (*ReapplyEventsResponse, error) {
	out := new(ReapplyEventsResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/ReapplyEvents", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) AddSearchAttribute(ctx context.Context, in *AddSearchAttributeRequest, opts ...grpc.CallOption) (*AddSearchAttributeResponse, error) {
	out := new(AddSearchAttributeResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/AddSearchAttribute", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) DescribeCluster(ctx context.Context, in *DescribeClusterRequest, opts ...grpc.CallOption) (*DescribeClusterResponse, error) {
	out := new(DescribeClusterResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/DescribeCluster", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) ReadDLQMessages(ctx context.Context, in *ReadDLQMessagesRequest, opts ...grpc.CallOption) (*ReadDLQMessagesResponse, error) {
	out := new(ReadDLQMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/ReadDLQMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) PurgeDLQMessages(ctx context.Context, in *PurgeDLQMessagesRequest, opts ...grpc.CallOption) (*PurgeDLQMessagesResponse, error) {
	out := new(PurgeDLQMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/PurgeDLQMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) MergeDLQMessages(ctx context.Context, in *MergeDLQMessagesRequest, opts ...grpc.CallOption) (*MergeDLQMessagesResponse, error) {
	out := new(MergeDLQMessagesResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/MergeDLQMessages", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) RefreshWorkflowTasks(ctx context.Context, in *RefreshWorkflowTasksRequest, opts ...grpc.CallOption) (*RefreshWorkflowTasksResponse, error) {
	out := new(RefreshWorkflowTasksResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/RefreshWorkflowTasks", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *adminServiceClient) ResendReplicationTasks(ctx context.Context, in *ResendReplicationTasksRequest, opts ...grpc.CallOption) (*ResendReplicationTasksResponse, error) {
	out := new(ResendReplicationTasksResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.adminservice.v1.AdminService/ResendReplicationTasks", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AdminServiceServer is the server API for AdminService service.
type AdminServiceServer interface {
	// DescribeWorkflowExecution returns information about the internal states of workflow execution.
	DescribeWorkflowExecution(context.Context, *DescribeWorkflowExecutionRequest) (*DescribeWorkflowExecutionResponse, error)
	// DescribeHistoryHost returns information about the internal states of a history host
	DescribeHistoryHost(context.Context, *DescribeHistoryHostRequest) (*DescribeHistoryHostResponse, error)
	CloseShard(context.Context, *CloseShardRequest) (*CloseShardResponse, error)
	RemoveTask(context.Context, *RemoveTaskRequest) (*RemoveTaskResponse, error)
	// Returns the raw history of specified workflow execution.  It fails with 'NotFound' if specified workflow
	// execution in unknown to the service.
	GetWorkflowExecutionRawHistory(context.Context, *GetWorkflowExecutionRawHistoryRequest) (*GetWorkflowExecutionRawHistoryResponse, error)
	// Returns the raw history of specified workflow execution.  It fails with 'NotFound' if specified workflow
	// execution in unknown to the service.
	// StartEventId defines the beginning of the event to fetch. The first event is inclusive.
	// EndEventId and EndEventVersion defines the end of the event to fetch. The end event is exclusive.
	GetWorkflowExecutionRawHistoryV2(context.Context, *GetWorkflowExecutionRawHistoryV2Request) (*GetWorkflowExecutionRawHistoryV2Response, error)
	// GetReplicationMessages returns new replication tasks since the read level provided in the token.
	GetReplicationMessages(context.Context, *GetReplicationMessagesRequest) (*GetReplicationMessagesResponse, error)
	// GetNamespaceReplicationMessages returns new namespace replication tasks since last retrieved task Id.
	GetNamespaceReplicationMessages(context.Context, *GetNamespaceReplicationMessagesRequest) (*GetNamespaceReplicationMessagesResponse, error)
	// GetDLQReplicationMessages return replication messages based on DLQ info.
	GetDLQReplicationMessages(context.Context, *GetDLQReplicationMessagesRequest) (*GetDLQReplicationMessagesResponse, error)
	// ReapplyEvents applies stale events to the current workflow and current run.
	ReapplyEvents(context.Context, *ReapplyEventsRequest) (*ReapplyEventsResponse, error)
	// AddSearchAttribute whitelist search attribute in request.
	AddSearchAttribute(context.Context, *AddSearchAttributeRequest) (*AddSearchAttributeResponse, error)
	// DescribeCluster returns information about Temporal cluster.
	DescribeCluster(context.Context, *DescribeClusterRequest) (*DescribeClusterResponse, error)
	// ReadDLQMessages returns messages from DLQ.
	ReadDLQMessages(context.Context, *ReadDLQMessagesRequest) (*ReadDLQMessagesResponse, error)
	// PurgeDLQMessages purges messages from DLQ.
	PurgeDLQMessages(context.Context, *PurgeDLQMessagesRequest) (*PurgeDLQMessagesResponse, error)
	// MergeDLQMessages merges messages from DLQ.
	MergeDLQMessages(context.Context, *MergeDLQMessagesRequest) (*MergeDLQMessagesResponse, error)
	// RefreshWorkflowTasks refreshes all tasks of a workflow.
	RefreshWorkflowTasks(context.Context, *RefreshWorkflowTasksRequest) (*RefreshWorkflowTasksResponse, error)
	// ResendReplicationTasks requests replication tasks from remote cluster and apply tasks to current cluster.
	ResendReplicationTasks(context.Context, *ResendReplicationTasksRequest) (*ResendReplicationTasksResponse, error)
}

// UnimplementedAdminServiceServer can be embedded to have forward compatible implementations.
type UnimplementedAdminServiceServer struct {
}

func (*UnimplementedAdminServiceServer) DescribeWorkflowExecution(ctx context.Context, req *DescribeWorkflowExecutionRequest) (*DescribeWorkflowExecutionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DescribeWorkflowExecution not implemented")
}
func (*UnimplementedAdminServiceServer) DescribeHistoryHost(ctx context.Context, req *DescribeHistoryHostRequest) (*DescribeHistoryHostResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DescribeHistoryHost not implemented")
}
func (*UnimplementedAdminServiceServer) CloseShard(ctx context.Context, req *CloseShardRequest) (*CloseShardResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CloseShard not implemented")
}
func (*UnimplementedAdminServiceServer) RemoveTask(ctx context.Context, req *RemoveTaskRequest) (*RemoveTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveTask not implemented")
}
func (*UnimplementedAdminServiceServer) GetWorkflowExecutionRawHistory(ctx context.Context, req *GetWorkflowExecutionRawHistoryRequest) (*GetWorkflowExecutionRawHistoryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkflowExecutionRawHistory not implemented")
}
func (*UnimplementedAdminServiceServer) GetWorkflowExecutionRawHistoryV2(ctx context.Context, req *GetWorkflowExecutionRawHistoryV2Request) (*GetWorkflowExecutionRawHistoryV2Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkflowExecutionRawHistoryV2 not implemented")
}
func (*UnimplementedAdminServiceServer) GetReplicationMessages(ctx context.Context, req *GetReplicationMessagesRequest) (*GetReplicationMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetReplicationMessages not implemented")
}
func (*UnimplementedAdminServiceServer) GetNamespaceReplicationMessages(ctx context.Context, req *GetNamespaceReplicationMessagesRequest) (*GetNamespaceReplicationMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNamespaceReplicationMessages not implemented")
}
func (*UnimplementedAdminServiceServer) GetDLQReplicationMessages(ctx context.Context, req *GetDLQReplicationMessagesRequest) (*GetDLQReplicationMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDLQReplicationMessages not implemented")
}
func (*UnimplementedAdminServiceServer) ReapplyEvents(ctx context.Context, req *ReapplyEventsRequest) (*ReapplyEventsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReapplyEvents not implemented")
}
func (*UnimplementedAdminServiceServer) AddSearchAttribute(ctx context.Context, req *AddSearchAttributeRequest) (*AddSearchAttributeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddSearchAttribute not implemented")
}
func (*UnimplementedAdminServiceServer) DescribeCluster(ctx context.Context, req *DescribeClusterRequest) (*DescribeClusterResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DescribeCluster not implemented")
}
func (*UnimplementedAdminServiceServer) ReadDLQMessages(ctx context.Context, req *ReadDLQMessagesRequest) (*ReadDLQMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReadDLQMessages not implemented")
}
func (*UnimplementedAdminServiceServer) PurgeDLQMessages(ctx context.Context, req *PurgeDLQMessagesRequest) (*PurgeDLQMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PurgeDLQMessages not implemented")
}
func (*UnimplementedAdminServiceServer) MergeDLQMessages(ctx context.Context, req *MergeDLQMessagesRequest) (*MergeDLQMessagesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method MergeDLQMessages not implemented")
}
func (*UnimplementedAdminServiceServer) RefreshWorkflowTasks(ctx context.Context, req *RefreshWorkflowTasksRequest) (*RefreshWorkflowTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RefreshWorkflowTasks not implemented")
}
func (*UnimplementedAdminServiceServer) ResendReplicationTasks(ctx context.Context, req *ResendReplicationTasksRequest) (*ResendReplicationTasksResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ResendReplicationTasks not implemented")
}

func RegisterAdminServiceServer(s *grpc.Server, srv AdminServiceServer) {
	s.RegisterService(&_AdminService_serviceDesc, srv)
}

func _AdminService_DescribeWorkflowExecution_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribeWorkflowExecutionRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).DescribeWorkflowExecution(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/DescribeWorkflowExecution",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).DescribeWorkflowExecution(ctx, req.(*DescribeWorkflowExecutionRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_DescribeHistoryHost_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribeHistoryHostRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).DescribeHistoryHost(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/DescribeHistoryHost",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).DescribeHistoryHost(ctx, req.(*DescribeHistoryHostRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_CloseShard_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CloseShardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).CloseShard(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/CloseShard",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).CloseShard(ctx, req.(*CloseShardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_RemoveTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemoveTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).RemoveTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/RemoveTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).RemoveTask(ctx, req.(*RemoveTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_GetWorkflowExecutionRawHistory_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWorkflowExecutionRawHistoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).GetWorkflowExecutionRawHistory(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistory",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).GetWorkflowExecutionRawHistory(ctx, req.(*GetWorkflowExecutionRawHistoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_GetWorkflowExecutionRawHistoryV2_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetWorkflowExecutionRawHistoryV2Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).GetWorkflowExecutionRawHistoryV2(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/GetWorkflowExecutionRawHistoryV2",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).GetWorkflowExecutionRawHistoryV2(ctx, req.(*GetWorkflowExecutionRawHistoryV2Request))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_GetReplicationMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetReplicationMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).GetReplicationMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/GetReplicationMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).GetReplicationMessages(ctx, req.(*GetReplicationMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_GetNamespaceReplicationMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetNamespaceReplicationMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).GetNamespaceReplicationMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/GetNamespaceReplicationMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).GetNamespaceReplicationMessages(ctx, req.(*GetNamespaceReplicationMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_GetDLQReplicationMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDLQReplicationMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).GetDLQReplicationMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/GetDLQReplicationMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).GetDLQReplicationMessages(ctx, req.(*GetDLQReplicationMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_ReapplyEvents_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReapplyEventsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).ReapplyEvents(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/ReapplyEvents",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).ReapplyEvents(ctx, req.(*ReapplyEventsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_AddSearchAttribute_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddSearchAttributeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).AddSearchAttribute(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/AddSearchAttribute",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).AddSearchAttribute(ctx, req.(*AddSearchAttributeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_DescribeCluster_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribeClusterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).DescribeCluster(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/DescribeCluster",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).DescribeCluster(ctx, req.(*DescribeClusterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_ReadDLQMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadDLQMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).ReadDLQMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/ReadDLQMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).ReadDLQMessages(ctx, req.(*ReadDLQMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_PurgeDLQMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PurgeDLQMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).PurgeDLQMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/PurgeDLQMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).PurgeDLQMessages(ctx, req.(*PurgeDLQMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_MergeDLQMessages_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MergeDLQMessagesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).MergeDLQMessages(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/MergeDLQMessages",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).MergeDLQMessages(ctx, req.(*MergeDLQMessagesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_RefreshWorkflowTasks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RefreshWorkflowTasksRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).RefreshWorkflowTasks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/RefreshWorkflowTasks",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).RefreshWorkflowTasks(ctx, req.(*RefreshWorkflowTasksRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _AdminService_ResendReplicationTasks_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ResendReplicationTasksRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServiceServer).ResendReplicationTasks(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.adminservice.v1.AdminService/ResendReplicationTasks",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServiceServer).ResendReplicationTasks(ctx, req.(*ResendReplicationTasksRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _AdminService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "temporal.server.api.adminservice.v1.AdminService",
	HandlerType: (*AdminServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DescribeWorkflowExecution",
			Handler:    _AdminService_DescribeWorkflowExecution_Handler,
		},
		{
			MethodName: "DescribeHistoryHost",
			Handler:    _AdminService_DescribeHistoryHost_Handler,
		},
		{
			MethodName: "CloseShard",
			Handler:    _AdminService_CloseShard_Handler,
		},
		{
			MethodName: "RemoveTask",
			Handler:    _AdminService_RemoveTask_Handler,
		},
		{
			MethodName: "GetWorkflowExecutionRawHistory",
			Handler:    _AdminService_GetWorkflowExecutionRawHistory_Handler,
		},
		{
			MethodName: "GetWorkflowExecutionRawHistoryV2",
			Handler:    _AdminService_GetWorkflowExecutionRawHistoryV2_Handler,
		},
		{
			MethodName: "GetReplicationMessages",
			Handler:    _AdminService_GetReplicationMessages_Handler,
		},
		{
			MethodName: "GetNamespaceReplicationMessages",
			Handler:    _AdminService_GetNamespaceReplicationMessages_Handler,
		},
		{
			MethodName: "GetDLQReplicationMessages",
			Handler:    _AdminService_GetDLQReplicationMessages_Handler,
		},
		{
			MethodName: "ReapplyEvents",
			Handler:    _AdminService_ReapplyEvents_Handler,
		},
		{
			MethodName: "AddSearchAttribute",
			Handler:    _AdminService_AddSearchAttribute_Handler,
		},
		{
			MethodName: "DescribeCluster",
			Handler:    _AdminService_DescribeCluster_Handler,
		},
		{
			MethodName: "ReadDLQMessages",
			Handler:    _AdminService_ReadDLQMessages_Handler,
		},
		{
			MethodName: "PurgeDLQMessages",
			Handler:    _AdminService_PurgeDLQMessages_Handler,
		},
		{
			MethodName: "MergeDLQMessages",
			Handler:    _AdminService_MergeDLQMessages_Handler,
		},
		{
			MethodName: "RefreshWorkflowTasks",
			Handler:    _AdminService_RefreshWorkflowTasks_Handler,
		},
		{
			MethodName: "ResendReplicationTasks",
			Handler:    _AdminService_ResendReplicationTasks_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "temporal/server/api/adminservice/v1/service.proto",
}
