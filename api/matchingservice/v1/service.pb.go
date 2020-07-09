// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: temporal/server/api/matchingservice/v1/service.proto

package matchingservice

import (
	context "context"
	fmt "fmt"
	math "math"

	proto "github.com/gogo/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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
	proto.RegisterFile("temporal/server/api/matchingservice/v1/service.proto", fileDescriptor_1a5c83076e651916)
}

var fileDescriptor_1a5c83076e651916 = []byte{
	// 464 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x95, 0xb1, 0x8e, 0xd3, 0x30,
	0x18, 0x80, 0xe3, 0x85, 0xc1, 0x08, 0x9d, 0x30, 0x42, 0x88, 0x1b, 0x3c, 0x30, 0x30, 0x26, 0x3a,
	0x60, 0xe3, 0x8e, 0xa3, 0xd7, 0x72, 0x80, 0x04, 0xe2, 0x0e, 0x90, 0x90, 0x58, 0x90, 0x2f, 0xf9,
	0x29, 0x56, 0xd3, 0x38, 0xd8, 0x4e, 0x50, 0x37, 0x9e, 0x00, 0x31, 0x30, 0x31, 0x31, 0x21, 0x06,
	0x26, 0x9e, 0x80, 0x91, 0xb1, 0x63, 0x47, 0x9a, 0x2e, 0x8c, 0x7d, 0x04, 0x94, 0xa6, 0x0e, 0x4d,
	0x68, 0x91, 0x49, 0x6e, 0x6b, 0x1d, 0x7f, 0xdf, 0xff, 0xc5, 0x8a, 0x64, 0x7c, 0x43, 0xc3, 0x30,
	0x16, 0x92, 0x85, 0x9e, 0x02, 0x99, 0x82, 0xf4, 0x58, 0xcc, 0xbd, 0x21, 0xd3, 0xfe, 0x2b, 0x1e,
	0xf5, 0xf3, 0x25, 0xee, 0x83, 0x97, 0xee, 0x78, 0xcb, 0x9f, 0x6e, 0x2c, 0x85, 0x16, 0xe4, 0xaa,
	0xa1, 0xdc, 0x82, 0x72, 0x59, 0xcc, 0xdd, 0x1a, 0xe5, 0xa6, 0x3b, 0xdb, 0x7b, 0x96, 0x76, 0x09,
	0xaf, 0x13, 0x50, 0xfa, 0x85, 0x04, 0x15, 0x8b, 0x48, 0x2d, 0xc7, 0x5c, 0xfb, 0x7e, 0x16, 0x6f,
	0x3d, 0x5c, 0xee, 0x7e, 0x52, 0xec, 0x26, 0x9f, 0x10, 0xbe, 0x70, 0x24, 0xc2, 0xf0, 0x50, 0xc8,
	0x1e, 0xf8, 0x5c, 0x71, 0x11, 0x3d, 0x65, 0x6a, 0x40, 0x0e, 0x5c, 0xbb, 0x26, 0x77, 0x0d, 0xfc,
	0xb8, 0x18, 0xbf, 0xdd, 0x6d, 0xe5, 0x28, 0xd2, 0xaf, 0x38, 0xab, 0x89, 0x1d, 0x5f, 0xf3, 0x94,
	0xeb, 0x51, 0xa3, 0xc4, 0x55, 0xb8, 0x69, 0x62, 0xd5, 0x51, 0x26, 0x7e, 0x40, 0x78, 0xab, 0x13,
	0x04, 0x95, 0x13, 0xbc, 0x65, 0xab, 0xae, 0x81, 0x26, 0x6d, 0xbf, 0x31, 0x5f, 0xcf, 0xaa, 0x9c,
	0xda, 0xff, 0x64, 0xad, 0x3b, 0xb1, 0xfd, 0xc6, 0x7c, 0x99, 0xf5, 0x0e, 0xe1, 0x73, 0xc7, 0x09,
	0xc8, 0xd1, 0x33, 0x21, 0x07, 0x2f, 0x43, 0xf1, 0x86, 0xec, 0xda, 0x4a, 0x2b, 0x98, 0x49, 0xda,
	0x6b, 0x48, 0x97, 0x41, 0xdf, 0x10, 0xbe, 0x5c, 0xfc, 0x0d, 0x16, 0x5b, 0xf2, 0xde, 0xae, 0x18,
	0xc6, 0x21, 0x68, 0x08, 0xc8, 0x3d, 0x5b, 0xfd, 0x46, 0x85, 0x09, 0xbd, 0x7f, 0x0a, 0xa6, 0x32,
	0xfa, 0x33, 0xc2, 0x17, 0xbb, 0x2c, 0xf2, 0x21, 0x7c, 0x94, 0x68, 0xa5, 0x59, 0x14, 0xf0, 0xa8,
	0x9f, 0x7f, 0xa6, 0xa4, 0x67, 0x3b, 0x66, 0x2d, 0x6e, 0x62, 0xef, 0xb4, 0xb4, 0x94, 0xa1, 0x1f,
	0x11, 0x3e, 0xdf, 0x03, 0xe5, 0x4b, 0x7e, 0x02, 0xf9, 0xcb, 0x1c, 0x27, 0x90, 0x00, 0xb9, 0x6d,
	0xab, 0xff, 0x0b, 0x35, 0x81, 0x9d, 0x16, 0x86, 0x32, 0xee, 0x2b, 0xc2, 0x97, 0x1e, 0x70, 0xa5,
	0xcb, 0x67, 0x47, 0x4c, 0x6a, 0xae, 0xb9, 0x88, 0x14, 0x39, 0xb4, 0x1d, 0xb0, 0x41, 0x60, 0x42,
	0xef, 0xb6, 0xf6, 0x98, 0xdc, 0x03, 0x39, 0x9e, 0x52, 0x67, 0x32, 0xa5, 0xce, 0x7c, 0x4a, 0xd1,
	0xdb, 0x8c, 0xa2, 0x2f, 0x19, 0x45, 0x3f, 0x32, 0x8a, 0xc6, 0x19, 0x45, 0x3f, 0x33, 0x8a, 0x7e,
	0x65, 0xd4, 0x99, 0x67, 0x14, 0xbd, 0x9f, 0x51, 0x67, 0x3c, 0xa3, 0xce, 0x64, 0x46, 0x9d, 0xe7,
	0xbb, 0x7d, 0xf1, 0x27, 0x81, 0x8b, 0x7f, 0xdf, 0x1e, 0x37, 0x6b, 0x4b, 0x27, 0x67, 0x16, 0xb7,
	0xc7, 0xf5, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0xaf, 0x3f, 0x69, 0x81, 0xdc, 0x06, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// MatchingServiceClient is the client API for MatchingService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type MatchingServiceClient interface {
	// PollForDecisionTask is called by frontend to process DecisionTask from a specific task queue.  A
	// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
	PollForDecisionTask(ctx context.Context, in *PollForDecisionTaskRequest, opts ...grpc.CallOption) (*PollForDecisionTaskResponse, error)
	// PollForActivityTask is called by frontend to process ActivityTask from a specific task queue.  ActivityTask
	// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
	PollForActivityTask(ctx context.Context, in *PollForActivityTaskRequest, opts ...grpc.CallOption) (*PollForActivityTaskResponse, error)
	// AddDecisionTask is called by the history service when a decision task is scheduled, so that it can be dispatched
	// by the MatchingEngine.
	AddDecisionTask(ctx context.Context, in *AddDecisionTaskRequest, opts ...grpc.CallOption) (*AddDecisionTaskResponse, error)
	// AddActivityTask is called by the history service when a decision task is scheduled, so that it can be dispatched
	// by the MatchingEngine.
	AddActivityTask(ctx context.Context, in *AddActivityTaskRequest, opts ...grpc.CallOption) (*AddActivityTaskResponse, error)
	// QueryWorkflow is called by frontend to query a workflow.
	QueryWorkflow(ctx context.Context, in *QueryWorkflowRequest, opts ...grpc.CallOption) (*QueryWorkflowResponse, error)
	// RespondQueryTaskCompleted is called by frontend to respond query completed.
	RespondQueryTaskCompleted(ctx context.Context, in *RespondQueryTaskCompletedRequest, opts ...grpc.CallOption) (*RespondQueryTaskCompletedResponse, error)
	// CancelOutstandingPoll is called by frontend to unblock long polls on matching for zombie pollers.
	// Our rpc stack does not support context propagation, so when a client connection goes away frontend sees
	// cancellation of context for that handler, but any corresponding calls (long-poll) to matching service does not
	// see the cancellation propagated so it can unblock corresponding long-polls on its end.  This results is tasks
	// being dispatched to zombie pollers in this situation.  This API is added so every time frontend makes a long-poll
	// api call to matching it passes in a pollerId and then calls this API when it detects client connection is closed
	// to unblock long polls for this poller and prevent tasks being sent to these zombie pollers.
	CancelOutstandingPoll(ctx context.Context, in *CancelOutstandingPollRequest, opts ...grpc.CallOption) (*CancelOutstandingPollResponse, error)
	// DescribeTaskQueue returns information about the target task queue, right now this API returns the
	// pollers which polled this task queue in last few minutes.
	DescribeTaskQueue(ctx context.Context, in *DescribeTaskQueueRequest, opts ...grpc.CallOption) (*DescribeTaskQueueResponse, error)
	// ListTaskQueuePartitions returns a map of partitionKey and hostAddress for a task queue.
	ListTaskQueuePartitions(ctx context.Context, in *ListTaskQueuePartitionsRequest, opts ...grpc.CallOption) (*ListTaskQueuePartitionsResponse, error)
}

type matchingServiceClient struct {
	cc *grpc.ClientConn
}

func NewMatchingServiceClient(cc *grpc.ClientConn) MatchingServiceClient {
	return &matchingServiceClient{cc}
}

func (c *matchingServiceClient) PollForDecisionTask(ctx context.Context, in *PollForDecisionTaskRequest, opts ...grpc.CallOption) (*PollForDecisionTaskResponse, error) {
	out := new(PollForDecisionTaskResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/PollForDecisionTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) PollForActivityTask(ctx context.Context, in *PollForActivityTaskRequest, opts ...grpc.CallOption) (*PollForActivityTaskResponse, error) {
	out := new(PollForActivityTaskResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/PollForActivityTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) AddDecisionTask(ctx context.Context, in *AddDecisionTaskRequest, opts ...grpc.CallOption) (*AddDecisionTaskResponse, error) {
	out := new(AddDecisionTaskResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/AddDecisionTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) AddActivityTask(ctx context.Context, in *AddActivityTaskRequest, opts ...grpc.CallOption) (*AddActivityTaskResponse, error) {
	out := new(AddActivityTaskResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/AddActivityTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) QueryWorkflow(ctx context.Context, in *QueryWorkflowRequest, opts ...grpc.CallOption) (*QueryWorkflowResponse, error) {
	out := new(QueryWorkflowResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) RespondQueryTaskCompleted(ctx context.Context, in *RespondQueryTaskCompletedRequest, opts ...grpc.CallOption) (*RespondQueryTaskCompletedResponse, error) {
	out := new(RespondQueryTaskCompletedResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/RespondQueryTaskCompleted", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) CancelOutstandingPoll(ctx context.Context, in *CancelOutstandingPollRequest, opts ...grpc.CallOption) (*CancelOutstandingPollResponse, error) {
	out := new(CancelOutstandingPollResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingPoll", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) DescribeTaskQueue(ctx context.Context, in *DescribeTaskQueueRequest, opts ...grpc.CallOption) (*DescribeTaskQueueResponse, error) {
	out := new(DescribeTaskQueueResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/DescribeTaskQueue", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *matchingServiceClient) ListTaskQueuePartitions(ctx context.Context, in *ListTaskQueuePartitionsRequest, opts ...grpc.CallOption) (*ListTaskQueuePartitionsResponse, error) {
	out := new(ListTaskQueuePartitionsResponse)
	err := c.cc.Invoke(ctx, "/temporal.server.api.matchingservice.v1.MatchingService/ListTaskQueuePartitions", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MatchingServiceServer is the server API for MatchingService service.
type MatchingServiceServer interface {
	// PollForDecisionTask is called by frontend to process DecisionTask from a specific task queue.  A
	// DecisionTask is dispatched to callers for active workflow executions, with pending decisions.
	PollForDecisionTask(context.Context, *PollForDecisionTaskRequest) (*PollForDecisionTaskResponse, error)
	// PollForActivityTask is called by frontend to process ActivityTask from a specific task queue.  ActivityTask
	// is dispatched to callers whenever a ScheduleTask decision is made for a workflow execution.
	PollForActivityTask(context.Context, *PollForActivityTaskRequest) (*PollForActivityTaskResponse, error)
	// AddDecisionTask is called by the history service when a decision task is scheduled, so that it can be dispatched
	// by the MatchingEngine.
	AddDecisionTask(context.Context, *AddDecisionTaskRequest) (*AddDecisionTaskResponse, error)
	// AddActivityTask is called by the history service when a decision task is scheduled, so that it can be dispatched
	// by the MatchingEngine.
	AddActivityTask(context.Context, *AddActivityTaskRequest) (*AddActivityTaskResponse, error)
	// QueryWorkflow is called by frontend to query a workflow.
	QueryWorkflow(context.Context, *QueryWorkflowRequest) (*QueryWorkflowResponse, error)
	// RespondQueryTaskCompleted is called by frontend to respond query completed.
	RespondQueryTaskCompleted(context.Context, *RespondQueryTaskCompletedRequest) (*RespondQueryTaskCompletedResponse, error)
	// CancelOutstandingPoll is called by frontend to unblock long polls on matching for zombie pollers.
	// Our rpc stack does not support context propagation, so when a client connection goes away frontend sees
	// cancellation of context for that handler, but any corresponding calls (long-poll) to matching service does not
	// see the cancellation propagated so it can unblock corresponding long-polls on its end.  This results is tasks
	// being dispatched to zombie pollers in this situation.  This API is added so every time frontend makes a long-poll
	// api call to matching it passes in a pollerId and then calls this API when it detects client connection is closed
	// to unblock long polls for this poller and prevent tasks being sent to these zombie pollers.
	CancelOutstandingPoll(context.Context, *CancelOutstandingPollRequest) (*CancelOutstandingPollResponse, error)
	// DescribeTaskQueue returns information about the target task queue, right now this API returns the
	// pollers which polled this task queue in last few minutes.
	DescribeTaskQueue(context.Context, *DescribeTaskQueueRequest) (*DescribeTaskQueueResponse, error)
	// ListTaskQueuePartitions returns a map of partitionKey and hostAddress for a task queue.
	ListTaskQueuePartitions(context.Context, *ListTaskQueuePartitionsRequest) (*ListTaskQueuePartitionsResponse, error)
}

// UnimplementedMatchingServiceServer can be embedded to have forward compatible implementations.
type UnimplementedMatchingServiceServer struct {
}

func (*UnimplementedMatchingServiceServer) PollForDecisionTask(ctx context.Context, req *PollForDecisionTaskRequest) (*PollForDecisionTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PollForDecisionTask not implemented")
}
func (*UnimplementedMatchingServiceServer) PollForActivityTask(ctx context.Context, req *PollForActivityTaskRequest) (*PollForActivityTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PollForActivityTask not implemented")
}
func (*UnimplementedMatchingServiceServer) AddDecisionTask(ctx context.Context, req *AddDecisionTaskRequest) (*AddDecisionTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddDecisionTask not implemented")
}
func (*UnimplementedMatchingServiceServer) AddActivityTask(ctx context.Context, req *AddActivityTaskRequest) (*AddActivityTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddActivityTask not implemented")
}
func (*UnimplementedMatchingServiceServer) QueryWorkflow(ctx context.Context, req *QueryWorkflowRequest) (*QueryWorkflowResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryWorkflow not implemented")
}
func (*UnimplementedMatchingServiceServer) RespondQueryTaskCompleted(ctx context.Context, req *RespondQueryTaskCompletedRequest) (*RespondQueryTaskCompletedResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RespondQueryTaskCompleted not implemented")
}
func (*UnimplementedMatchingServiceServer) CancelOutstandingPoll(ctx context.Context, req *CancelOutstandingPollRequest) (*CancelOutstandingPollResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelOutstandingPoll not implemented")
}
func (*UnimplementedMatchingServiceServer) DescribeTaskQueue(ctx context.Context, req *DescribeTaskQueueRequest) (*DescribeTaskQueueResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DescribeTaskQueue not implemented")
}
func (*UnimplementedMatchingServiceServer) ListTaskQueuePartitions(ctx context.Context, req *ListTaskQueuePartitionsRequest) (*ListTaskQueuePartitionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListTaskQueuePartitions not implemented")
}

func RegisterMatchingServiceServer(s *grpc.Server, srv MatchingServiceServer) {
	s.RegisterService(&_MatchingService_serviceDesc, srv)
}

func _MatchingService_PollForDecisionTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PollForDecisionTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).PollForDecisionTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/PollForDecisionTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).PollForDecisionTask(ctx, req.(*PollForDecisionTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_PollForActivityTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PollForActivityTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).PollForActivityTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/PollForActivityTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).PollForActivityTask(ctx, req.(*PollForActivityTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_AddDecisionTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddDecisionTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).AddDecisionTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/AddDecisionTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).AddDecisionTask(ctx, req.(*AddDecisionTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_AddActivityTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddActivityTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).AddActivityTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/AddActivityTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).AddActivityTask(ctx, req.(*AddActivityTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_QueryWorkflow_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryWorkflowRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).QueryWorkflow(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).QueryWorkflow(ctx, req.(*QueryWorkflowRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_RespondQueryTaskCompleted_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RespondQueryTaskCompletedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).RespondQueryTaskCompleted(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/RespondQueryTaskCompleted",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).RespondQueryTaskCompleted(ctx, req.(*RespondQueryTaskCompletedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_CancelOutstandingPoll_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelOutstandingPollRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).CancelOutstandingPoll(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingPoll",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).CancelOutstandingPoll(ctx, req.(*CancelOutstandingPollRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_DescribeTaskQueue_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DescribeTaskQueueRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).DescribeTaskQueue(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/DescribeTaskQueue",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).DescribeTaskQueue(ctx, req.(*DescribeTaskQueueRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MatchingService_ListTaskQueuePartitions_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListTaskQueuePartitionsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MatchingServiceServer).ListTaskQueuePartitions(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/ListTaskQueuePartitions",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MatchingServiceServer).ListTaskQueuePartitions(ctx, req.(*ListTaskQueuePartitionsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _MatchingService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "temporal.server.api.matchingservice.v1.MatchingService",
	HandlerType: (*MatchingServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "PollForDecisionTask",
			Handler:    _MatchingService_PollForDecisionTask_Handler,
		},
		{
			MethodName: "PollForActivityTask",
			Handler:    _MatchingService_PollForActivityTask_Handler,
		},
		{
			MethodName: "AddDecisionTask",
			Handler:    _MatchingService_AddDecisionTask_Handler,
		},
		{
			MethodName: "AddActivityTask",
			Handler:    _MatchingService_AddActivityTask_Handler,
		},
		{
			MethodName: "QueryWorkflow",
			Handler:    _MatchingService_QueryWorkflow_Handler,
		},
		{
			MethodName: "RespondQueryTaskCompleted",
			Handler:    _MatchingService_RespondQueryTaskCompleted_Handler,
		},
		{
			MethodName: "CancelOutstandingPoll",
			Handler:    _MatchingService_CancelOutstandingPoll_Handler,
		},
		{
			MethodName: "DescribeTaskQueue",
			Handler:    _MatchingService_DescribeTaskQueue_Handler,
		},
		{
			MethodName: "ListTaskQueuePartitions",
			Handler:    _MatchingService_ListTaskQueuePartitions_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "temporal/server/api/matchingservice/v1/service.proto",
}
