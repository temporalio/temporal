// Command chasmwasm builds a wasip1 reactor module exposing the local server to a host.
//
//	GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared -o local-server.wasm ./cmd/chasmwasm
//
// The host calls temporal_alloc to get a buffer for a request, writes the request into it, and calls
// temporal_call with the gRPC method name and the request. The result packs a pointer and length
// (ptr<<32 | len) to a response buffer whose first byte is a gRPC status code; the rest is the
// response proto when the code is 0 and an error message otherwise. The host frees buffers with
// temporal_free. WorkflowService methods are named without their service, and AdminService methods
// with the prefix "AdminService/".
package main

import (
	"context"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/localserver"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {}

var (
	server  *localserver.Server
	buffers = map[uint32][]byte{}
	methods = map[string]func(context.Context, []byte) (proto.Message, error){
		"GetSystemInfo": method(func(ctx context.Context, r *workflowservice.GetSystemInfoRequest) (proto.Message, error) {
			return server.GetSystemInfo(ctx, r)
		}),
		"DescribeNamespace": method(func(ctx context.Context, r *workflowservice.DescribeNamespaceRequest) (proto.Message, error) {
			return server.DescribeNamespace(ctx, r)
		}),
		"ShutdownWorker": method(func(ctx context.Context, r *workflowservice.ShutdownWorkerRequest) (proto.Message, error) {
			return server.ShutdownWorker(ctx, r)
		}),
		"StartWorkflowExecution": method(func(ctx context.Context, r *workflowservice.StartWorkflowExecutionRequest) (proto.Message, error) {
			return server.StartWorkflowExecution(ctx, r)
		}),
		"DeleteWorkflowExecution": method(func(ctx context.Context, r *workflowservice.DeleteWorkflowExecutionRequest) (proto.Message, error) {
			return server.DeleteWorkflowExecution(ctx, r)
		}),
		"GetWorkflowExecutionHistory": method(func(ctx context.Context, r *workflowservice.GetWorkflowExecutionHistoryRequest) (proto.Message, error) {
			return server.GetWorkflowExecutionHistory(ctx, r)
		}),
		"PollWorkflowTaskQueue": method(func(ctx context.Context, r *workflowservice.PollWorkflowTaskQueueRequest) (proto.Message, error) {
			return server.PollWorkflowTaskQueue(ctx, r)
		}),
		"RespondWorkflowTaskCompleted": method(func(ctx context.Context, r *workflowservice.RespondWorkflowTaskCompletedRequest) (proto.Message, error) {
			return server.RespondWorkflowTaskCompleted(ctx, r)
		}),
		"RespondWorkflowTaskFailed": method(func(ctx context.Context, r *workflowservice.RespondWorkflowTaskFailedRequest) (proto.Message, error) {
			return server.RespondWorkflowTaskFailed(ctx, r)
		}),
		"PollActivityTaskQueue": method(func(ctx context.Context, r *workflowservice.PollActivityTaskQueueRequest) (proto.Message, error) {
			return server.PollActivityTaskQueue(ctx, r)
		}),
		"RespondActivityTaskCompleted": method(func(ctx context.Context, r *workflowservice.RespondActivityTaskCompletedRequest) (proto.Message, error) {
			return server.RespondActivityTaskCompleted(ctx, r)
		}),
		"RespondActivityTaskFailed": method(func(ctx context.Context, r *workflowservice.RespondActivityTaskFailedRequest) (proto.Message, error) {
			return server.RespondActivityTaskFailed(ctx, r)
		}),
		"AdminService/ImportWorkflowExecution": method(func(ctx context.Context, r *adminservice.ImportWorkflowExecutionRequest) (proto.Message, error) {
			return server.Admin().ImportWorkflowExecution(ctx, r)
		}),
		"AdminService/GetWorkflowExecutionRawHistoryV2": method(func(ctx context.Context, r *adminservice.GetWorkflowExecutionRawHistoryV2Request) (proto.Message, error) {
			return server.Admin().GetWorkflowExecutionRawHistoryV2(ctx, r)
		}),
		"AdminService/DeleteWorkflowExecution": method(func(ctx context.Context, r *adminservice.DeleteWorkflowExecutionRequest) (proto.Message, error) {
			return server.Admin().DeleteWorkflowExecution(ctx, r)
		}),
	}
)

func method[R any, PR interface {
	*R
	proto.Message
}](handle func(context.Context, PR) (proto.Message, error)) func(context.Context, []byte) (proto.Message, error) {
	return func(ctx context.Context, data []byte) (proto.Message, error) {
		request := PR(new(R))
		if err := proto.Unmarshal(data, request); err != nil {
			return nil, serviceerror.NewInvalidArgument(err.Error())
		}
		return handle(ctx, request)
	}
}

//go:wasmexport temporal_init
func initServer(unixNanos int64) uint32 {
	var err error
	server, err = localserver.New(time.Unix(0, unixNanos), uuid.NewString)
	if err != nil {
		return 1
	}
	return 0
}

// temporal_advance_time sets the clock, runs the tasks that are due, and responds with the time at
// which the next task is due (absent if none is pending).
//
//go:wasmexport temporal_advance_time
func advanceTime(unixNanos int64) uint64 {
	if err := server.AdvanceTime(context.Background(), time.Unix(0, unixNanos)); err != nil {
		return respond(nil, err)
	}
	deadline, ok := server.NextDeadline()
	if !ok {
		return respond(&timestamppb.Timestamp{}, nil)
	}
	return respond(timestamppb.New(deadline), nil)
}

//go:wasmexport temporal_call
func call(methodPtr, methodLen, requestPtr, requestLen uint32) uint64 {
	name := string(buffers[methodPtr][:methodLen])
	handle, ok := methods[name]
	if !ok {
		return respond(nil, serviceerror.NewUnimplementedf("method %s is not implemented", name))
	}
	response, err := handle(context.Background(), buffers[requestPtr][:requestLen])
	return respond(response, err)
}

//go:wasmexport temporal_alloc
func alloc(size uint32) uint32 {
	buffer := make([]byte, size+1) // +1 so that a zero-size buffer has an address
	ptr := uint32(uintptr(unsafe.Pointer(&buffer[0])))
	buffers[ptr] = buffer
	return ptr
}

//go:wasmexport temporal_free
func free(ptr uint32) {
	delete(buffers, ptr)
}

func respond(response proto.Message, err error) uint64 {
	var out []byte
	if err != nil {
		status := serviceerror.ToStatus(err)
		out = append([]byte{byte(status.Code())}, status.Message()...)
	} else if data, marshalErr := proto.Marshal(response); marshalErr != nil {
		out = append([]byte{byte(codes.Internal)}, marshalErr.Error()...)
	} else {
		out = append([]byte{byte(codes.OK)}, data...)
	}
	ptr := alloc(uint32(len(out)))
	copy(buffers[ptr], out)
	return uint64(ptr)<<32 | uint64(len(out))
}
