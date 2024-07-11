// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package callbacks

import (
	"context"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CanGetCompletionEvent interface {
	GetCompletionEvent(ctx context.Context) (*historypb.HistoryEvent, error)
}

type hsmInvocation struct {
	hsm             *persistencepb.Callback_Hsm
	completionEvent *historypb.HistoryEvent
}

func isRetryableRpcResponse(err error) bool {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		// TODO(Tianyu): Are there other types of retryable errors, and are these always retryable?
		case codes.Unavailable:
		case codes.DeadlineExceeded:
		case codes.ResourceExhausted:
		case codes.Aborted:
		case codes.Internal:
			return true
		default:
			return false
		}
	}
	// Not a gRPC induced error
	// TODO(Tianyu): Can handler return some kind of non-gRPC retryable error?
	return false
}

func (s hsmInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) (invocationResult, error) {
	completionEventSerialized, err := s.completionEvent.Marshal()
	if err != nil {
		return Failed, err
	}

	request := historyservice.InvokeStateMachineMethodRequest{
		NamespaceId: ns.ID().String(),
		WorkflowId:  s.hsm.WorkflowId,
		RunId:       s.hsm.RunId,
		Ref:         s.hsm.Ref,
		MethodName:  s.hsm.Method,
		Input:       completionEventSerialized,
	}

	// TODO(Tianyu): Will we want to log the response somewhere?
	_, err = e.HistoryClient.InvokeStateMachineMethod(ctx, &request)
	// TODO(Tianyu): Add metrics
	if err != nil {
		e.Logger.Error("Callback request failed", tag.Error(err))
		if isRetryableRpcResponse(err) {
			return Retry, err
		}
		return Failed, err
	}
	return Ok, nil
}
