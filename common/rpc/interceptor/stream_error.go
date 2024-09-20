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

package interceptor

import (
	"context"
	"io"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	ClientStreamErrorInterceptor struct {
		grpc.ClientStream
	}
)

var _ grpc.ClientStream = (*ClientStreamErrorInterceptor)(nil)

func NewClientStreamErrorInterceptor(
	clientStream grpc.ClientStream,
) *ClientStreamErrorInterceptor {
	return &ClientStreamErrorInterceptor{
		ClientStream: clientStream,
	}
}

func (c *ClientStreamErrorInterceptor) CloseSend() error {
	return errorConvert(c.ClientStream.CloseSend())
}

func (c *ClientStreamErrorInterceptor) SendMsg(m interface{}) error {
	return errorConvert(c.ClientStream.SendMsg(m))
}

func (c *ClientStreamErrorInterceptor) RecvMsg(m interface{}) error {
	return errorConvert(c.ClientStream.RecvMsg(m))
}

func StreamErrorInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	clientStream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, errorConvert(err)
	}
	return NewClientStreamErrorInterceptor(clientStream), nil
}

func errorConvert(err error) error {
	switch err {
	case nil:
		return nil
	case io.EOF:
		return io.EOF
	default:
		return FromStatus(status.Convert(err))
	}
}

// FromStatus converts gRPC Status to service error.
func FromStatus(st *status.Status) error {
	if st == nil {
		return nil
	}

	switch st.Code() {
	case codes.OK:
		return nil
	case codes.DeadlineExceeded:
		return serviceerror.NewDeadlineExceeded(st.Message())
	case codes.Canceled:
		return serviceerror.NewCanceled(st.Message())
	case codes.InvalidArgument:
		return serviceerror.NewInvalidArgument(st.Message())
	case codes.FailedPrecondition:
		return serviceerror.NewFailedPrecondition(st.Message())
	case codes.Unavailable:
		return serviceerror.NewUnavailable(st.Message())
	case codes.Internal:
		return serviceerror.NewInternal(st.Message())
	case codes.Unknown:
		return serviceerror.NewInternal(st.Message())
	default:
		return serviceerror.NewInternal(st.Message())
	}
}
