package interceptor

import (
	"context"
	"io"

	"go.temporal.io/api/serviceerror"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

func CustomErrorStreamInterceptor(
	srv interface{},
	serverStream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	err := handler(srv, serverStream)
	if err != nil {
		err = serviceerror.ToStatus(err).Err()
	}
	return err
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
	case codes.Unknown:
		return serviceerror.NewInternal(st.Message())
	default:
		return serviceerrors.FromStatus(st)
	}
}
