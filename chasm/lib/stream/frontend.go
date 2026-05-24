package stream

import (
	"context"

	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type frontendHandler struct {
	streampb.UnimplementedStreamServiceServer

	client       streampb.StreamServiceClient
	namespaceReg namespace.Registry
}

func NewFrontendHandler(
	client streampb.StreamServiceClient,
	namespaceReg namespace.Registry,
) streampb.StreamServiceServer {
	return &frontendHandler{
		client:       client,
		namespaceReg: namespaceReg,
	}
}

func RegisterFrontendService(
	server *grpc.Server,
	handler streampb.StreamServiceServer,
) {
	streampb.RegisterStreamServiceServer(server, handler)
}

func (h *frontendHandler) CreateStream(
	ctx context.Context,
	req *streampb.CreateStreamRequest,
) (*streampb.CreateStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.CreateStream(ctx, modifiedReq)
}

func (h *frontendHandler) DescribeStream(
	ctx context.Context,
	req *streampb.DescribeStreamRequest,
) (*streampb.DescribeStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.DescribeStream(ctx, modifiedReq)
}

func (h *frontendHandler) Publish(
	ctx context.Context,
	req *streampb.PublishRequest,
) (*streampb.PublishResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.Publish(ctx, modifiedReq)
}

func (h *frontendHandler) ReadRange(
	ctx context.Context,
	req *streampb.ReadRangeRequest,
) (*streampb.ReadRangeResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.ReadRange(ctx, modifiedReq)
}

func (h *frontendHandler) Close(
	ctx context.Context,
	req *streampb.CloseRequest,
) (*streampb.CloseResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.Close(ctx, modifiedReq)
}

func (h *frontendHandler) DeleteStream(
	ctx context.Context,
	req *streampb.DeleteStreamRequest,
) (*streampb.DeleteStreamResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.DeleteStream(ctx, modifiedReq)
}

func (h *frontendHandler) Truncate(
	ctx context.Context,
	req *streampb.TruncateRequest,
) (*streampb.TruncateResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.Truncate(ctx, modifiedReq)
}

func (h *frontendHandler) ListStreams(
	ctx context.Context,
	req *streampb.ListStreamsRequest,
) (*streampb.ListStreamsResponse, error) {
	namespaceID, err := h.namespaceID(req.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	modifiedReq := common.CloneProto(req)
	modifiedReq.NamespaceId = namespaceID
	return h.client.ListStreams(ctx, modifiedReq)
}

func (h *frontendHandler) namespaceID(namespaceName string) (string, error) {
	namespaceID, err := h.namespaceReg.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return "", err
	}
	return namespaceID.String(), nil
}
