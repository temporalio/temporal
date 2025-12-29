package stream

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/namespace"
)

// StreamFrontendHandler handles stream-related frontend requests.
type StreamFrontendHandler interface {
	CreateStream(ctx context.Context, req *workflowservice.CreateStreamRequest) (*workflowservice.CreateStreamResponse, error)
	AddToStream(ctx context.Context, req *workflowservice.AddToStreamRequest) (*workflowservice.AddToStreamResponse, error)
	PollStream(ctx context.Context, req *workflowservice.PollStreamRequest) (*workflowservice.PollStreamResponse, error)
}

type frontendHandler struct {
	client            streampb.StreamServiceClient
	namespaceRegistry namespace.Registry
}

// NewFrontendHandler creates a new FrontendHandler instance.
func NewFrontendHandler(
	client streampb.StreamServiceClient,
	namespaceRegistry namespace.Registry,
) StreamFrontendHandler {
	return &frontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
	}
}

// CreateStream creates a new stream with optional initial messages.
func (h *frontendHandler) CreateStream(
	ctx context.Context,
	req *workflowservice.CreateStreamRequest,
) (*workflowservice.CreateStreamResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.CreateStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetFrontendResponse(), nil
}

// AddToStream pushes messages to the stream.
func (h *frontendHandler) AddToStream(
	ctx context.Context,
	req *workflowservice.AddToStreamRequest,
) (*workflowservice.AddToStreamResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.AddToStream(ctx, &streampb.AddToStreamRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetFrontendResponse(), nil
}

// PollStream long-polls for new messages on the stream.
func (h *frontendHandler) PollStream(
	ctx context.Context,
	req *workflowservice.PollStreamRequest,
) (*workflowservice.PollStreamResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.PollStream(ctx, &streampb.PollStreamRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetFrontendResponse(), nil
}
