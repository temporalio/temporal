package frontend

import (
	"context"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/server/common/log"
)

func (h *OperatorHandlerImpl) CreateNexusEndpoint(
	ctx context.Context,
	request *operatorservice.CreateNexusEndpointRequest,
) (_ *operatorservice.CreateNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Create(ctx, request)
}

func (h *OperatorHandlerImpl) UpdateNexusEndpoint(
	ctx context.Context,
	request *operatorservice.UpdateNexusEndpointRequest,
) (_ *operatorservice.UpdateNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Update(ctx, request)
}

func (h *OperatorHandlerImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *operatorservice.DeleteNexusEndpointRequest,
) (_ *operatorservice.DeleteNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Delete(ctx, request)
}

func (h *OperatorHandlerImpl) GetNexusEndpoint(
	ctx context.Context,
	request *operatorservice.GetNexusEndpointRequest,
) (_ *operatorservice.GetNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Get(ctx, request)
}

func (h *OperatorHandlerImpl) ListNexusEndpoints(
	ctx context.Context,
	request *operatorservice.ListNexusEndpointsRequest,
) (_ *operatorservice.ListNexusEndpointsResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.List(ctx, request)
}
