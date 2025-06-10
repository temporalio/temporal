package persistence

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
)

var (
	ErrNexusTableVersionConflict          = serviceerror.NewFailedPrecondition("nexus endpoints table version mismatch")
	ErrNexusEndpointVersionConflict       = serviceerror.NewFailedPrecondition("nexus endpoint version mismatch")
	ErrNegativeListNexusEndpointsPageSize = &InvalidPersistenceRequestError{
		Msg: "received negative page size for listing Nexus endpoints",
	}
)

type (
	nexusEndpointManagerImpl struct {
		persistence NexusEndpointStore
		serializer  serialization.Serializer
		logger      log.Logger
	}
)

func NewNexusEndpointManager(
	persistence NexusEndpointStore,
	serializer serialization.Serializer,
	logger log.Logger,
) NexusEndpointManager {
	return &nexusEndpointManagerImpl{
		persistence: persistence,
		serializer:  serializer,
		logger:      logger,
	}
}

func (m *nexusEndpointManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusEndpointManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusEndpointManagerImpl) GetNexusEndpoint(
	ctx context.Context,
	request *GetNexusEndpointRequest,
) (*persistencespb.NexusEndpointEntry, error) {
	internalEndpoint, err := m.persistence.GetNexusEndpoint(ctx, request)
	if err != nil {
		return nil, err
	}

	endpoint, err := m.serializer.NexusEndpointFromBlob(internalEndpoint.Data)
	if err != nil {
		m.logger.Error(fmt.Sprintf("error deserializing nexus endpoint with ID:%v", internalEndpoint.ID), tag.Error(err))
		return nil, err
	}

	return &persistencespb.NexusEndpointEntry{
		Id:       internalEndpoint.ID,
		Version:  internalEndpoint.Version,
		Endpoint: endpoint,
	}, nil
}

func (m *nexusEndpointManagerImpl) ListNexusEndpoints(
	ctx context.Context,
	request *ListNexusEndpointsRequest,
) (*ListNexusEndpointsResponse, error) {
	if request.PageSize < 0 {
		return nil, ErrNegativeListNexusEndpointsPageSize
	}

	result := &ListNexusEndpointsResponse{}

	resp, err := m.persistence.ListNexusEndpoints(ctx, request)
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, err
	}

	entries := make([]*persistencespb.NexusEndpointEntry, len(resp.Endpoints))
	for i, entry := range resp.Endpoints {
		endpoint, err := m.serializer.NexusEndpointFromBlob(entry.Data)
		if err != nil {
			m.logger.Error(fmt.Sprintf("error deserializing nexus endpoint with ID: %v", entry.ID), tag.Error(err))
			return nil, err
		}
		entries[i] = &persistencespb.NexusEndpointEntry{
			Id:       entry.ID,
			Version:  entry.Version,
			Endpoint: endpoint,
		}
	}

	result.NextPageToken = resp.NextPageToken
	result.Entries = entries
	return result, nil
}

func (m *nexusEndpointManagerImpl) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *CreateOrUpdateNexusEndpointRequest,
) (*CreateOrUpdateNexusEndpointResponse, error) {
	blob, err := m.serializer.NexusEndpointToBlob(request.Entry.Endpoint)
	if err != nil {
		return nil, err
	}

	err = m.persistence.CreateOrUpdateNexusEndpoint(ctx, &InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Endpoint: InternalNexusEndpoint{
			ID:      request.Entry.Id,
			Version: request.Entry.Version,
			Data:    blob,
		},
	})
	if err != nil {
		return nil, err
	}

	return &CreateOrUpdateNexusEndpointResponse{Version: request.Entry.Version + 1}, nil
}

func (m *nexusEndpointManagerImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *DeleteNexusEndpointRequest,
) error {
	return m.persistence.DeleteNexusEndpoint(ctx, request)
}
