package nexus

import (
	"context"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
)

const defaultDirectEndpointRegistryPageSize = 100

type DirectEndpointRegistry struct {
	pageSize    int
	persistence p.NexusEndpointManager
}

func NewDirectEndpointRegistry(
	pageSize int,
	persistence p.NexusEndpointManager,
) *DirectEndpointRegistry {
	if pageSize <= 0 {
		pageSize = defaultDirectEndpointRegistryPageSize
	}

	return &DirectEndpointRegistry{
		pageSize:    pageSize,
		persistence: persistence,
	}
}

func (r *DirectEndpointRegistry) GetByName(ctx context.Context, _ namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	var nextPageToken []byte

	for ctx.Err() == nil {
		resp, err := r.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			NextPageToken: nextPageToken,
			PageSize:      r.pageSize,
		})
		if err != nil {
			return nil, err
		}

		for _, entry := range resp.Entries {
			if entry.GetEndpoint().GetSpec().GetName() == endpointName {
				return entry, nil
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		nextPageToken = resp.NextPageToken
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return nil, serviceerror.NewNotFoundf("could not find Nexus endpoint by name: %v", endpointName)
}

func (r *DirectEndpointRegistry) GetByID(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
	return r.persistence.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{
		ID: endpointID,
	})
}

func (*DirectEndpointRegistry) StartLifecycle() {}

func (*DirectEndpointRegistry) StopLifecycle() {}
