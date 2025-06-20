package nexustest

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

type FakeEndpointRegistry struct {
	OnGetByID   func(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error)
	OnGetByName func(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error)
}

func (f FakeEndpointRegistry) GetByID(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error) {
	return f.OnGetByID(ctx, endpointID)
}

func (f FakeEndpointRegistry) GetByName(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	return f.OnGetByName(ctx, namespaceID, endpointName)
}

func (f FakeEndpointRegistry) StartLifecycle() {
	panic("unimplemented")
}

func (f FakeEndpointRegistry) StopLifecycle() {
	panic("unimplemented")
}

var _ commonnexus.EndpointRegistry = FakeEndpointRegistry{}
