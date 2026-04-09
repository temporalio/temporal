package nexus

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
	"go.uber.org/mock/gomock"
)

func TestDirectEndpointRegistryGetByIDAlwaysReadsPersistence(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	persistenceManager := persistence.NewMockNexusEndpointManager(ctrl)
	registry := NewDirectEndpointRegistry(10, persistenceManager)
	entry := newEndpointEntry(t.Name())

	persistenceManager.EXPECT().GetNexusEndpoint(gomock.Any(), &persistence.GetNexusEndpointRequest{
		ID: entry.Id,
	}).Return(entry, nil).Times(2)

	first, err := registry.GetByID(context.Background(), entry.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, entry, first)

	second, err := registry.GetByID(context.Background(), entry.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, entry, second)
}

func TestDirectEndpointRegistryGetByNameScansPersistencePages(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	persistenceManager := persistence.NewMockNexusEndpointManager(ctrl)
	registry := NewDirectEndpointRegistry(1, persistenceManager)
	firstEntry := newEndpointEntry(t.Name() + "-first")
	targetEntry := newEndpointEntry(t.Name() + "-target")

	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken: nil,
		PageSize:      1,
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{firstEntry},
		NextPageToken: []byte(targetEntry.Id),
	}, nil)
	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken: []byte(targetEntry.Id),
		PageSize:      1,
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries: []*persistencespb.NexusEndpointEntry{targetEntry},
	}, nil)

	entry, err := registry.GetByName(context.Background(), "ignored", targetEntry.Endpoint.Spec.Name)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, targetEntry, entry)
}

func TestDirectEndpointRegistryGetByNameNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	persistenceManager := persistence.NewMockNexusEndpointManager(ctrl)
	registry := NewDirectEndpointRegistry(10, persistenceManager)
	name := uuid.NewString()

	persistenceManager.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken: nil,
		PageSize:      10,
	}).Return(&persistence.ListNexusEndpointsResponse{}, nil)

	entry, err := registry.GetByName(context.Background(), "ignored", name)
	require.Nil(t, entry)
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.EqualError(t, err, "could not find Nexus endpoint by name: "+name)
}
