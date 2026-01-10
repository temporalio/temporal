package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

type fakeNexusDatabase struct {
	endpoints *fakeCollection
	metadata  *fakeCollection
}

func newFakeNexusDatabase(t *testing.T) *fakeNexusDatabase {
	return &fakeNexusDatabase{
		endpoints: newFakeCollection(t),
		metadata:  newFakeCollection(t),
	}
}

func (d *fakeNexusDatabase) Collection(name string) client.Collection {
	switch name {
	case collectionNexusEndpoints:
		return d.endpoints
	case collectionNexusEndpointsVersion:
		return d.metadata
	default:
		panic("unexpected collection " + name)
	}
}

func (d *fakeNexusDatabase) RunCommand(context.Context, interface{}) client.SingleResult {
	return noopSingleResult{}
}

func TestNewNexusEndpointStoreRequiresTransactions(t *testing.T) {
	db := newFakeNexusDatabase(t)
	_, err := NewNexusEndpointStore(db, &fakeMongoClient{}, metrics.NoopMetricsHandler, log.NewTestLogger(), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transactions-enabled")
}

func TestNexusEndpointStoreCreateGetListDelete(t *testing.T) {
	db := newFakeNexusDatabase(t)
	store, err := NewNexusEndpointStore(db, &fakeMongoClient{}, metrics.NoopMetricsHandler, log.NewTestLogger(), true)
	require.NoError(t, err)

	ctx := context.Background()
	tableVersion := int64(0)

	// ensure metadata insert and increment succeed
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1})

	endpoint := persistence.InternalNexusEndpoint{
		ID:      "test-id",
		Version: 0,
		Data:    persistence.NewDataBlob([]byte("payload"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}
	err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: tableVersion,
		Endpoint:              endpoint,
	})
	require.NoError(t, err)
	tableVersion++

	require.Len(t, db.endpoints.insertDocs, 1)

	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueFind(&nexusEndpointMetadataDocument{Version: tableVersion})
	db.endpoints.enqueueFind(&nexusEndpointDocument{
		ID:           endpoint.ID,
		Version:      1,
		Data:         []byte("payload"),
		DataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		UpdatedAt:    time.Now().UTC(),
	})

	doc, err := store.GetNexusEndpoint(ctx, &persistence.GetNexusEndpointRequest{ID: endpoint.ID})
	require.NoError(t, err)
	require.Equal(t, int64(1), doc.Version)

	db.endpoints.enqueueFindMany(&nexusEndpointDocument{
		ID:           endpoint.ID,
		Version:      1,
		Data:         []byte("payload"),
		DataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
	})

	resp, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, resp.Endpoints, 1)
	require.Equal(t, tableVersion, resp.TableVersion)

	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1})
	db.endpoints.enqueueDeleteResult(1)

	err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
		ID:                    endpoint.ID,
		LastKnownTableVersion: tableVersion,
	})
	require.NoError(t, err)
}

func TestNexusEndpointStoreVersionConflicts(t *testing.T) {
	db := newFakeNexusDatabase(t)
	store, err := NewNexusEndpointStore(db, &fakeMongoClient{}, metrics.NoopMetricsHandler, log.NewTestLogger(), true)
	require.NoError(t, err)

	ctx := context.Background()

	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 1,
		Endpoint: persistence.InternalNexusEndpoint{
			ID:      "conflict",
			Version: 0,
			Data:    persistence.NewDataBlob([]byte("payload"), enumspb.ENCODING_TYPE_PROTO3.String()),
		},
	})
	require.ErrorIs(t, err, persistence.ErrNexusTableVersionConflict)

	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1})
	db.endpoints.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 0, ModifiedCount: 0})
	err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: 0,
		Endpoint: persistence.InternalNexusEndpoint{
			ID:      "missing",
			Version: 1,
			Data:    persistence.NewDataBlob([]byte("payload"), enumspb.ENCODING_TYPE_PROTO3.String()),
		},
	})
	require.ErrorIs(t, err, persistence.ErrNexusEndpointVersionConflict)

	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 0})
	db.metadata.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1})
	db.endpoints.enqueueDeleteResult(0)
	err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
		ID:                    "missing",
		LastKnownTableVersion: 0,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}
