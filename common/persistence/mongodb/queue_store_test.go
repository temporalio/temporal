package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

type fakeQueueDatabase struct {
	collections map[string]*fakeCollection
}

func newFakeQueueDatabase(t *testing.T) *fakeQueueDatabase {
	return &fakeQueueDatabase{
		collections: map[string]*fakeCollection{
			collectionQueueMessages: newFakeCollection(t),
			collectionQueueMetadata: newFakeCollection(t),
		},
	}
}

func (d *fakeQueueDatabase) Collection(name string) client.Collection {
	if col, ok := d.collections[name]; ok {
		return col
	}
	panic("unexpected collection " + name)
}

func (d *fakeQueueDatabase) RunCommand(context.Context, interface{}) client.SingleResult {
	return noopSingleResult{}
}

func TestNewQueueStoreRequiresTransactions(t *testing.T) {
	db := newFakeQueueDatabase(t)
	_, err := NewQueueStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, false, persistence.NamespaceReplicationQueueType)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires transactions-enabled")
}

func TestQueueStoreEnqueueMessage(t *testing.T) {
	db := newFakeQueueDatabase(t)
	metaCol := db.collections[collectionQueueMetadata]
	msgCol := db.collections[collectionQueueMessages]

	store, err := NewQueueStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, true, persistence.NamespaceReplicationQueueType)
	require.NoError(t, err)

	blob := persistence.NewDataBlob([]byte("initial"), enumspb.ENCODING_TYPE_PROTO3.String())
	err = store.Init(context.Background(), blob)
	require.NoError(t, err)

	metaCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	metaCol.enqueueFind(&queueMetadataDocument{LastMessageID: 1})

	err = store.EnqueueMessage(context.Background(), persistence.NewDataBlob([]byte("msg1"), enumspb.ENCODING_TYPE_PROTO3.String()))
	require.NoError(t, err)

	require.Len(t, msgCol.insertDocs, 1)
	inserted, ok := msgCol.insertDocs[0].(queueMessageDocument)
	require.True(t, ok)
	require.Equal(t, int32(persistence.NamespaceReplicationQueueType), inserted.QueueType)
	require.Equal(t, int64(1), inserted.MessageID)
}

func TestQueueStoreDLQPagination(t *testing.T) {
	db := newFakeQueueDatabase(t)
	metaCol := db.collections[collectionQueueMetadata]
	msgCol := db.collections[collectionQueueMessages]

	store, err := NewQueueStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, true, persistence.NamespaceReplicationQueueType)
	require.NoError(t, err)

	blob := persistence.NewDataBlob([]byte("meta"), enumspb.ENCODING_TYPE_PROTO3.String())
	require.NoError(t, store.Init(context.Background(), blob))

	// prepare metadata increments for DLQ
	metaCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	metaCol.enqueueFind(&queueMetadataDocument{LastMessageID: 5})

	payload := persistence.NewDataBlob([]byte("dlq"), enumspb.ENCODING_TYPE_PROTO3.String())
	id, err := store.EnqueueMessageToDLQ(context.Background(), payload)
	require.NoError(t, err)
	require.Equal(t, int64(5), id)
	require.Len(t, msgCol.insertDocs, 1)

	dlqType := -persistence.NamespaceReplicationQueueType
	msgCol.enqueueFindMany(&queueMessageDocument{
		ID:        queueMessageDocID(dlqType, id),
		QueueType: int32(dlqType),
		MessageID: id,
		Payload:   []byte("dlq"),
		Encoding:  enumspb.ENCODING_TYPE_PROTO3.String(),
	})

	first, next, err := store.ReadMessagesFromDLQ(context.Background(), persistence.EmptyQueueMessageID, persistence.MaxQueueMessageID, 1, nil)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NotNil(t, next)

	// consume next page
	entries, next2, err := store.ReadMessagesFromDLQ(context.Background(), persistence.EmptyQueueMessageID, persistence.MaxQueueMessageID, 1, next)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.Nil(t, next2)
}
