package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"google.golang.org/protobuf/proto"
)

type fakeQueueV2Database struct {
	collections map[string]*fakeCollection
}

func newFakeQueueV2Database(t *testing.T) *fakeQueueV2Database {
	return &fakeQueueV2Database{
		collections: map[string]*fakeCollection{
			collectionQueueV2Metadata: newFakeCollection(t),
			collectionQueueV2Messages: newFakeCollection(t),
		},
	}
}

func (d *fakeQueueV2Database) Collection(name string) client.Collection {
	if col, ok := d.collections[name]; ok {
		return col
	}
	panic("unexpected collection " + name)
}

func (d *fakeQueueV2Database) RunCommand(context.Context, interface{}) client.SingleResult {
	return noopSingleResult{}
}

func TestQueueV2CreateQueue(t *testing.T) {
	db := newFakeQueueV2Database(t)
	metaCol := db.collections[collectionQueueV2Metadata]

	store, err := NewQueueV2Store(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, true)
	require.NoError(t, err)

	queueType := persistence.QueueV2Type(1)

	_, err = store.CreateQueue(context.Background(), &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: "queueA",
	})
	require.NoError(t, err)
	require.Len(t, metaCol.insertDocs, 1)

	metaCol.enqueueInsertError(mongo.CommandError{Code: 11000})
	_, err = store.CreateQueue(context.Background(), &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: "queueA",
	})
	require.Error(t, err)
}

func TestQueueV2EnqueueAndRead(t *testing.T) {
	db := newFakeQueueV2Database(t)
	metaCol := db.collections[collectionQueueV2Metadata]
	msgCol := db.collections[collectionQueueV2Messages]

	store, err := NewQueueV2Store(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, true)
	require.NoError(t, err)

	queueType := persistence.QueueV2Type(1)
	queueName := "queueB"
	protoPayload := &persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			defaultQueueV2Partition: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	payload, _ := proto.Marshal(protoPayload)

	metaDoc := &queueV2MetadataDocument{
		ID:            queueV2MetadataDocID(queueType, queueName),
		QueueType:     int32(queueType),
		QueueName:     queueName,
		Payload:       payload,
		Encoding:      enumspb.ENCODING_TYPE_PROTO3.String(),
		LastMessageID: persistence.FirstQueueMessageID,
		UpdatedAt:     time.Now().UTC(),
	}

	// Sequence of metadata reads: Enqueue load, allocate reload, Read load
	metaCol.enqueueFind(metaDoc)
	metaCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	metaAfterInc := *metaDoc
	metaAfterInc.LastMessageID = persistence.FirstQueueMessageID + 1
	metaCol.enqueueFind(&metaAfterInc)
	metaCol.enqueueFind(&metaAfterInc)

	_, err = store.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob:      persistence.NewDataBlob([]byte("msg"), enumspb.ENCODING_TYPE_PROTO3.String()),
	})
	require.NoError(t, err)
	require.Len(t, msgCol.insertDocs, 1)

	msgCol.enqueueFindMany(&queueV2MessageDocument{
		ID:        queueV2MessageDocID(queueType, queueName, defaultQueueV2Partition, persistence.FirstQueueMessageID+1),
		QueueType: int32(queueType),
		QueueName: queueName,
		Partition: defaultQueueV2Partition,
		MessageID: persistence.FirstQueueMessageID + 1,
		Payload:   []byte("msg"),
		Encoding:  enumspb.ENCODING_TYPE_PROTO3.String(),
	})

	resp, err := store.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  1,
	})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	require.NotNil(t, resp.Messages[0].Data)
}

func TestQueueV2ListQueues(t *testing.T) {
	db := newFakeQueueV2Database(t)
	metaCol := db.collections[collectionQueueV2Metadata]

	store, err := NewQueueV2Store(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), metrics.NoopMetricsHandler, true)
	require.NoError(t, err)

	protoPayload := &persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			defaultQueueV2Partition: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	payload, _ := proto.Marshal(protoPayload)

	listQueueType := persistence.QueueV2Type(2)
	metaDoc := &queueV2MetadataDocument{
		ID:            queueV2MetadataDocID(listQueueType, "queueC"),
		QueueType:     int32(listQueueType),
		QueueName:     "queueC",
		Payload:       payload,
		Encoding:      enumspb.ENCODING_TYPE_PROTO3.String(),
		LastMessageID: persistence.FirstQueueMessageID,
		UpdatedAt:     time.Now().UTC(),
	}
	metaCol.enqueueFindMany(metaDoc)

	resp, err := store.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: listQueueType,
		PageSize:  10,
	})
	require.NoError(t, err)
	require.Len(t, resp.Queues, 1)
	require.Equal(t, int64(1), resp.Queues[0].MessageCount)
}
