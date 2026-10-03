package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
)

// TestCassandraDelayedQueueWriterCharacterization documents the current publication gap, not the desired contract.
// A future fix should prevent an acknowledged insert from newly appearing behind the retired read boundary.
func TestCassandraDelayedQueueWriterCharacterization(t *testing.T) {
	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)

	t.Run("QueueV2", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		session := &blockingSession{
			Session:          cluster.GetSession(),
			queryToBlockOn:   cassandra.TemplateEnqueueMessageQuery,
			queryStarted:     make(chan struct{}, 1),
			queryCanContinue: make(chan struct{}),
		}
		resume := sync.OnceFunc(func() { close(session.queryCanContinue) })
		defer resume()
		delayed := newQueueV2Store(session)
		active := newQueueV2Store(cluster.GetSession())
		queueType := persistence.QueueTypeHistoryNormal
		queueName := t.Name()
		_, err := active.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{QueueType: queueType, QueueName: queueName})
		require.NoError(t, err)
		blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("delayed")}
		type result struct {
			response *persistence.InternalEnqueueMessageResponse
			err      error
		}
		results := make(chan result, 1)
		go func() {
			response, err := delayed.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{QueueType: queueType, QueueName: queueName, Blob: blob})
			results <- result{response, err}
		}()
		select {
		case <-session.queryStarted:
		case <-ctx.Done():
			t.Fatal("delayed writer did not reach insert")
		}
		for range 2 {
			_, err := active.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
				QueueType: queueType, QueueName: queueName,
				Blob: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("active")},
			})
			require.NoError(t, err)
		}
		consumed, err := active.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{QueueType: queueType, QueueName: queueName, PageSize: 10})
		require.NoError(t, err)
		require.Len(t, consumed.Messages, 2)
		_, err = active.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType, QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: persistence.FirstQueueMessageID},
		})
		require.NoError(t, err)
		q, err := cassandra.GetQueue(ctx, cluster.GetSession(), queueName, queueType)
		require.NoError(t, err)
		require.Equal(t, int64(persistence.FirstQueueMessageID+1), q.Metadata.Partitions[0].MinMessageId)
		resume()
		var res result
		select {
		case res = <-results:
		case <-ctx.Done():
			t.Fatal("delayed writer did not finish")
		}
		require.NoError(t, res.err)
		require.NotNil(t, res.response)
		require.Equal(t, int64(persistence.FirstQueueMessageID), res.response.Metadata.ID)
		var payload []byte
		err = cluster.GetSession().Query("SELECT message_payload FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id = ?",
			queueType, queueName, 0, res.response.Metadata.ID).WithContext(ctx).Scan(&payload)
		require.NoError(t, err)
		require.Equal(t, blob.Data, payload)
		read, err := active.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{QueueType: queueType, QueueName: queueName, PageSize: 10})
		require.NoError(t, err)
		t.Logf("delayed enqueue returned success: ID=%d, stored payload=%q, minimum readable ID=%d, visible messages=%v", res.response.Metadata.ID, payload, q.Metadata.Partitions[0].MinMessageId, read.Messages)
		require.Less(t, res.response.Metadata.ID, q.Metadata.Partitions[0].MinMessageId)
		require.Len(t, read.Messages, 1)
		require.Equal(t, int64(persistence.FirstQueueMessageID+1), read.Messages[0].MetaData.ID)
		require.Equal(t, []byte("active"), read.Messages[0].Data.Data)
	})

	t.Run("LegacyQueue", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		session := &blockingSession{
			Session:          cluster.GetSession(),
			queryToBlockOn:   "INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?) IF NOT EXISTS",
			queryStarted:     make(chan struct{}, 1),
			queryCanContinue: make(chan struct{}),
		}
		resume := sync.OnceFunc(func() { close(session.queryCanContinue) })
		defer resume()
		queueType := persistence.NamespaceReplicationQueueType
		delayed, err := cassandra.NewQueueStore(queueType, session, log.NewNoopLogger())
		require.NoError(t, err)
		active, err := cassandra.NewQueueStore(queueType, cluster.GetSession(), log.NewNoopLogger())
		require.NoError(t, err)
		serializer := serialization.NewSerializer()
		metadataBlob, err := serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{ClusterAckLevels: map[string]int64{}})
		require.NoError(t, err)
		require.NoError(t, active.Init(ctx, metadataBlob))
		blob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("delayed")}
		results := make(chan error, 1)
		go func() { results <- delayed.EnqueueMessage(ctx, blob) }()
		select {
		case <-session.queryStarted:
		case <-ctx.Done():
			t.Fatal("delayed writer did not reach insert")
		}
		for range 2 {
			require.NoError(t, active.EnqueueMessage(ctx, &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("active")}))
		}
		consumed, err := active.ReadMessages(ctx, persistence.EmptyQueueMessageID, 10)
		require.NoError(t, err)
		require.Len(t, consumed, 2)
		lastRead := consumed[1].ID
		metadata, err := active.GetAckLevels(ctx)
		require.NoError(t, err)
		metadata.Blob, err = serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{ClusterAckLevels: map[string]int64{"reader": lastRead}})
		require.NoError(t, err)
		require.NoError(t, active.UpdateAckLevel(ctx, metadata))
		require.NoError(t, active.DeleteMessagesBefore(ctx, lastRead))
		resume()
		select {
		case err = <-results:
		case <-ctx.Done():
			t.Fatal("delayed writer did not finish")
		}
		require.NoError(t, err)
		var payload []byte
		err = cluster.GetSession().Query("SELECT message_payload FROM queue WHERE queue_type = ? AND message_id = ?", queueType, persistence.FirstQueueMessageID).WithContext(ctx).Scan(&payload)
		require.NoError(t, err)
		require.Equal(t, blob.Data, payload)
		read, err := active.ReadMessages(ctx, lastRead, 10)
		require.NoError(t, err)
		t.Logf("delayed enqueue returned success: ID=%d, stored payload=%q, acknowledged/read cursor=%d, visible messages=%v", persistence.FirstQueueMessageID, payload, lastRead, read)
		require.Equal(t, int64(persistence.FirstQueueMessageID+1), lastRead)
		require.Empty(t, read, "the delayed payload exists physically, but is behind the acknowledged cursor")
	})
}
