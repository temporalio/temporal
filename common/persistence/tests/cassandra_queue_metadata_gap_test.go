package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

// TestCassandraDelayedWriterBeforeRetirementMetadataCharacterization demonstrates why checking metadata after
// insertion alone would not fence retirement: cleanup may have deleted the row without updating metadata yet.
func TestCassandraDelayedWriterBeforeRetirementMetadataCharacterization(t *testing.T) {
	cluster := persistencetests.NewTestClusterForCassandra(&persistencetests.TestBaseOptions{}, log.NewNoopLogger())
	cluster.SetupTestDatabase()
	t.Cleanup(cluster.TearDownTestDatabase)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	writerSession := &blockingSession{
		Session: cluster.GetSession(), queryToBlockOn: cassandra.TemplateEnqueueMessageQuery,
		queryStarted: make(chan struct{}, 1), queryCanContinue: make(chan struct{}),
	}
	cleanerSession := &blockingSession{
		Session: cluster.GetSession(), queryToBlockOn: cassandra.TemplateUpdateQueueMetadataQuery,
		queryStarted: make(chan struct{}, 1), queryCanContinue: make(chan struct{}),
	}
	resumeWriter := sync.OnceFunc(func() { close(writerSession.queryCanContinue) })
	defer resumeWriter()
	resumeCleaner := sync.OnceFunc(func() { close(cleanerSession.queryCanContinue) })
	defer resumeCleaner()
	delayed := newQueueV2Store(writerSession)
	cleaner := newQueueV2Store(cleanerSession)
	active := newQueueV2Store(cluster.GetSession())
	queueType := persistence.QueueTypeHistoryNormal
	queueName := t.Name()
	_, err := active.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{QueueType: queueType, QueueName: queueName})
	require.NoError(t, err)
	results := make(chan error, 1)
	go func() {
		_, err := delayed.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: queueType, QueueName: queueName,
			Blob: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("delayed")},
		})
		results <- err
	}()
	select {
	case <-writerSession.queryStarted:
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
	deletes := make(chan error, 1)
	go func() {
		_, err := cleaner.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType, QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
		})
		deletes <- err
	}()
	select {
	case <-cleanerSession.queryStarted:
	case <-ctx.Done():
		t.Fatal("cleaner did not reach metadata update")
	}
	resumeWriter()
	select {
	case err = <-results:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("delayed writer did not finish")
	}
	q, err := cassandra.GetQueue(ctx, cluster.GetSession(), queueName, queueType)
	require.NoError(t, err)
	require.Zero(t, q.Metadata.Partitions[0].MinMessageId)
	beforeRetirement, err := active.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{QueueType: queueType, QueueName: queueName, PageSize: 10})
	require.NoError(t, err)
	require.Len(t, beforeRetirement.Messages, 2)
	require.Equal(t, int64(persistence.FirstQueueMessageID), beforeRetirement.Messages[0].MetaData.ID)
	require.Equal(t, []byte("delayed"), beforeRetirement.Messages[0].Data.Data)
	t.Logf("delayed enqueue returned success after physical deletion; subsequent metadata read still permits ID 0, version=%d", q.Version)
	resumeCleaner()
	select {
	case err = <-deletes:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("cleaner did not finish")
	}
	read, err := active.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{QueueType: queueType, QueueName: queueName, PageSize: 10})
	require.NoError(t, err)
	t.Logf("after metadata commit, visible messages=%v", read.Messages)
	q, err = cassandra.GetQueue(ctx, cluster.GetSession(), queueName, queueType)
	require.NoError(t, err)
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), q.Metadata.Partitions[0].MinMessageId)
	require.Len(t, read.Messages, 1)
	require.Equal(t, int64(persistence.FirstQueueMessageID+1), read.Messages[0].MetaData.ID)
	require.Equal(t, []byte("active"), read.Messages[0].Data.Data)
}
