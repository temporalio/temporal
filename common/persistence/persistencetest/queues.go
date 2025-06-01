package persistencetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	getQueueKeyParams struct {
		QueueType persistence.QueueV2Type
		Category  tasks.Category
	}
)

func WithQueueType(queueType persistence.QueueV2Type) func(p *getQueueKeyParams) {
	return func(p *getQueueKeyParams) {
		p.QueueType = queueType
	}
}

func WithCategory(category tasks.Category) func(p *getQueueKeyParams) {
	return func(p *getQueueKeyParams) {
		p.Category = category
	}
}

func GetQueueKey(t *testing.T, opts ...func(p *getQueueKeyParams)) persistence.QueueKey {
	params := &getQueueKeyParams{
		QueueType: persistence.QueueTypeHistoryNormal,
		Category:  tasks.CategoryTransfer,
	}
	for _, opt := range opts {
		opt(params)
	}
	// Note that it is important to include the test name in the cluster name to ensure that the generated queue name is
	// unique across tests. That way, we can run many queue tests without any risk of queue name collisions.
	return persistence.QueueKey{
		QueueType:     params.QueueType,
		Category:      params.Category,
		SourceCluster: "test-source-cluster-" + t.Name(),
		TargetCluster: "test-target-cluster-" + t.Name(),
	}
}

type EnqueueParams struct {
	Data         []byte
	EncodingType int
}

func EnqueueMessage(
	ctx context.Context,
	queue persistence.QueueV2,
	queueType persistence.QueueV2Type,
	queueName string,
	opts ...func(p *EnqueueParams),
) (*persistence.InternalEnqueueMessageResponse, error) {
	params := EnqueueParams{
		Data:         []byte("1"),
		EncodingType: int(enumspb.ENCODING_TYPE_JSON),
	}
	for _, opt := range opts {
		opt(&params)
	}
	return queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: &commonpb.DataBlob{
			EncodingType: enumspb.EncodingType(params.EncodingType),
			Data:         params.Data,
		},
	})
}

func EnqueueMessagesForDelete(t *testing.T, q persistence.QueueV2, queueName string, queueType persistence.QueueV2Type) {
	for i := 0; i < 2; i++ {
		// We have to actually enqueue 2 messages. Otherwise, there won't be anything to actually delete,
		// since we never delete the last message.
		_, err := EnqueueMessage(context.Background(), q, queueType, queueName)
		require.NoError(t, err)
	}
}
