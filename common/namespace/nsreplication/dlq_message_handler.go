//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination dlq_message_handler_mock.go

package nsreplication

import (
	"context"

	"go.temporal.io/api/serviceerror"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

type (
	// DLQMessageHandler is the interface handles namespace DLQ messages
	DLQMessageHandler interface {
		Read(ctx context.Context, lastMessageID int64, pageSize int, pageToken []byte) ([]*replicationspb.ReplicationTask, []byte, error)
		Purge(ctx context.Context, lastMessageID int64) error
		Merge(ctx context.Context, lastMessageID int64, pageSize int, pageToken []byte) ([]byte, error)
	}

	dlqMessageHandlerImpl struct {
		replicationHandler        TaskExecutor
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		logger                    log.Logger
	}
)

// NewDLQMessageHandler returns a DLQMessageHandler instance
func NewDLQMessageHandler(
	replicationHandler TaskExecutor,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	logger log.Logger,
) DLQMessageHandler {
	return &dlqMessageHandlerImpl{
		replicationHandler:        replicationHandler,
		namespaceReplicationQueue: namespaceReplicationQueue,
		logger:                    logger,
	}
}

// Read reads namespace replication DLQ messages
func (d *dlqMessageHandlerImpl) Read(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel(ctx)
	if err != nil {
		return nil, nil, err
	}

	return d.namespaceReplicationQueue.GetMessagesFromDLQ(
		ctx,
		ackLevel,
		lastMessageID,
		pageSize,
		pageToken,
	)
}

// Purge purges namespace replication DLQ messages
func (d *dlqMessageHandlerImpl) Purge(
	ctx context.Context,
	lastMessageID int64,
) error {
	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel(ctx)
	if err != nil {
		return err
	}

	if err := d.namespaceReplicationQueue.RangeDeleteMessagesFromDLQ(
		ctx,
		ackLevel,
		lastMessageID,
	); err != nil {
		return err
	}

	if err := d.namespaceReplicationQueue.UpdateDLQAckLevel(
		ctx,
		lastMessageID,
	); err != nil {
		d.logger.Error("Failed to update DLQ ack level after purging messages", tag.Error(err))
	}

	return nil
}

// Merge merges namespace replication DLQ messages
func (d *dlqMessageHandlerImpl) Merge(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]byte, error) {
	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel(ctx)
	if err != nil {
		return nil, err
	}

	messages, token, err := d.namespaceReplicationQueue.GetMessagesFromDLQ(
		ctx,
		ackLevel,
		lastMessageID,
		pageSize,
		pageToken,
	)
	if err != nil {
		return nil, err
	}

	var ackedMessageID int64
	for _, message := range messages {
		namespaceTask := message.GetNamespaceTaskAttributes()
		if namespaceTask == nil {
			return nil, serviceerror.NewInternal("Encounter non namespace replication task in namespace replication queue.")
		}

		if err := d.replicationHandler.Execute(
			ctx,
			namespaceTask,
		); err != nil {
			return nil, err
		}
		ackedMessageID = message.SourceTaskId
	}

	if err := d.namespaceReplicationQueue.RangeDeleteMessagesFromDLQ(
		ctx,
		ackLevel,
		ackedMessageID,
	); err != nil {
		d.logger.Error("failed to delete merged tasks on merging namespace DLQ message", tag.Error(err))
		return nil, err
	}
	if err := d.namespaceReplicationQueue.UpdateDLQAckLevel(ctx, ackedMessageID); err != nil {
		d.logger.Error("failed to update ack level on merging namespace DLQ message", tag.Error(err))
	}

	return token, nil
}
