// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dlqMessageHandler_mock.go

package namespace

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
		replicationHandler        ReplicationTaskExecutor
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		logger                    log.Logger
	}
)

// NewDLQMessageHandler returns a DLQTaskHandler instance
func NewDLQMessageHandler(
	replicationHandler ReplicationTaskExecutor,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	logger log.Logger,
) DLQMessageHandler {
	return &dlqMessageHandlerImpl{
		replicationHandler:        replicationHandler,
		namespaceReplicationQueue: namespaceReplicationQueue,
		logger:                    logger,
	}
}

// ReadMessages reads namespace replication DLQ messages
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

// PurgeMessages purges namespace replication DLQ messages
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

// MergeMessages merges namespace replication DLQ messages
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
