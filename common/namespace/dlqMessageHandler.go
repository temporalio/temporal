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
	"go.temporal.io/temporal-proto/serviceerror"

	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	// DLQMessageHandler is the interface handles namespace DLQ messages
	DLQMessageHandler interface {
		Read(lastMessageID int, pageSize int, pageToken []byte) ([]*replicationgenpb.ReplicationTask, []byte, error)
		Purge(lastMessageID int) error
		Merge(lastMessageID int, pageSize int, pageToken []byte) ([]byte, error)
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
	lastMessageID int,
	pageSize int,
	pageToken []byte,
) ([]*replicationgenpb.ReplicationTask, []byte, error) {

	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel()
	if err != nil {
		return nil, nil, err
	}

	return d.namespaceReplicationQueue.GetMessagesFromDLQ(
		ackLevel,
		lastMessageID,
		pageSize,
		pageToken,
	)
}

// PurgeMessages purges namespace replication DLQ messages
func (d *dlqMessageHandlerImpl) Purge(
	lastMessageID int,
) error {

	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel()
	if err != nil {
		return err
	}

	if err := d.namespaceReplicationQueue.RangeDeleteMessagesFromDLQ(
		ackLevel,
		lastMessageID,
	); err != nil {
		return err
	}

	if err := d.namespaceReplicationQueue.UpdateDLQAckLevel(
		lastMessageID,
	); err != nil {
		d.logger.Error("Failed to update DLQ ack level after purging messages", tag.Error(err))
	}

	return nil
}

// MergeMessages merges namespace replication DLQ messages
func (d *dlqMessageHandlerImpl) Merge(
	lastMessageID int,
	pageSize int,
	pageToken []byte,
) ([]byte, error) {

	ackLevel, err := d.namespaceReplicationQueue.GetDLQAckLevel()
	if err != nil {
		return nil, err
	}

	messages, token, err := d.namespaceReplicationQueue.GetMessagesFromDLQ(
		ackLevel,
		lastMessageID,
		pageSize,
		pageToken,
	)
	if err != nil {
		return nil, err
	}

	var ackedMessageID int
	for _, message := range messages {
		namespaceTask := message.GetNamespaceTaskAttributes()
		if namespaceTask == nil {
			return nil, serviceerror.NewInternal("Encounter non namespace replication task in namespace replication queue.")
		}

		if err := d.replicationHandler.Execute(
			namespaceTask,
		); err != nil {
			return nil, err
		}
		ackedMessageID = int(message.SourceTaskId)
	}

	if err := d.namespaceReplicationQueue.RangeDeleteMessagesFromDLQ(
		ackLevel,
		ackedMessageID,
	); err != nil {
		d.logger.Error("failed to delete merged tasks on merging namespace DLQ message", tag.Error(err))
		return nil, err
	}
	if err := d.namespaceReplicationQueue.UpdateDLQAckLevel(ackedMessageID); err != nil {
		d.logger.Error("failed to update ack level on merging namespace DLQ message", tag.Error(err))
	}

	return token, nil
}
