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

package sqlplugin

import (
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// clusterMetadata is the SQL persistence interface for cluster metadata
	clusterMetadata interface {
		SaveClusterMetadata(row *ClusterMetadataRow) (sql.Result, error)
		GetClusterMetadata() (*ClusterMetadataRow, error)
		WriteLockGetClusterMetadata() (*ClusterMetadataRow, error)
		GetClusterMembers(filter *ClusterMembershipFilter) ([]ClusterMembershipRow, error)
		UpsertClusterMembership(row *ClusterMembershipRow) (sql.Result, error)
		PruneClusterMembership(filter *PruneClusterMembershipFilter) (sql.Result, error)
	}

	// Namespace is the SQL persistence interface for namespaces
	Namespace interface {
		InsertIntoNamespace(rows *NamespaceRow) (sql.Result, error)
		UpdateNamespace(row *NamespaceRow) (sql.Result, error)
		// SelectFromNamespace returns namespaces that match filter criteria. Either ID or
		// Name can be specified to filter results. If both are not specified, all rows
		// will be returned
		SelectFromNamespace(filter *NamespaceFilter) ([]NamespaceRow, error)
		// DeleteNamespace deletes a single row. One of ID or Name MUST be specified
		DeleteFromNamespace(filter *NamespaceFilter) (sql.Result, error)

		LockNamespaceMetadata() (*NamespaceMetadataRow, error)
		UpdateNamespaceMetadata(row *NamespaceMetadataRow) (sql.Result, error)
		SelectFromNamespaceMetadata() (*NamespaceMetadataRow, error)
	}

	queue interface {
		InsertIntoQueue(row *QueueRow) (sql.Result, error)
		GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int64, error)
		GetMessagesFromQueue(queueType persistence.QueueType, lastMessageID int64, maxRows int) ([]QueueRow, error)
		GetMessagesBetween(queueType persistence.QueueType, firstMessageID int64, lastMessageID int64, maxRows int) ([]QueueRow, error)
		DeleteMessagesBefore(queueType persistence.QueueType, messageID int64) (sql.Result, error)
		RangeDeleteMessages(queueType persistence.QueueType, exclusiveBeginMessageID int64, inclusiveEndMessageID int64) (sql.Result, error)
		DeleteMessage(queueType persistence.QueueType, messageID int64) (sql.Result, error)
		InsertAckLevel(queueType persistence.QueueType, messageID int64, clusterName string) error
		UpdateAckLevels(queueType persistence.QueueType, clusterAckLevels map[string]int64) error
		GetAckLevels(queueType persistence.QueueType, forUpdate bool) (map[string]int64, error)
	}
)
