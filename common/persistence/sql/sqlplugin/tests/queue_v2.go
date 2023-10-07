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

package tests

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	faultyDB struct {
		sqlplugin.DB
		getLastMessageIdErr error
		txBeginErr          error
		insertErr           error
		txRollbackErr       error
		rangeSelectError    error
		commitCalls         int
	}
	faultyTx struct {
		db *faultyDB
		sqlplugin.Tx
		commitCalls *int
	}
	logRecorder struct {
		log.Logger
		errMsgs []string
	}
)

func (db *faultyDB) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	if db.txBeginErr != nil {
		return nil, db.txBeginErr
	}
	tx, err := db.DB.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &faultyTx{db: db, commitCalls: &db.commitCalls, Tx: tx}, nil
}

func (tx *faultyTx) InsertIntoQueueV2Messages(ctx context.Context, row []sqlplugin.QueueV2MessageRow) (sql.Result, error) {
	if _, err := tx.Tx.InsertIntoQueueV2Messages(ctx, row); err != nil {
		return nil, err
	}
	return nil, tx.db.insertErr
}

func (tx *faultyTx) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	return 0, tx.db.getLastMessageIdErr

}

func (db *faultyDB) RangeSelectFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) ([]sqlplugin.QueueV2MessageRow, error) {
	return []sqlplugin.QueueV2MessageRow{}, db.rangeSelectError

}

func (tx *faultyTx) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}
	return tx.db.txRollbackErr
}

func (tx *faultyTx) Commit() error {
	*tx.commitCalls++
	return tx.Tx.Commit()
}

func (l *logRecorder) Error(msg string, _ ...tag.Tag) {
	l.errMsgs = append(l.errMsgs, msg)
}

func RunSQLQueueV2TestSuite(t *testing.T, baseDB sqlplugin.DB) {
	t.Run("QueueInsertFails", func(t *testing.T) {
		t.Parallel()
		testQueueInsertFails(t, baseDB)
	})
	t.Run("TxBeginFails", func(t *testing.T) {
		t.Parallel()
		testBeginTxFails(t, baseDB)
	})
	t.Run("FailedToGetLastMessageIDFromDB", func(t *testing.T) {
		t.Parallel()
		testGetLastMessageIDFails(t, baseDB)
	})
	t.Run("FailedToGetLastMessageIDFromDB", func(t *testing.T) {
		t.Parallel()
		rangeSelectFromQueueV2MessagesFails(t, baseDB)
	})
}

func testQueueInsertFails(t *testing.T, baseDB sqlplugin.DB) {
	db := &faultyDB{
		DB:            baseDB,
		insertErr:     errors.New("insert error"),
		txRollbackErr: errors.New("rollback error"),
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: persistence.QueueTypeHistoryNormal,
		QueueName: "test-queue-" + t.Name(),
		Blob: commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "transaction has already been committed or rolled back")
	require.Len(t, logger.errMsgs, 1)
	assert.Contains(t, logger.errMsgs[0], "rollback error")
	assert.Equal(t, db.commitCalls, 1)
}

func testBeginTxFails(t *testing.T, baseDB sqlplugin.DB) {
	db := &faultyDB{
		DB:         baseDB,
		txBeginErr: errors.New("EnqueueMessage failed. Failed to start transaction"),
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: persistence.QueueTypeHistoryNormal,
		QueueName: "test-queue-" + t.Name(),
		Blob: commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "EnqueueMessage failed. Failed to start transaction.")
	assert.Equal(t, db.commitCalls, 0)
}

func testGetLastMessageIDFails(t *testing.T, baseDB sqlplugin.DB) {
	db := &faultyDB{
		DB:                  baseDB,
		getLastMessageIdErr: errors.New("getLastMessageId error"),
		txRollbackErr:       errors.New("rollback error"),
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.EnqueueMessage(context.Background(), &persistence.InternalEnqueueMessageRequest{
		QueueType: persistence.QueueTypeHistoryNormal,
		QueueName: "test-queue-" + t.Name(),
		Blob: commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_JSON,
			Data:         []byte("1"),
		},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to get last enqueued message id")
	assert.Equal(t, db.commitCalls, 0)
}

func rangeSelectFromQueueV2MessagesFails(t *testing.T, baseDB sqlplugin.DB) {
	db := &faultyDB{
		DB:               baseDB,
		rangeSelectError: errors.New("rangeSelect error"),
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType:     persistence.QueueTypeHistoryNormal,
		QueueName:     "test-queue-" + t.Name(),
		PageSize:      1,
		NextPageToken: nil,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "RangeSelectFromQueueV2Messages operation failed")
}
