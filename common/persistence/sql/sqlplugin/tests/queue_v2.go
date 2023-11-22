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

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/persistence/serialization"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var (
	ErrGetLastMessageIdFailed = errors.New("getLastMessageId error")
	ErrTxBeginFailed          = errors.New("txBegin error")
	ErrInsertFailed           = errors.New("insert error")
	ErrTxRollbackFailed       = errors.New("txRollBack err")
	ErrTxCommitFailed         = errors.New("txCommit err")
	ErrRangeSelectFailed      = errors.New("rangeSelect err")
	ErrSelectMetadataFailed   = errors.New("selectFromMetadata err")
	ErrInsertMetadataFailed   = errors.New("insertMetadataFailed")
	ErrRangeDeleteFailed      = errors.New("rangeDeleteFailed")
	ErrUpdateMetadataFailed   = errors.New("updateMetadataFailed")
	ErrSelectQueueNames       = errors.New("selectQueueNamesFailed")
)

type (
	faultyDB struct {
		sqlplugin.DB
		getLastMessageIdErr   error
		txBeginErr            error
		txCommitErr           error
		insertErr             error
		txRollbackErr         error
		rangeSelectError      error
		selectMetadataError   error
		insertMetadataError   error
		rangeDeleteError      error
		updateMetadataError   error
		selectQueueNamesError error
		commitCalls           int
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
	if tx.db.insertErr != nil {
		return nil, tx.db.insertErr
	}
	return tx.Tx.InsertIntoQueueV2Messages(ctx, row)
}

func (tx *faultyTx) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	if tx.db.getLastMessageIdErr != nil {
		return 0, tx.db.getLastMessageIdErr
	}
	return tx.Tx.GetLastEnqueuedMessageIDForUpdateV2(ctx, filter)
}

func (db *faultyDB) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	if db.getLastMessageIdErr != nil {
		return 0, db.getLastMessageIdErr
	}
	return db.DB.GetLastEnqueuedMessageIDForUpdateV2(ctx, filter)
}

func (db *faultyDB) RangeSelectFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) ([]sqlplugin.QueueV2MessageRow, error) {
	return []sqlplugin.QueueV2MessageRow{}, db.rangeSelectError

}

func (db *faultyDB) SelectFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	if db.selectMetadataError != nil {
		return &sqlplugin.QueueV2MetadataRow{}, db.selectMetadataError
	}
	return db.DB.SelectFromQueueV2Metadata(ctx, filter)
}

func (db *faultyDB) SelectNameFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataTypeFilter) ([]sqlplugin.QueueV2MetadataRow, error) {
	if db.selectQueueNamesError != nil {
		return nil, db.selectQueueNamesError
	}
	return db.DB.SelectNameFromQueueV2Metadata(ctx, filter)
}

func (db *faultyDB) SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	if db.selectMetadataError != nil {
		return &sqlplugin.QueueV2MetadataRow{}, db.selectMetadataError
	}
	return db.DB.SelectFromQueueV2MetadataForUpdate(ctx, filter)
}

func (tx *faultyTx) SelectFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	if tx.db.selectMetadataError != nil {
		return &sqlplugin.QueueV2MetadataRow{}, tx.db.selectMetadataError
	}
	return tx.Tx.SelectFromQueueV2Metadata(ctx, filter)
}

func (tx *faultyTx) SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	if tx.db.selectMetadataError != nil {
		return &sqlplugin.QueueV2MetadataRow{}, tx.db.selectMetadataError
	}
	return tx.Tx.SelectFromQueueV2MetadataForUpdate(ctx, filter)
}

func (db *faultyDB) InsertIntoQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	if db.insertMetadataError != nil {
		return nil, db.insertMetadataError
	}
	return db.DB.InsertIntoQueueV2Metadata(ctx, row)
}

func (tx *faultyTx) RangeDeleteFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	if tx.db.rangeDeleteError != nil {
		return nil, tx.db.rangeDeleteError
	}
	return tx.Tx.RangeDeleteFromQueueV2Messages(ctx, filter)
}

func (tx *faultyTx) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	if tx.db.updateMetadataError != nil {
		return nil, tx.db.updateMetadataError
	}
	return tx.Tx.UpdateQueueV2Metadata(ctx, row)
}

func (tx *faultyTx) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}
	return tx.db.txRollbackErr
}

func (tx *faultyTx) Commit() error {
	*tx.commitCalls++
	if tx.db.txCommitErr != nil {
		err := tx.Rollback()
		if err != nil {
			return err
		}
		return tx.db.txCommitErr
	}
	return tx.Tx.Commit()
}

func (l *logRecorder) Error(msg string, _ ...tag.Tag) {
	l.errMsgs = append(l.errMsgs, msg)
}

func RunSQLQueueV2TestSuite(t *testing.T, baseDB sqlplugin.DB) {
	ctx := context.Background()
	t.Run("TestListQueueFailsToGetLastMessageID", func(t *testing.T) {
		t.Parallel()
		testListQueueFailsToGetLastMessageID(ctx, t, baseDB)
	})
	t.Run("TestListQueueFailsToExtractQueueMetadata", func(t *testing.T) {
		t.Parallel()
		testListQueueFailsToExtractQueueMetadata(ctx, t, baseDB)
	})
	t.Run("GetPartitionFailsForListQueues", func(t *testing.T) {
		t.Parallel()
		testListQueuesGetPartitionFails(ctx, t, baseDB)
	})
	t.Run("QueueInsertFails", func(t *testing.T) {
		t.Parallel()
		testQueueInsertFails(ctx, t, baseDB)
	})
	t.Run("TxBeginFails", func(t *testing.T) {
		t.Parallel()
		testBeginTxFails(ctx, t, baseDB)
	})
	t.Run("TxCommitFails", func(t *testing.T) {
		t.Parallel()
		testCommitTxFails(ctx, t, baseDB)
	})
	t.Run("FailedToGetLastMessageIDFromDB", func(t *testing.T) {
		t.Parallel()
		testGetLastMessageIDFails(ctx, t, baseDB)
	})
	t.Run("FailedToGetLastMessageIDFromDB", func(t *testing.T) {
		t.Parallel()
		testRangeSelectFromQueueV2MessagesFails(ctx, t, baseDB)
	})
	t.Run("InsertIntoQueueV2MetadataFails", func(t *testing.T) {
		t.Parallel()
		testInsertIntoQueueV2MetadataFails(ctx, t, baseDB)
	})
	t.Run("GetPartitionFailsForRangeDelete", func(t *testing.T) {
		t.Parallel()
		testGetPartitionFailsForRangeDelete(ctx, t, baseDB)
	})
	t.Run("GetLastMessageIDForDeleteFails", func(t *testing.T) {
		t.Parallel()
		testGetLastMessageIDForDeleteFails(ctx, t, baseDB)
	})
	t.Run("RangeDeleteMessagesFails", func(t *testing.T) {
		t.Parallel()
		testRangeDeleteMessagesFails(ctx, t, baseDB)
	})
	t.Run("RangeDeleteActuallyDeletes", func(t *testing.T) {
		t.Parallel()
		testRangeDeleteActuallyDeletes(ctx, t, baseDB)
	})
	t.Run("UpdateMetadataFails", func(t *testing.T) {
		t.Parallel()
		testUpdateMetadataFails(ctx, t, baseDB)
	})
	t.Run("InvalidMetadataEncoding", func(t *testing.T) {
		t.Parallel()
		testInvalidMetadataEncoding(ctx, t, baseDB)
	})
	t.Run("InvalidMetadataPayload", func(t *testing.T) {
		t.Parallel()
		testInvalidMetadataPayload(ctx, t, baseDB)
	})
	t.Run("SelectMetadataFails", func(t *testing.T) {
		t.Parallel()
		testSelectMetadataFails(ctx, t, baseDB)
	})
	t.Run("SelectNameFromQueueV2MetadataFails", func(t *testing.T) {
		t.Parallel()
		testSelectNameFromQueueV2MetadataFails(ctx, t, baseDB)
	})
	t.Run("SelectNameFromQueueV2NegativeToken", func(t *testing.T) {
		t.Parallel()
		testSelectNameFromQueueV2NegativeToken(ctx, t, baseDB)
	})
}

func testQueueInsertFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:            baseDB,
		insertErr:     ErrInsertFailed,
		txRollbackErr: ErrTxRollbackFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorContains(t, err, "insert error")
	require.Len(t, logger.errMsgs, 1)
	assert.Contains(t, logger.errMsgs[0], "transaction rollback error")
	assert.Equal(t, db.commitCalls, 0)
}

func testCommitTxFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:          baseDB,
		txCommitErr: ErrTxCommitFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorContains(t, err, "EnqueueMessage failed")
	assert.Equal(t, db.commitCalls, 1)
}

func testBeginTxFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:         baseDB,
		txBeginErr: ErrTxBeginFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorContains(t, err, "txBegin error")
	assert.Equal(t, db.commitCalls, 0)
}

func testGetLastMessageIDFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		getLastMessageIdErr: ErrGetLastMessageIdFailed,
		txRollbackErr:       ErrTxRollbackFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to get next messageId")
	assert.Equal(t, db.commitCalls, 0)
}

func testRangeSelectFromQueueV2MessagesFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:               baseDB,
		rangeSelectError: ErrRangeSelectFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType:     queueType,
		QueueName:     queueName,
		PageSize:      1,
		NextPageToken: nil,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "RangeSelectFromQueueV2Messages operation failed")
}

func testInsertIntoQueueV2MetadataFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		insertMetadataError: ErrInsertMetadataFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "InsertIntoQueueV2Metadata operation failed")
}

func testGetPartitionFailsForRangeDelete(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(baseDB, logger)
	queuePB := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {},
			1: {},
		},
	}
	bytes, _ := queuePB.Marshal()
	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        queueType,
		QueueName:        queueName,
		MetadataPayload:  bytes,
		MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
	}
	_, err := baseDB.InsertIntoQueueV2Metadata(ctx, &row)
	require.NoError(t, err)
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   persistence.QueueTypeHistoryNormal,
		QueueName:                   "test-queue-" + t.Name(),
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testGetLastMessageIDForDeleteFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		getLastMessageIdErr: ErrGetLastMessageIdFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   persistence.QueueTypeHistoryNormal,
		QueueName:                   "test-queue-" + t.Name(),
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "getLastMessageId error")
}

func testRangeDeleteMessagesFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:               baseDB,
		rangeDeleteError: ErrRangeDeleteFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   queueType,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "rangeDeleteFailed")
}

func testUpdateMetadataFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		updateMetadataError: ErrUpdateMetadataFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	require.NoError(t, err)
	persistencetest.EnqueueMessagesForDelete(t, q, queueName, queueType)
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   queueType,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "updateMetadataFailed")
}

func testSelectMetadataFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		selectMetadataError: ErrSelectMetadataFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  10,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, ErrSelectMetadataFailed.Error())
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	assert.Error(t, err)
	assert.ErrorContains(t, err, ErrSelectMetadataFailed.Error())
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   queueType,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
}

func testInvalidMetadataPayload(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(baseDB, logger)

	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        queueType,
		QueueName:        queueName,
		MetadataPayload:  []byte("invalid_payload"),
		MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
	}
	_, err := baseDB.InsertIntoQueueV2Metadata(ctx, &row)
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  10,
	})
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.DeserializationError))
}

func testInvalidMetadataEncoding(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryNormal
	queueName := "test-queue-" + t.Name()
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(baseDB, logger)

	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        queueType,
		QueueName:        queueName,
		MetadataPayload:  []byte("test"),
		MetadataEncoding: "invalid_encoding",
	}
	_, err := baseDB.InsertIntoQueueV2Metadata(ctx, &row)
	require.NoError(t, err)
	_, err = q.ReadMessages(context.Background(), &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		PageSize:  10,
	})
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueName)
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
	_, err = q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   queueType,
		QueueName:                   queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: 0},
	})
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serviceerror.Unavailable))
}

func testRangeDeleteActuallyDeletes(ctx context.Context, t *testing.T, db sqlplugin.DB) {
	queueKey := persistencetest.GetQueueKey(t)
	queueType := persistence.QueueTypeHistoryNormal
	q := persistencesql.NewQueueV2(db, log.NewTestLogger())
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueKey.GetQueueName(),
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = persistencetest.EnqueueMessage(context.Background(), q, queueType, queueKey.GetQueueName())
		require.NoError(t, err)
	}
	resp, err := q.RangeDeleteMessages(context.Background(), &persistence.InternalRangeDeleteMessagesRequest{
		QueueType:                   queueType,
		QueueName:                   queueKey.GetQueueName(),
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: persistence.FirstQueueMessageID + 2},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.MessagesDeleted)
	result, err := q.ReadMessages(ctx, &persistence.InternalReadMessagesRequest{
		QueueType: queueType,
		QueueName: queueKey.GetQueueName(),
		PageSize:  10,
	})
	require.NoError(t, err)
	assert.Empty(t, result.Messages)
	messages, err := db.RangeSelectFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesFilter{
		QueueType:    queueType,
		QueueName:    queueKey.GetQueueName(),
		MinMessageID: 0,
		MaxMessageID: 100,
		PageSize:     10,
	})
	require.NoError(t, err)
	if assert.Len(t, messages, 1) {
		assert.Equal(t, int64(persistence.FirstQueueMessageID+2), messages[0].MessageID)
	}
	response, err := persistencetest.EnqueueMessage(context.Background(), q, queueType, queueKey.GetQueueName())
	require.NoError(t, err)
	assert.Equal(t, int64(persistence.FirstQueueMessageID+3), response.Metadata.ID)
}

func testSelectNameFromQueueV2MetadataFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryDLQ
	db := &faultyDB{
		DB:                    baseDB,
		selectQueueNamesError: ErrSelectQueueNames,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType:     queueType,
		PageSize:      10,
		NextPageToken: nil,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "SelectNameFromQueueV2Metadata operation failed")
}

func testSelectNameFromQueueV2NegativeToken(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	queueType := persistence.QueueTypeHistoryDLQ
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(baseDB, logger)
	_, err := q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType:     queueType,
		PageSize:      1,
		NextPageToken: persistence.GetNextPageTokenForListQueues(-1),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, persistence.ErrNegativeListQueuesOffset)
}

func testListQueuesGetPartitionFails(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	// Using a different QueueType to prevent this test from failing because of queues created in previous tests.
	queueType := persistence.QueueV2Type(4)
	queueName := "test-queue-" + t.Name()
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(baseDB, logger)
	queuePB := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {},
			1: {},
		},
	}
	bytes, _ := queuePB.Marshal()
	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        queueType,
		QueueName:        queueName,
		MetadataPayload:  bytes,
		MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
	}
	_, err := baseDB.InsertIntoQueueV2Metadata(ctx, &row)
	require.NoError(t, err)
	_, err = q.ListQueues(context.Background(), &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "partitions")
}

func testListQueueFailsToGetLastMessageID(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	// Using a different QueueType to prevent this test from failing because of queues created in previous tests.
	queueType := persistence.QueueV2Type(5)
	queueName := "test-queue-" + t.Name()
	db := &faultyDB{
		DB:                  baseDB,
		getLastMessageIdErr: ErrGetLastMessageIdFailed,
	}
	logger := &logRecorder{Logger: log.NewTestLogger()}
	q := persistencesql.NewQueueV2(db, logger)
	_, err := q.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueName,
	})
	assert.NoError(t, err)
	_, err = q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, ErrGetLastMessageIdFailed.Error())
}

func testListQueueFailsToExtractQueueMetadata(ctx context.Context, t *testing.T, baseDB sqlplugin.DB) {
	// Using a different QueueType to prevent this test from failing because of queues created in previous tests.
	queueType := persistence.QueueV2Type(6)
	queueName := "test-queue-" + t.Name()
	q := persistencesql.NewQueueV2(baseDB, log.NewTestLogger())
	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        queueType,
		QueueName:        queueName,
		MetadataPayload:  []byte("test"),
		MetadataEncoding: "invalid_encoding",
	}
	_, err := baseDB.InsertIntoQueueV2Metadata(ctx, &row)
	assert.NoError(t, err)
	_, err = q.ListQueues(ctx, &persistence.InternalListQueuesRequest{
		QueueType: queueType,
		PageSize:  100,
	})
	assert.Error(t, err)
	assert.ErrorAs(t, err, new(*serialization.UnknownEncodingTypeError))
}
