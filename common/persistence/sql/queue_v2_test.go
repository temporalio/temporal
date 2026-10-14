package sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.uber.org/mock/gomock"
)

func TestQueueV2EnqueueMessageRollback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		insertFails bool
		rollbackErr error
		wantLog     bool
	}{
		{name: "get last message ID fails, transaction already done", rollbackErr: sql.ErrTxDone},
		{name: "get last message ID fails, rollback fails", rollbackErr: errors.New("connection reset"), wantLog: true},
		{name: "insert fails, transaction already done", insertFails: true, rollbackErr: sql.ErrTxDone},
		{name: "insert fails, rollback fails", insertFails: true, rollbackErr: errors.New("connection reset"), wantLog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			tx := sqlplugin.NewMockTx(ctrl)
			db := sqlplugin.NewMockDB(ctrl)
			logger := log.NewMockLogger(ctrl)
			db.EXPECT().SelectFromQueueV2Metadata(gomock.Any(), gomock.Any()).Return(
				&sqlplugin.QueueV2MetadataRow{MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String()}, nil)
			db.EXPECT().BeginTx(gomock.Any()).Return(tx, nil)
			if tc.insertFails {
				tx.EXPECT().GetLastEnqueuedMessageIDForUpdateV2(gomock.Any(), gomock.Any()).Return(int64(0), sql.ErrNoRows)
				tx.EXPECT().InsertIntoQueueV2Messages(gomock.Any(), gomock.Any()).Return(nil, context.Canceled)
			} else {
				tx.EXPECT().GetLastEnqueuedMessageIDForUpdateV2(gomock.Any(), gomock.Any()).Return(int64(0), context.Canceled)
			}
			tx.EXPECT().Rollback().Return(tc.rollbackErr)
			if tc.wantLog {
				logger.EXPECT().Error("transaction rollback error", tag.Error(tc.rollbackErr))
			}

			request := &persistence.InternalEnqueueMessageRequest{
				QueueType: persistence.QueueTypeHistoryNormal, QueueName: "test-queue", Blob: &commonpb.DataBlob{},
			}
			_, err := NewQueueV2(db, logger, nil).EnqueueMessage(context.Background(), request)
			require.ErrorContains(t, err, context.Canceled.Error())
		})
	}
}
