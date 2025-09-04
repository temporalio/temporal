package sql

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type taskQueuePageToken struct {
	MinRangeHash   uint32
	MinTaskQueueId []byte
}

type matchingTaskPageToken struct {
	TaskPass int64 `json:",omitempty"`
	TaskID   int64
}

type userDataListNextPageToken struct {
	LastTaskQueueName string
}

// newTaskPersistence creates a new instance of TaskStore
func newTaskPersistence(
	db sqlplugin.DB,
	taskScanPartitions int,
	logger log.Logger,
	enableFairness bool,
) (persistence.TaskStore, error) {
	store := SqlStore{
		DB:     db,
		logger: logger,
	}
	userDataStore := userDataStore{SqlStore: store}
	taskQueueStore := taskQueueStore{
		SqlStore:           store,
		version:            sqlplugin.MatchingTaskVersion1,
		taskScanPartitions: uint32(taskScanPartitions),
	}
	if enableFairness {
		taskQueueStore.version = sqlplugin.MatchingTaskVersion2
		return newTaskManagerV2(db, userDataStore, taskQueueStore, logger)
	}
	return newTaskManagerV1(db, userDataStore, taskQueueStore, logger)
}
