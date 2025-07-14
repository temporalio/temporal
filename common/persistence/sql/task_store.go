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
	TaskID int64
}

type userDataListNextPageToken struct {
	LastTaskQueueName string
}

// newTaskPersistence creates a new instance of TaskManager
func newTaskPersistence(
	db sqlplugin.DB,
	taskScanPartitions int,
	logger log.Logger,
	enableFairness bool,
) (persistence.TaskStore, error) {
	userDataStore := newUserDataStore(db, logger)
	if enableFairness {
		return newTaskManagerV2(db, userDataStore, taskScanPartitions, logger)
	}
	return newTaskManagerV1(db, userDataStore, taskScanPartitions, logger)
}
