package cassandra

import (
	"time"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewMatchingTaskStore(
	session gocql.Session,
	logger log.Logger,
	enableFairness bool,
) p.TaskStore {
	if enableFairness {
		return newMatchingTaskStoreV2(session)
	}
	return newMatchingTaskStoreV1(session)
}

const (
	// Row types for table tasks. Lower bit only: see rowTypeTaskInSubqueue for more details.
	rowTypeTask      = 0
	rowTypeTaskQueue = 1
)

// We steal some upper bits of the "row type" field to hold a subqueue index.
// Subqueue 0 must be the same as rowTypeTask (before subqueues were introduced).
// 00000000: task in subqueue 0 (rowTypeTask)
// 00000001: task queue metadata (rowTypeTaskQueue)
// xxxxxx1x: reserved
// 00000100: task in subqueue 1
// nnnnnn00: task in subqueue n, etc.
func rowTypeTaskInSubqueue(subqueue int) int {
	return subqueue<<2 | rowTypeTask // nolint:staticcheck
}

func getTaskTTL(expireTime *timestamppb.Timestamp) int64 {
	if expireTime == nil || expireTime.AsTime().IsZero() {
		return 0
	}

	expiryTtl := convert.Int64Ceil(time.Until(expireTime.AsTime()).Seconds())

	// 0 means no ttl, we dont want that.
	// Todo: Come back and correctly ignore expired in-memory tasks before persisting
	if expiryTtl < 1 {
		expiryTtl = 1
	}

	return expiryTtl
}
