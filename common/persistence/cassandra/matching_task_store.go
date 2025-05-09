package cassandra

import (
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

func NewMatchingTaskStore(
	session gocql.Session,
	logger log.Logger,
	enableFairness bool,
) p.TaskStore {
	userDataStore := &userDataStore{Session: session, Logger: logger}
	if enableFairness {
		return newMatchingTaskStoreV2(userDataStore)
	}
	return newMatchingTaskStoreV1(userDataStore)
}
