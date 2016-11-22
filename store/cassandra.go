package store

import (
	"github.com/gocql/gocql"
)

// CassandraStoreService store service
type CassandraStoreService struct {
	session *gocql.Session
}
