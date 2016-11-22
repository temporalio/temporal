package store

import (
	"github.com/gocql/gocql"
)

type CassandraStoreService struct {
	session *gocql.Session
}
