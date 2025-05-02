package gocql

import (
	"github.com/gocql/gocql"
)

type iter struct {
	session   *session
	gocqlIter *gocql.Iter
}

func newIter(session *session, gocqlIter *gocql.Iter) *iter {
	return &iter{
		session:   session,
		gocqlIter: gocqlIter,
	}
}

func (it *iter) Scan(dest ...interface{}) bool {
	return it.gocqlIter.Scan(dest...)
}

func (it *iter) MapScan(m map[string]interface{}) bool {
	return it.gocqlIter.MapScan(m)
}

func (it *iter) PageState() []byte {
	return it.gocqlIter.PageState()
}

func (it *iter) Close() (retError error) {
	defer func() { it.session.handleError(retError) }()

	return it.gocqlIter.Close()
}
