package gocql

import (
	"github.com/gocql/gocql"
)

func UUIDToString(
	item interface{},
) string {
	return item.(gocql.UUID).String()
}

func UUIDsToStringSlice(
	item interface{},
) []string {
	uuids := item.([]gocql.UUID)
	results := make([]string, len(uuids))
	for i, uuid := range uuids {
		results[i] = uuid.String()
	}
	return results
}
