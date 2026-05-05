package gocql

import (
	"encoding/binary"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"go.temporal.io/server/chasm"
)

func UUIDToString(
	item any,
) string {
	return item.(gocql.UUID).String()
}

func UUIDsToStringSlice(
	item any,
) []string {
	uuids := item.([]gocql.UUID)
	results := make([]string, len(uuids))
	for i, uuid := range uuids {
		results[i] = uuid.String()
	}
	return results
}

func ArchetypeIDToUUID(
	archetypeID chasm.ArchetypeID,
) string {
	var uuid gocql.UUID
	binary.BigEndian.PutUint32(uuid[12:], archetypeID)
	return uuid.String()
}
