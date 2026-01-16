package mongodb

import (
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/api/serviceerror"
)

// isDuplicateIndexError reports whether the provided error corresponds to a duplicate index creation attempt.
// Mongo surfaces these through mongo.CommandError (code 85) or duplicate key error (11000) depending on context.
func isDuplicateIndexError(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr.Code == 85 || cmdErr.Code == 11000
	}
	return mongo.IsDuplicateKeyError(err)
}

// mapDuplicateNamespaceError normalizes Mongo duplicate errors to serviceerror.Unavailable to align with
// persistence test expectations (Cassandra/SQL emit Unavailable for rename conflicts).
func mapDuplicateNamespaceError(err error, namespace string) error {
	if err == nil {
		return nil
	}
	if isDuplicateIndexError(err) || mongo.IsDuplicateKeyError(err) {
		return serviceerror.NewUnavailable(fmt.Sprintf("namespace %s already exists", namespace))
	}
	return nil
}
