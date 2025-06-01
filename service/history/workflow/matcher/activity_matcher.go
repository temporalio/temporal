package matcher

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

/*
MatchActivity matches the given query with the given activity info.
The query should be a valid SQL query with WHERE clause.
The supported fields are:
  - ActivityId
  - ActivityType
  - Attempts
  - BackoffInterval (in seconds)
  - Status
  - LastFailure

Field names are case-sensitive.

The query can have multiple conditions combined with AND/OR.
The query can have conditions on multiple fields.
Date time fields should be in RFC3339 format.
Example query:

	"WHERE ActivityType = 'my_activity' AND Attempts > 10"

Different fields can support different operators:
  - LastFailure: =, !=, starts_with, not starts_with, contains, not contains
  - ActivityType: =, !=, starts_with, not starts_with
  - ActivityId, Status: =, !=
  - Attempts, BackoffInterval: =, !=, >, >=, <, <=

Returns true if the query matches the activity info, false otherwise, or error if the query is invalid.
*/
func MatchActivity(ai *persistencespb.ActivityInfo, query string) (bool, error) {
	evaluator := newActivityMatchEvaluator(ai)
	return evaluator.Evaluate(query)
}
