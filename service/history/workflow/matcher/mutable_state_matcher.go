package matcher

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

/*
MatchMutableState matches the given query with the given mutable state.
The query should be a valid SQL query with WHERE clause.
The supported fields are:
  - WorkflowId
  - WorkflowType
  - StartTime
  - ExecutionStatus

Field names are case-sensitive.

The query can have multiple conditions combined with AND/OR.
The query can have conditions on multiple fields.
Date time fields should be in RFC3339 format.
Example query:

	"WHERE WorkflowId = 'some_workflow_id' AND StartTime > '2023-10-26T14:30:00Z'"

Different fields can support different operators:
  - WorkflowId: =, !=, starts_with, not starts_with
  - WorkflowType: =, !=, starts_with, not starts_with
  - StartTime: =, !=, >, >=, <, <=
  - ExecutionStatus: =, !=

Returns true if the query matches the mutable state, false otherwise, or error if the query is invalid.
*/
func MatchMutableState(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	query string,
) (bool, error) {
	evaluator := newMutableStateMatchEvaluator(executionInfo, executionState)
	return evaluator.Evaluate(query)
}
