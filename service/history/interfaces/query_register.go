package interfaces

import querypb "go.temporal.io/api/query/v1"

type QueryCompletionType int
type QueryCompletionState struct {
	Type   QueryCompletionType
	Result *querypb.WorkflowQueryResult
	Err    error
}

type QueryRegistry interface {
	HasBufferedQuery() bool
	GetBufferedIDs() []string
	HasCompletedQuery() bool
	GetCompletedIDs() []string
	HasUnblockedQuery() bool
	GetUnblockedIDs() []string
	HasFailedQuery() bool
	GetFailedIDs() []string

	GetQueryCompletionCh(string) (<-chan struct{}, error)
	GetQueryInput(string) (*querypb.WorkflowQuery, error)
	GetCompletionState(string) (*QueryCompletionState, error)

	BufferQuery(queryInput *querypb.WorkflowQuery) (string, <-chan struct{})
	SetCompletionState(string, *QueryCompletionState) error
	RemoveQuery(id string)
	Clear()
}
