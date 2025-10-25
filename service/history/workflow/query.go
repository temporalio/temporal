package workflow

import (
	"sync/atomic"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const (
	QueryCompletionTypeSucceeded historyi.QueryCompletionType = iota
	QueryCompletionTypeUnblocked
	QueryCompletionTypeFailed
)

var (
	errCompletionStateInvalid    = serviceerror.NewInternal("query completion state invalid")
	errAlreadyInCompletionState  = serviceerror.NewInternal("query is already in completion state")
	errQueryNotInCompletionState = serviceerror.NewInternal("query is not in completion state")
)

type (
	query interface {
		getID() string
		getCompletionCh() <-chan struct{}
		getQueryInput() *querypb.WorkflowQuery
		GetCompletionState() (*historyi.QueryCompletionState, error)
		setCompletionState(*historyi.QueryCompletionState) error
	}

	queryImpl struct {
		id           string
		queryInput   *querypb.WorkflowQuery
		completionCh chan struct{}

		completionState atomic.Value
	}
)

func newQuery(queryInput *querypb.WorkflowQuery) query {
	return &queryImpl{
		id:           uuid.NewString(),
		queryInput:   queryInput,
		completionCh: make(chan struct{}),
	}
}

func (q *queryImpl) getID() string {
	return q.id
}

func (q *queryImpl) getCompletionCh() <-chan struct{} {
	return q.completionCh
}

func (q *queryImpl) getQueryInput() *querypb.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) GetCompletionState() (*historyi.QueryCompletionState, error) {
	ts := q.completionState.Load()
	if ts == nil {
		return nil, errQueryNotInCompletionState
	}
	queryState, ok := ts.(*historyi.QueryCompletionState)
	if !ok {
		return nil, errCompletionStateInvalid
	}
	return queryState, nil
}

func (q *queryImpl) setCompletionState(completionState *historyi.QueryCompletionState) error {
	if err := q.validateCompletionState(completionState); err != nil {
		return err
	}
	currCompletionState, _ := q.GetCompletionState()
	if currCompletionState != nil {
		return errAlreadyInCompletionState
	}
	q.completionState.Store(completionState)
	close(q.completionCh)
	return nil
}

func (q *queryImpl) validateCompletionState(
	completionState *historyi.QueryCompletionState,
) error {
	if completionState == nil {
		return errCompletionStateInvalid
	}
	switch completionState.Type {
	case QueryCompletionTypeSucceeded:
		if completionState.Result == nil || completionState.Err != nil {
			return errCompletionStateInvalid
		}
		queryResult := completionState.Result
		validAnswered := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_ANSWERED &&
			queryResult.Answer != nil &&
			queryResult.GetErrorMessage() == ""
		validFailed := queryResult.GetResultType() == enumspb.QUERY_RESULT_TYPE_FAILED &&
			queryResult.Answer == nil &&
			queryResult.GetErrorMessage() != ""
		if !validAnswered && !validFailed {
			return errCompletionStateInvalid
		}
		return nil
	case QueryCompletionTypeUnblocked:
		if completionState.Result != nil || completionState.Err != nil {
			return errCompletionStateInvalid
		}
		return nil
	case QueryCompletionTypeFailed:
		if completionState.Result != nil || completionState.Err == nil {
			return errCompletionStateInvalid
		}
		return nil
	default:
		return errCompletionStateInvalid
	}
}
