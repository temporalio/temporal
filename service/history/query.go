package history

import (
	"sync/atomic"

	"github.com/pborman/uuid"
	querypb "go.temporal.io/temporal-proto/query"
	"go.temporal.io/temporal-proto/serviceerror"
)

const (
	queryTerminationTypeCompleted queryTerminationType = iota
	queryTerminationTypeUnblocked
	queryTerminationTypeFailed
)

var (
	errTerminationStateInvalid = serviceerror.NewInternal("query termination state invalid")
	errAlreadyInTerminalState  = serviceerror.NewInternal("query already in terminal state")
	errQueryNotInTerminalState = serviceerror.NewInternal("query not in terminal state")
)

type (
	queryTerminationType int

	query interface {
		getQueryID() string
		getQueryTermCh() <-chan struct{}
		getQueryInput() *querypb.WorkflowQuery
		getTerminationState() (*queryTerminationState, error)
		setTerminationState(*queryTerminationState) error
	}

	queryImpl struct {
		id         string
		queryInput *querypb.WorkflowQuery
		termCh     chan struct{}

		terminationState atomic.Value
	}

	queryTerminationState struct {
		queryTerminationType queryTerminationType
		queryResult          *querypb.WorkflowQueryResult
		failure              error
	}
)

func newQuery(queryInput *querypb.WorkflowQuery) query {
	return &queryImpl{
		id:         uuid.New(),
		queryInput: queryInput,
		termCh:     make(chan struct{}),
	}
}

func (q *queryImpl) getQueryID() string {
	return q.id
}

func (q *queryImpl) getQueryTermCh() <-chan struct{} {
	return q.termCh
}

func (q *queryImpl) getQueryInput() *querypb.WorkflowQuery {
	return q.queryInput
}

func (q *queryImpl) getTerminationState() (*queryTerminationState, error) {
	ts := q.terminationState.Load()
	if ts == nil {
		return nil, errQueryNotInTerminalState
	}
	return ts.(*queryTerminationState), nil
}

func (q *queryImpl) setTerminationState(terminationState *queryTerminationState) error {
	if err := q.validateTerminationState(terminationState); err != nil {
		return err
	}
	currTerminationState, _ := q.getTerminationState()
	if currTerminationState != nil {
		return errAlreadyInTerminalState
	}
	q.terminationState.Store(terminationState)
	close(q.termCh)
	return nil
}

func (q *queryImpl) validateTerminationState(
	terminationState *queryTerminationState,
) error {
	if terminationState == nil {
		return errTerminationStateInvalid
	}
	switch terminationState.queryTerminationType {
	case queryTerminationTypeCompleted:
		if terminationState.queryResult == nil || terminationState.failure != nil {
			return errTerminationStateInvalid
		}
		queryResult := terminationState.queryResult
		validAnswered := queryResult.GetResultType() == querypb.QueryResultType_Answered &&
			queryResult.Answer != nil &&
			queryResult.GetErrorMessage() == ""
		validFailed := queryResult.GetResultType() == querypb.QueryResultType_Failed &&
			queryResult.Answer == nil &&
			queryResult.GetErrorMessage() != ""
		if !validAnswered && !validFailed {
			return errTerminationStateInvalid
		}
		return nil
	case queryTerminationTypeUnblocked:
		if terminationState.queryResult != nil || terminationState.failure != nil {
			return errTerminationStateInvalid
		}
		return nil
	case queryTerminationTypeFailed:
		if terminationState.queryResult != nil || terminationState.failure == nil {
			return errTerminationStateInvalid
		}
		return nil
	default:
		return errTerminationStateInvalid
	}
}
