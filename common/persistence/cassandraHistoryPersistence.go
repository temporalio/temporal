package persistence

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

const (
	templateAppendHistoryEvents = `INSERT INTO events (` +
		`domain_id, workflow_id, run_id, first_event_id, range_id, tx_id, data) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateOverwriteHistoryEvents = `UPDATE events ` +
		`SET range_id = ?, tx_id = ?, data = ? ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ? ` +
		`IF range_id <= ? AND tx_id < ?`

	templateGetWorkflowExecutionHistory = `SELECT first_event_id, data FROM events ` +
		`WHERE domain_id = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? ` +
		`AND first_event_id < ?`

	templateDeleteWorkflowExecutionHistory = `DELETE FROM events ` +
		`WHERE domain_id = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? `
)

type (
	cassandraHistoryPersistence struct {
		session      *gocql.Session
		logger       bark.Logger
	}
)

// NewCassandraHistoryPersistence is used to create an instance of HistoryManager implementation
func NewCassandraHistoryPersistence(hosts string, dc string, keyspace string, logger bark.Logger) (HistoryManager,
	error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraHistoryPersistence{session: session, logger: logger}, nil
}

func (h *cassandraHistoryPersistence) AppendHistoryEvents(request *AppendHistoryEventsRequest) error {
	var query *gocql.Query
	if request.Overwrite {
		query = h.session.Query(templateOverwriteHistoryEvents,
			request.RangeID,
			request.TransactionID,
			request.Events,
			request.DomainID,
			request.Execution.GetWorkflowId(),
			request.Execution.GetRunId(),
			request.FirstEventID,
			request.RangeID,
			request.TransactionID)
	} else {
		query = h.session.Query(templateAppendHistoryEvents,
			request.DomainID,
			request.Execution.GetWorkflowId(),
			request.Execution.GetRunId(),
			request.FirstEventID,
			request.RangeID,
			request.TransactionID,
			request.Events)
	}

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if _, ok := err.(*gocql.RequestErrWriteTimeout); ok {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &TimeoutError{Msg: fmt.Sprintf("AppendHistoryEvents timed out. Error: %v", err)}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("AppendHistoryEvents operation failed. Error: %v", err),
		}
	}

	if !applied {
		return &ConditionFailedError{
			Msg: "Failed to append history events.",
		}
	}

	return nil
}

func (h *cassandraHistoryPersistence) GetWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest) (
	*GetWorkflowExecutionHistoryResponse, error) {
	execution := request.Execution
	query := h.session.Query(templateGetWorkflowExecutionHistory,
		request.DomainID,
		execution.GetWorkflowId(),
		execution.GetRunId(),
		request.NextEventID)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetWorkflowExecutionHistory operation failed.  Not able to create query iterator.",
		}
	}

	var firstEventID int64
	var events []byte
	response := &GetWorkflowExecutionHistoryResponse{}
	found := false
	for iter.Scan(&firstEventID, &events) {
		found = true
		response.Events = append(response.Events, events)
		events = nil
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecutionHistory operation failed. Error: %v", err),
		}
	}

	if !found {
		return nil, &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Workflow execution history not found.  WorkflowId: %v, RunId: %v",
				execution.GetWorkflowId(), execution.GetRunId()),
		}
	}

	return response, nil
}

func (h *cassandraHistoryPersistence) DeleteWorkflowExecutionHistory(
	request *DeleteWorkflowExecutionHistoryRequest) error {
	execution := request.Execution
	query := h.session.Query(templateDeleteWorkflowExecutionHistory,
		request.DomainID,
		execution.GetWorkflowId(),
		execution.GetRunId())

	err := query.Exec()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecutionHistory operation failed. Error: %v", err),
		}
	}

	return nil
}
