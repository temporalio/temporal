// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		`domain_id, workflow_id, run_id, first_event_id, range_id, tx_id, data, data_encoding, data_version) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateOverwriteHistoryEvents = `UPDATE events ` +
		`SET range_id = ?, tx_id = ?, data = ?, data_encoding = ?, data_version = ? ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ? ` +
		`IF range_id <= ? AND tx_id < ?`

	templateGetWorkflowExecutionHistory = `SELECT first_event_id, data, data_encoding, data_version FROM events ` +
		`WHERE domain_id = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? ` +
		`AND first_event_id >= ? ` +
		`AND first_event_id < ?`

	templateDeleteWorkflowExecutionHistory = `DELETE FROM events ` +
		`WHERE domain_id = ? ` +
		`AND workflow_id = ? ` +
		`AND run_id = ? `
)

type (
	cassandraHistoryPersistence struct {
		session *gocql.Session
		logger  bark.Logger
	}
)

// NewCassandraHistoryPersistence is used to create an instance of HistoryManager implementation
func NewCassandraHistoryPersistence(hosts string, port int, user, password, dc string, keyspace string,
	numConns int, logger bark.Logger) (HistoryManager,
	error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout
	cluster.NumConns = numConns

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraHistoryPersistence{session: session, logger: logger}, nil
}

// Close gracefully releases the resources held by this object
func (h *cassandraHistoryPersistence) Close() {
	if h.session != nil {
		h.session.Close()
	}
}

func (h *cassandraHistoryPersistence) AppendHistoryEvents(request *AppendHistoryEventsRequest) error {
	var query *gocql.Query
	if request.Overwrite {
		query = h.session.Query(templateOverwriteHistoryEvents,
			request.RangeID,
			request.TransactionID,
			request.Events.Data,
			request.Events.EncodingType,
			request.Events.Version,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.FirstEventID,
			request.RangeID,
			request.TransactionID)
	} else {
		query = h.session.Query(templateAppendHistoryEvents,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.FirstEventID,
			request.RangeID,
			request.TransactionID,
			request.Events.Data,
			request.Events.EncodingType,
			request.Events.Version)
	}

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Error: %v", err),
			}
		} else if isTimeoutError(err) {
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
		*execution.WorkflowId,
		*execution.RunId,
		request.FirstEventID,
		request.NextEventID)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetWorkflowExecutionHistory operation failed.  Not able to create query iterator.",
		}
	}

	var firstEventID int64
	var history SerializedHistoryEventBatch
	response := &GetWorkflowExecutionHistoryResponse{}
	found := false
	for iter.Scan(&firstEventID, &history.Data, &history.EncodingType, &history.Version) {
		found = true
		response.Events = append(response.Events, history)
		history = SerializedHistoryEventBatch{}
	}

	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecutionHistory operation failed. Error: %v", err),
		}
	}

	if !found && len(request.NextPageToken) == 0 {
		// adding the check of request next token being not nil, since
		// there can be case when found == false at the very end of pagination.
		return nil, &workflow.EntityNotExistsError{
			Message: fmt.Sprintf("Workflow execution history not found.  WorkflowId: %v, RunId: %v",
				*execution.WorkflowId, *execution.RunId),
		}
	}

	return response, nil
}

func (h *cassandraHistoryPersistence) DeleteWorkflowExecutionHistory(
	request *DeleteWorkflowExecutionHistoryRequest) error {
	execution := request.Execution
	query := h.session.Query(templateDeleteWorkflowExecutionHistory,
		request.DomainID,
		*execution.WorkflowId,
		*execution.RunId)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("DeleteWorkflowExecutionHistory operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecutionHistory operation failed. Error: %v", err),
		}
	}

	return nil
}
