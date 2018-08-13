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
	"encoding/json"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
)

const (
	templateAppendHistoryEvents = `INSERT INTO events (` +
		`domain_id, workflow_id, run_id, first_event_id, event_batch_version, range_id, tx_id, data, data_encoding, data_version) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateOverwriteHistoryEvents = `UPDATE events ` +
		`SET event_batch_version = ?, range_id = ?, tx_id = ?, data = ?, data_encoding = ?, data_version = ? ` +
		`WHERE domain_id = ? AND workflow_id = ? AND run_id = ? AND first_event_id = ? ` +
		`IF range_id <= ? AND tx_id < ?`

	templateGetWorkflowExecutionHistory = `SELECT first_event_id, event_batch_version, data, data_encoding, data_version FROM events ` +
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
	historyToken struct {
		LastEventBatchVersion int64
		LastEventID           int64
		Data                  []byte
	}

	cassandraHistoryPersistence struct {
		session           *gocql.Session
		logger            bark.Logger
		serializerFactory HistorySerializerFactory
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

	return &cassandraHistoryPersistence{session: session, logger: logger, serializerFactory: NewHistorySerializerFactory()}, nil
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
			request.EventBatchVersion,
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
			request.EventBatchVersion,
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
	token, err := h.deserializeToken(request)
	if err != nil {
		return nil, err
	}
	query := h.session.Query(templateGetWorkflowExecutionHistory,
		request.DomainID,
		*execution.WorkflowId,
		*execution.RunId,
		request.FirstEventID,
		request.NextEventID)

	iter := query.PageSize(request.PageSize).PageState(token.Data).Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetWorkflowExecutionHistory operation failed.  Not able to create query iterator.",
		}
	}

	found := false
	token.Data = iter.PageState()

	eventBatchVersionPointer := new(int64)
	eventBatchVersion := common.EmptyVersion
	lastFirstEventID := common.EmptyEventID // first_event_id of last batch
	eventBatch := SerializedHistoryEventBatch{}
	history := &workflow.History{}
	for iter.Scan(nil, &eventBatchVersionPointer, &eventBatch.Data, &eventBatch.EncodingType, &eventBatch.Version) {
		found = true

		if eventBatchVersionPointer != nil {
			eventBatchVersion = *eventBatchVersionPointer
		}
		if eventBatchVersion >= token.LastEventBatchVersion {
			historyBatch, err := h.deserializeEvents(&eventBatch)
			if err != nil {
				return nil, err
			}
			if len(historyBatch.Events) == 0 || historyBatch.Events[0].GetEventId() > token.LastEventID+1 {
				logger := h.logger.WithFields(bark.Fields{
					logging.TagWorkflowExecutionID: request.Execution.GetWorkflowId(),
					logging.TagWorkflowRunID:       request.Execution.GetRunId(),
					logging.TagDomainID:            request.DomainID,
				})
				logger.Error("Unexpected event batch")
				return nil, fmt.Errorf("corrupted history event batch")
			}

			if historyBatch.Events[0].GetEventId() != token.LastEventID+1 {
				// staled event batch, skip it
				continue
			}

			lastFirstEventID = historyBatch.Events[0].GetEventId()
			history.Events = append(history.Events, historyBatch.Events...)
			token.LastEventID = historyBatch.Events[len(historyBatch.Events)-1].GetEventId()
			token.LastEventBatchVersion = eventBatchVersion
		}

		eventBatchVersionPointer = new(int64)
		eventBatchVersion = common.EmptyVersion
		eventBatch = SerializedHistoryEventBatch{}
	}

	data, err := h.serializeToken(token)
	if err != nil {
		return nil, err
	}
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

	response := &GetWorkflowExecutionHistoryResponse{
		NextPageToken:    data,
		History:          history,
		LastFirstEventID: lastFirstEventID,
	}

	return response, nil
}

func (h *cassandraHistoryPersistence) deserializeEvents(e *SerializedHistoryEventBatch) (*HistoryEventBatch, error) {
	SetSerializedHistoryDefaults(e)
	s, _ := h.serializerFactory.Get(e.EncodingType)
	return s.Deserialize(e)
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

func (h *cassandraHistoryPersistence) serializeToken(token *historyToken) ([]byte, error) {
	if len(token.Data) == 0 {
		return nil, nil
	}

	data, err := json.Marshal(token)
	if err != nil {
		return nil, &workflow.InternalServiceError{Message: "Error generating history event token."}
	}
	return data, nil
}

func (h *cassandraHistoryPersistence) deserializeToken(request *GetWorkflowExecutionHistoryRequest) (*historyToken, error) {
	token := &historyToken{
		LastEventBatchVersion: common.EmptyVersion,
		LastEventID:           request.FirstEventID - 1,
	}

	if len(request.NextPageToken) == 0 {
		return token, nil
	}

	err := json.Unmarshal(request.NextPageToken, token)
	if err == nil {
		return token, nil
	}

	// for backward compatible reason, the input data can be raw Cassandra token
	token.Data = request.NextPageToken
	return token, nil
}
