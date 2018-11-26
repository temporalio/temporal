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

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
)

type (

	// historyManagerImpl implements HistoryManager based on HistoryStore and HistorySerializer
	historyManagerImpl struct {
		serializer  HistorySerializer
		persistence HistoryStore
		logger      bark.Logger
	}

	// historyToken is used to serialize/deserialize pagination token for GetWorkflowExecutionHistory
	historyToken struct {
		LastEventBatchVersion int64
		LastEventID           int64
		Data                  []byte
	}
)

var _ HistoryManager = (*historyManagerImpl)(nil)

//NewHistoryManagerImpl returns new HistoryManager
func NewHistoryManagerImpl(persistence HistoryStore, logger bark.Logger) HistoryManager {
	return &historyManagerImpl{
		serializer:  NewHistorySerializer(),
		persistence: persistence,
		logger:      logger,
	}
}

func (m *historyManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *historyManagerImpl) AppendHistoryEvents(request *AppendHistoryEventsRequest) (*AppendHistoryEventsResponse, error) {
	if len(request.Events) == 0 {
		return nil, fmt.Errorf("events to be appended cannot be empty")
	}
	eventsData, err := m.serializer.SerializeBatchEvents(request.Events, request.Encoding)
	if err != nil {
		return nil, err
	}

	resp := &AppendHistoryEventsResponse{Size: len(eventsData.Data)}
	return resp, m.persistence.AppendHistoryEvents(
		&InternalAppendHistoryEventsRequest{
			DomainID:          request.DomainID,
			Execution:         request.Execution,
			FirstEventID:      request.FirstEventID,
			EventBatchVersion: request.EventBatchVersion,
			RangeID:           request.RangeID,
			TransactionID:     request.TransactionID,
			Events:            eventsData,
			Overwrite:         request.Overwrite,
		})
}

// GetWorkflowExecutionHistoryByBatch retrieves the paginated list of history events for given execution
func (m *historyManagerImpl) GetWorkflowExecutionHistoryByBatch(request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryByBatchResponse, error) {
	resp := &GetWorkflowExecutionHistoryByBatchResponse{}
	var err error
	resp.History, _, resp.NextPageToken, resp.LastFirstEventID, resp.Size, err = m.getWorkflowExecutionHistory(request, true)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWorkflowExecutionHistory retrieves the paginated list of history events for given execution
func (m *historyManagerImpl) GetWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest) (*GetWorkflowExecutionHistoryResponse, error) {
	resp := &GetWorkflowExecutionHistoryResponse{}
	var err error
	_, resp.History, resp.NextPageToken, resp.LastFirstEventID, resp.Size, err = m.getWorkflowExecutionHistory(request, false)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetWorkflowExecutionHistory retrieves the paginated list of history events for given execution
func (m *historyManagerImpl) getWorkflowExecutionHistory(request *GetWorkflowExecutionHistoryRequest, byBatch bool) ([]*workflow.History, *workflow.History, []byte, int64, int, error) {
	token, err := m.deserializeToken(request)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}

	// persistence API expects the actual cassandra paging token
	newRequest := &InternalGetWorkflowExecutionHistoryRequest{
		LastEventBatchVersion: token.LastEventBatchVersion,
		NextPageToken:         token.Data,

		DomainID:     request.DomainID,
		Execution:    request.Execution,
		FirstEventID: request.FirstEventID,
		NextEventID:  request.NextEventID,
		PageSize:     request.PageSize,
	}
	response, err := m.persistence.GetWorkflowExecutionHistory(newRequest)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	// we store LastEventBatchVersion in the token. The reason we do it here is for historic reason.
	token.LastEventBatchVersion = response.LastEventBatchVersion
	token.Data = response.NextPageToken

	history := &workflow.History{
		Events: make([]*workflow.HistoryEvent, 0, request.PageSize),
	}
	historyBatches := make([]*workflow.History, 0, request.PageSize)

	// first_event_id of the last batch
	lastFirstEventID := common.EmptyEventID
	size := 0

	for _, b := range response.History {
		size += len(b.Data)
		historyBatch, err := m.serializer.DeserializeBatchEvents(b)
		if err != nil {
			return nil, nil, nil, 0, 0, err
		}

		if len(historyBatch) == 0 || historyBatch[0].GetEventId() > token.LastEventID+1 {
			logger := m.logger.WithFields(bark.Fields{
				logging.TagWorkflowExecutionID: request.Execution.GetWorkflowId(),
				logging.TagWorkflowRunID:       request.Execution.GetRunId(),
				logging.TagDomainID:            request.DomainID,
			})
			logger.Error("Unexpected event batch")
			return nil, nil, nil, 0, 0, fmt.Errorf("corrupted history event batch")
		}

		if historyBatch[0].GetEventId() != token.LastEventID+1 {
			// staled event batch, skip it
			continue
		}

		lastFirstEventID = historyBatch[0].GetEventId()
		if byBatch {
			batch := workflow.History{
				Events: historyBatch,
			}
			historyBatches = append(historyBatches, &batch)
		}
		history.Events = append(history.Events, historyBatch...)
		token.LastEventID = historyBatch[len(historyBatch)-1].GetEventId()
	}

	nextToken, err := m.serializeToken(token, request.NextEventID)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}

	return historyBatches, history, nextToken, lastFirstEventID, size, nil
}

func (m *historyManagerImpl) deserializeToken(request *GetWorkflowExecutionHistoryRequest) (*historyToken, error) {
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

func (m *historyManagerImpl) serializeToken(token *historyToken, nextEventID int64) ([]byte, error) {
	if token.LastEventID+1 >= nextEventID || len(token.Data) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(token)
	if err != nil {
		return nil, &workflow.InternalServiceError{Message: "Error generating history event token."}
	}
	return data, nil
}

func (m *historyManagerImpl) DeleteWorkflowExecutionHistory(request *DeleteWorkflowExecutionHistoryRequest) error {
	return m.persistence.DeleteWorkflowExecutionHistory(request)
}

func (m *historyManagerImpl) Close() {
	m.persistence.Close()
}
