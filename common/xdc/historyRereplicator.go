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

package xdc

import (
	"context"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	a "github.com/uber/cadence/client/admin"
	h "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

var (
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = errors.NewInternalFailureError("encounter empty history batch")

	// ErrNoHistoryRawEventBatches indicate that number of batches of history raw events is of size 0
	ErrNoHistoryRawEventBatches = errors.NewInternalFailureError("no history batches are returned")

	// ErrFristHistoryRawEventBatch indicate that first batch of history raw events is malformed
	ErrFristHistoryRawEventBatch = errors.NewInternalFailureError("encounter malformed first history batch")

	// ErrUnknownEncodingType indicate that the encoding type is unknown
	ErrUnknownEncodingType = errors.NewInternalFailureError("unknown encoding type")
)

type (
	// HistoryRereplicator is the interface for resending history events to remote
	HistoryRereplicator interface {
		// SendMultiWorkflowHistory sends multiple run IDs's history events to remote
		SendMultiWorkflowHistory(domainID string, workflowID string,
			beginingRunID string, beginingFirstEventID int64, endingRunID string, endingNextEventID int64) error
	}

	// HistoryRereplicatorImpl is the implementation of HistoryRereplicator
	HistoryRereplicatorImpl struct {
		domainCache        cache.DomainCache
		adminClient        a.Client
		historyClient      h.Client
		serializer         persistence.HistorySerializer
		replicationTimeout time.Duration
		logger             bark.Logger
	}
)

// NewHistoryRereplicator create a new HistoryRereplicatorImpl
func NewHistoryRereplicator(domainCache cache.DomainCache, adminClient a.Client, historyClient h.Client,
	serializer persistence.HistorySerializer, replicationTimeout time.Duration, logger bark.Logger) *HistoryRereplicatorImpl {

	return &HistoryRereplicatorImpl{
		domainCache:   domainCache,
		adminClient:   adminClient,
		historyClient: historyClient,
		serializer:    serializer,
		logger:        logger,
	}
}

// SendMultiWorkflowHistory sends multiple run IDs's history events to remote
func (h *HistoryRereplicatorImpl) SendMultiWorkflowHistory(domainID string, workflowID string,
	beginingRunID string, beginingFirstEventID int64, endingRunID string, endingNextEventID int64) (err error) {
	// NOTE: begining run ID and ending run ID can be different
	// the logic will try to grab events from begining run ID, first event ID to ending run ID, ending next event ID

	// beginingRunID can be empty, if there is no workflow in DB
	// endingRunID must not be empty, since this function is trigger missing events of endingRunID

	runID := beginingRunID
	for len(runID) != 0 && runID != endingRunID {
		firstEventID, nextEventID := h.eventIDRange(runID, beginingRunID, beginingFirstEventID, endingRunID, endingNextEventID)
		runID, err = h.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		if err != nil {
			return err
		}
	}

	if runID == endingRunID {
		firstEventID, nextEventID := h.eventIDRange(runID, beginingRunID, beginingFirstEventID, endingRunID, endingNextEventID)
		_, err = h.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		return err
	}

	// we have runID being empty string
	// this means that beginingRunID to endingRunID does not have a continue as new relation ship
	// such as beginingRunID -> runID1, then runID2 (no continued as new), then runID3 -> endingRunID
	// need to work backwords using endingRunID,
	// moreover, in the above case, runID2 cannot be replicated since no one points to it

	// runIDs to be use to resend history, in reverse order
	runIDs := []string{endingRunID}
	runID = endingRunID
	for len(runID) != 0 {
		runID, err = h.getPrevRunID(domainID, workflowID, runID)
		if err != nil {
			return err
		}
		runIDs = append(runIDs, runID)
	}

	// the last runID append in the array is empty
	// for all the runIDs, send the history
	for index := len(runIDs) - 2; index > -1; index-- {
		runID = runIDs[index]
		firstEventID, nextEventID := h.eventIDRange(runID, beginingRunID, beginingFirstEventID, endingRunID, endingNextEventID)
		_, err = h.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *HistoryRereplicatorImpl) sendSingleWorkflowHistory(domainID string, workflowID string, runID string,
	firstEventID int64, nextEventID int64) (string, error) {

	if firstEventID == nextEventID {
		// this is handling the case where the first event of a workflow triggers a resend
		return "", nil
	}

	var pendingRequest *history.ReplicateRawEventsRequest // pending replication request to history, initialized to nil

	// event store version, replication
	// for each pendingRequest to history
	var eventStoreVersion int32
	var replicationInfo map[string]*shared.ReplicationInfo

	pageSize := int32(100)
	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		response, err := h.getHistory(domainID, workflowID, runID, firstEventID, nextEventID, token, pageSize)
		if err != nil {
			return "", err
		}

		eventStoreVersion = response.GetEventStoreVersion()
		replicationInfo = response.ReplicationInfo
		token = response.NextPageToken

		for _, batch := range response.HistoryBatches {
			// it is intentional that the first request is nil
			// the reason is, we need to check the last request, if that request contains
			// continue as new, then new run history shall be included
			err := h.sendReplicationRawRequest(pendingRequest)
			if err != nil {
				return "", err
			}
			pendingRequest = h.createReplicationRawRequest(domainID, workflowID, runID, batch, eventStoreVersion, replicationInfo)
		}
	}
	// after this for loop, there shall be one request not sent yet
	// this request contains the last event, possible continue as new event
	lastBatch := pendingRequest.History
	nextRunID, err := h.getNextRunID(lastBatch)
	if err != nil {
		return "", err
	}
	if len(nextRunID) > 0 {
		// last event is continue as new
		// we need to do something special for that
		var token []byte
		pageSize := int32(1)
		response, err := h.getHistory(domainID, workflowID, nextRunID, common.FirstEventID, common.EndEventID, token, pageSize)
		if err != nil {
			return "", err
		}

		batch := response.HistoryBatches[0]

		pendingRequest.NewRunHistory = batch
		pendingRequest.NewRunEventStoreVersion = response.EventStoreVersion
	}

	return nextRunID, h.sendReplicationRawRequest(pendingRequest)
}

func (h *HistoryRereplicatorImpl) eventIDRange(currentRunID string,
	beginingRunID string, beginingFirstEventID int64,
	endingRunID string, endingNextEventID int64) (int64, int64) {

	if beginingRunID == endingRunID {
		return beginingFirstEventID, endingNextEventID
	}

	// beginingRunID != endingRunID

	if currentRunID == beginingRunID {
		// return all events from beginingFirstEventID to the end
		return beginingFirstEventID, common.EndEventID
	}

	if currentRunID == endingRunID {
		// return all events from the begining to endingNextEventID
		return common.FirstEventID, endingNextEventID
	}

	// for everything else, just dump the emtire history
	return common.FirstEventID, common.EndEventID
}

func (h *HistoryRereplicatorImpl) createReplicationRawRequest(
	domainID string, workflowID string, runID string,
	historyBlob *shared.DataBlob,
	eventStoreVersion int32,
	replicationInfo map[string]*shared.ReplicationInfo,
) *history.ReplicateRawEventsRequest {

	request := &history.ReplicateRawEventsRequest{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		ReplicationInfo:   replicationInfo,
		History:           historyBlob,
		EventStoreVersion: common.Int32Ptr(eventStoreVersion),
		// NewRunHistory this will be handled separately
		// NewRunEventStoreVersion  this will be handled separately
	}

	return request
}

func (h *HistoryRereplicatorImpl) sendReplicationRawRequest(request *history.ReplicateRawEventsRequest) error {

	if request == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.replicationTimeout)
	defer cancel()
	err := h.historyClient.ReplicateRawEvents(ctx, request)
	if err != nil {
		h.logger.WithFields(bark.Fields{
			logging.TagDomainID:            request.GetDomainUUID(),
			logging.TagWorkflowExecutionID: request.WorkflowExecution.GetWorkflowId(),
			logging.TagWorkflowRunID:       request.WorkflowExecution.GetRunId(),
			logging.TagErr:                 err,
		}).Error("error sending history")
	}
	return err
}

func (h *HistoryRereplicatorImpl) getHistory(domainID string, workflowID string, runID string,
	firstEventID int64, nextEventID int64, token []byte, pageSize int32) (*admin.GetWorkflowExecutionRawHistoryResponse, error) {

	logger := h.logger.WithFields(bark.Fields{
		logging.TagDomainID:            domainID,
		logging.TagWorkflowExecutionID: workflowID,
		logging.TagWorkflowRunID:       runID,
		logging.TagFirstEventID:        firstEventID,
		logging.TagNextEventID:         nextEventID,
	})

	domainEntry, err := h.domainCache.GetDomainByID(domainID)
	if err != nil {
		logger.WithField(logging.TagErr, err).Error("error getting domain")
		return nil, err
	}
	domainName := domainEntry.GetInfo().Name

	ctx, cancel := context.WithTimeout(context.Background(), h.replicationTimeout)
	defer cancel()
	response, err := h.adminClient.GetWorkflowExecutionRawHistory(ctx, &admin.GetWorkflowExecutionRawHistoryRequest{
		Domain: common.StringPtr(domainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		FirstEventId:    common.Int64Ptr(firstEventID),
		NextEventId:     common.Int64Ptr(nextEventID),
		MaximumPageSize: common.Int32Ptr(pageSize),
		NextPageToken:   token,
	})

	if err != nil {
		logger.WithField(logging.TagErr, err).Error("error getting history")
		return nil, err
	}
	if len(response.HistoryBatches) == 0 {
		logger.WithField(logging.TagErr, ErrNoHistoryRawEventBatches).Error(ErrNoHistoryRawEventBatches.Error())
		return nil, ErrNoHistoryRawEventBatches
	}

	return response, nil
}

func (h *HistoryRereplicatorImpl) getPrevRunID(domainID string, workflowID string, runID string) (string, error) {

	var token []byte // use nil since we are only getting the first event batch, for the start event
	pageSize := int32(1)
	response, err := h.getHistory(domainID, workflowID, runID, common.FirstEventID, common.EndEventID, token, pageSize)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return "", err
		}
		// EntityNotExistsError error, set the run ID to "" indicating no prev run
		return "", nil
	}

	blob := response.HistoryBatches[0]
	historyEvents, err := h.deserializeBlob(blob)
	if err != nil {
		return "", err
	}

	firstEvent := historyEvents[0]
	attr := firstEvent.WorkflowExecutionStartedEventAttributes
	if attr == nil {
		// malformed first event batch
		return "", ErrFristHistoryRawEventBatch
	}
	return attr.GetContinuedExecutionRunId(), nil
}

func (h *HistoryRereplicatorImpl) getNextRunID(blob *shared.DataBlob) (string, error) {

	historyEvents, err := h.deserializeBlob(blob)
	if err != nil {
		return "", err
	}

	lastEvent := historyEvents[len(historyEvents)-1]
	attr := lastEvent.WorkflowExecutionContinuedAsNewEventAttributes
	if attr == nil {
		// either workflow has not finished, or finished but not continue as new
		return "", nil
	}
	return attr.GetNewExecutionRunId(), nil
}

func (h *HistoryRereplicatorImpl) deserializeBlob(blob *shared.DataBlob) ([]*shared.HistoryEvent, error) {

	var err error
	var historyEvents []*shared.HistoryEvent

	switch blob.GetEncodingType() {
	case shared.EncodingTypeThriftRW:
		historyEvents, err = h.serializer.DeserializeBatchEvents(&persistence.DataBlob{
			Encoding: common.EncodingTypeThriftRW,
			Data:     blob.Data,
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrUnknownEncodingType
	}

	if len(historyEvents) == 0 {
		return nil, ErrEmptyHistoryRawEventBatch
	}
	return historyEvents, nil
}
