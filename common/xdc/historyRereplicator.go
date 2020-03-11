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

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/rpc"
)

var (
	// ErrNoHistoryRawEventBatches indicate that number of batches of history raw events is of size 0
	ErrNoHistoryRawEventBatches = serviceerror.NewInternal("no history batches are returned")

	// ErrFirstHistoryRawEventBatch indicate that first batch of history raw events is malformed
	ErrFirstHistoryRawEventBatch = serviceerror.NewInternal("encounter malformed first history batch")

	// ErrUnknownEncodingType indicate that the encoding type is unknown
	ErrUnknownEncodingType = serviceerror.NewInternal("unknown encoding type")
)

const (
	defaultPageSize = int32(100)
)

type (
	// historyReplicationFn provides the functionality to deliver replication raw history request to history
	historyReplicationFn func(ctx context.Context, request *historyservice.ReplicateRawEventsRequest) error

	// HistoryRereplicator is the interface for resending history events to remote
	HistoryRereplicator interface {
		// SendMultiWorkflowHistory sends multiple run IDs's history events to remote
		SendMultiWorkflowHistory(domainID string, workflowID string,
			beginningRunID string, beginningFirstEventID int64, endingRunID string, endingNextEventID int64) error
	}

	// HistoryRereplicatorImpl is the implementation of HistoryRereplicator
	HistoryRereplicatorImpl struct {
		targetClusterName    string
		domainCache          cache.DomainCache
		adminClient          admin.Client
		historyReplicationFn historyReplicationFn
		serializer           persistence.PayloadSerializer
		replicationTimeout   time.Duration
		logger               log.Logger
	}

	historyRereplicationContext struct {
		seenEmptyEvents       bool
		rpcCalls              int
		domainID              string
		workflowID            string
		beginningRunID        string
		beginningFirstEventID int64
		endingRunID           string
		endingNextEventID     int64
		rereplicator          *HistoryRereplicatorImpl
		logger                log.Logger
	}
)

func newHistoryRereplicationContext(domainID string, workflowID string,
	beginningRunID string, beginningFirstEventID int64,
	endingRunID string, endingNextEventID int64,
	rereplicator *HistoryRereplicatorImpl) *historyRereplicationContext {
	logger := rereplicator.logger.WithTags(
		tag.WorkflowDomainID(domainID), tag.WorkflowID(workflowID), tag.WorkflowBeginningRunID(beginningRunID),
		tag.WorkflowBeginningFirstEventID(beginningFirstEventID), tag.WorkflowEndingRunID(endingRunID),
		tag.WorkflowEndingNextEventID(endingNextEventID))
	return &historyRereplicationContext{
		seenEmptyEvents:       false,
		rpcCalls:              0,
		domainID:              domainID,
		workflowID:            workflowID,
		beginningRunID:        beginningRunID,
		beginningFirstEventID: beginningFirstEventID,
		endingRunID:           endingRunID,
		endingNextEventID:     endingNextEventID,
		rereplicator:          rereplicator,
		logger:                logger,
	}
}

// NewHistoryRereplicator create a new HistoryRereplicatorImpl
func NewHistoryRereplicator(targetClusterName string, domainCache cache.DomainCache, adminClient admin.Client, historyReplicationFn historyReplicationFn,
	serializer persistence.PayloadSerializer, replicationTimeout time.Duration, logger log.Logger) *HistoryRereplicatorImpl {

	return &HistoryRereplicatorImpl{
		targetClusterName:    targetClusterName,
		domainCache:          domainCache,
		adminClient:          adminClient,
		historyReplicationFn: historyReplicationFn,
		serializer:           serializer,
		replicationTimeout:   replicationTimeout,
		logger:               logger,
	}
}

// SendMultiWorkflowHistory sends multiple run IDs's history events to remote
func (h *HistoryRereplicatorImpl) SendMultiWorkflowHistory(domainID string, workflowID string,
	beginningRunID string, beginningFirstEventID int64, endingRunID string, endingNextEventID int64) (err error) {
	// NOTE: beginning run ID and ending run ID can be different
	// the logic will try to grab events from beginning run ID, first event ID to ending run ID, ending next event ID

	// beginningRunID can be empty, if there is no workflow in DB
	// endingRunID must not be empty, since this function is trigger missing events of endingRunID

	rereplicationContext := newHistoryRereplicationContext(domainID, workflowID, beginningRunID, beginningFirstEventID, endingRunID, endingNextEventID, h)

	runID := beginningRunID
	for len(runID) != 0 && runID != endingRunID {
		firstEventID, nextEventID := rereplicationContext.eventIDRange(runID, beginningRunID, beginningFirstEventID, endingRunID, endingNextEventID)
		runID, err = rereplicationContext.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		if err != nil {
			return err
		}
	}

	if runID == endingRunID {
		firstEventID, nextEventID := rereplicationContext.eventIDRange(runID, beginningRunID, beginningFirstEventID, endingRunID, endingNextEventID)
		_, err = rereplicationContext.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		return err
	}

	// we have runID being empty string
	// this means that beginningRunID to endingRunID does not have a continue as new relation ship
	// such as beginningRunID -> runID1, then runID2 (no continued as new), then runID3 -> endingRunID
	// need to work backwards using endingRunID,
	// moreover, in the above case, runID2 cannot be replicated since no one points to it

	// runIDs to be use to resend history, in reverse order
	runIDs := []string{endingRunID}
	runID = endingRunID
	for len(runID) != 0 {
		runID, err = rereplicationContext.getPrevRunID(domainID, workflowID, runID)
		if err != nil {
			if _, ok := err.(*serviceerror.NotFound); ok {
				// it is possible that this run ID (input)'s corresponding history does not exists
				break
			}
			return err
		}
		runIDs = append(runIDs, runID)
	}

	// the last run ID append in the array is empty, or last run ID's corresponding history is deleted
	// for all the other run IDs, send the history
	for index := len(runIDs) - 2; index > -1; index-- {
		runID = runIDs[index]
		firstEventID, nextEventID := rereplicationContext.eventIDRange(runID, beginningRunID, beginningFirstEventID, endingRunID, endingNextEventID)
		_, err = rereplicationContext.sendSingleWorkflowHistory(domainID, workflowID, runID, firstEventID, nextEventID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *historyRereplicationContext) sendSingleWorkflowHistory(domainID string, workflowID string, runID string,
	firstEventID int64, nextEventID int64) (string, error) {

	if firstEventID == nextEventID {
		// this is handling the case where the first event of a workflow triggers a resend
		return "", nil
	}

	var pendingRequest *historyservice.ReplicateRawEventsRequest // pending replication request to history, initialized to nil

	var replicationInfo map[string]*replication.ReplicationInfo

	var token []byte
	for doPaging := true; doPaging; doPaging = len(token) > 0 {
		response, err := c.getHistory(domainID, workflowID, runID, firstEventID, nextEventID, token, defaultPageSize)
		if err != nil {
			return "", err
		}

		replicationInfo = response.ReplicationInfo
		token = response.NextPageToken

		if len(response.HistoryBatches) == 0 {
			// this case can happen if standby side try to fetch history events
			// from active while active's history length < standby's history length
			// due to standby containing stale history
			return "", c.handleEmptyHistory(domainID, workflowID, runID, replicationInfo)
		}

		for _, batch := range response.HistoryBatches {
			// it is intentional that the first request is nil
			// the reason is, we need to check the last request, if that request contains
			// continue as new, then new run history shall be included
			err := c.sendReplicationRawRequest(pendingRequest)
			if err != nil {
				return "", err
			}
			pendingRequest = c.createReplicationRawRequest(domainID, workflowID, runID, batch, replicationInfo)
		}
	}
	// after this for loop, there shall be one request not sent yet
	// this request contains the last event, possible continue as new event
	lastBatch := pendingRequest.History
	nextRunID, err := c.getNextRunID(lastBatch)
	if err != nil {
		return "", err
	}
	if len(nextRunID) > 0 {
		// last event is continue as new
		// we need to do something special for that
		var token []byte
		pageSize := int32(1)
		response, err := c.getHistory(domainID, workflowID, nextRunID, common.FirstEventID, common.EndEventID, token, pageSize)
		if err != nil {
			return "", err
		}

		batch := response.HistoryBatches[0]

		pendingRequest.NewRunHistory = batch
	}

	return nextRunID, c.sendReplicationRawRequest(pendingRequest)
}

func (c *historyRereplicationContext) eventIDRange(currentRunID string,
	beginningRunID string, beginningFirstEventID int64,
	endingRunID string, endingNextEventID int64) (int64, int64) {

	if beginningRunID == endingRunID {
		return beginningFirstEventID, endingNextEventID
	}

	// beginningRunID != endingRunID

	if currentRunID == beginningRunID {
		// return all events from beginningFirstEventID to the end
		return beginningFirstEventID, common.EndEventID
	}

	if currentRunID == endingRunID {
		// return all events from the beginning to endingNextEventID
		return common.FirstEventID, endingNextEventID
	}

	// for everything else, just dump the entire history
	return common.FirstEventID, common.EndEventID
}

func (c *historyRereplicationContext) createReplicationRawRequest(
	domainID string, workflowID string, runID string,
	historyBlob *commonproto.DataBlob,
	replicationInfo map[string]*replication.ReplicationInfo,
) *historyservice.ReplicateRawEventsRequest {

	request := &historyservice.ReplicateRawEventsRequest{
		DomainUUID: domainID,
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ReplicationInfo: replicationInfo,
		History:         historyBlob,
		// NewRunHistory this will be handled separately
	}

	return request
}

func (c *historyRereplicationContext) sendReplicationRawRequest(request *historyservice.ReplicateRawEventsRequest) error {

	if request == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.rereplicator.replicationTimeout)
	defer func() {
		cancel()
		c.rpcCalls++
	}()
	err := c.rereplicator.historyReplicationFn(ctx, request)
	if err == nil {
		return nil
	}

	logger := c.logger.WithTags(tag.WorkflowEndingRunID(request.WorkflowExecution.GetRunId()))

	// sometimes there can be case when the first re-replication call
	// trigger an history reset and this reset can leave a hole in target
	// workflow, we should amend that hole and continue
	retryErr, ok := err.(*serviceerror.RetryTask)
	if !ok {
		logger.Error("error sending history", tag.Error(err))
		return err
	}
	if c.rpcCalls > 0 {
		logger.Error("encounter RetryTaskError not in first call")
		return err
	}
	if retryErr.RunId != c.beginningRunID {
		logger.Error("encounter RetryTaskError with non expected run ID")
		return err
	}
	if retryErr.NextEventId >= c.beginningFirstEventID {
		logger.Error("encounter RetryTaskError with larger event ID")
		return err
	}

	_, err = c.sendSingleWorkflowHistory(c.domainID, c.workflowID, retryErr.RunId,
		retryErr.NextEventId, c.beginningFirstEventID)
	if err != nil {
		logger.Error("error sending history", tag.Error(err))
		return err
	}

	// after the amend of the missing history events after history reset, redo the request
	ctxAgain, cancelAgain := context.WithTimeout(context.Background(), c.rereplicator.replicationTimeout)
	defer cancelAgain()
	return c.rereplicator.historyReplicationFn(ctxAgain, request)
}

func (c *historyRereplicationContext) handleEmptyHistory(domainID string, workflowID string, runID string,
	replicationInfo map[string]*replication.ReplicationInfo) error {

	if c.seenEmptyEvents {
		c.logger.Error("error, encounter empty history more than once", tag.WorkflowRunID(runID))
		return ErrNoHistoryRawEventBatches
	}

	c.seenEmptyEvents = true
	ri, ok := replicationInfo[c.rereplicator.targetClusterName]
	var firstEventID int64
	if !ok {
		firstEventID = common.FirstEventID
	} else {
		firstEventID = ri.GetLastEventId() + 1
	}
	nextEventID := common.EndEventID

	_, err := c.sendSingleWorkflowHistory(
		domainID,
		workflowID,
		runID,
		firstEventID,
		nextEventID,
	)
	if err != nil {
		c.logger.Error("error sending history", tag.WorkflowRunID(runID), tag.WorkflowFirstEventID(firstEventID), tag.WorkflowNextEventID(nextEventID), tag.Error(err))
	}
	return err
}

func (c *historyRereplicationContext) getHistory(
	domainID string,
	workflowID string,
	runID string,
	firstEventID int64,
	nextEventID int64,
	token []byte,
	pageSize int32,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

	logger := c.logger.WithTags(tag.WorkflowRunID(runID), tag.WorkflowFirstEventID(firstEventID), tag.WorkflowNextEventID(nextEventID))

	domainEntry, err := c.rereplicator.domainCache.GetDomainByID(domainID)
	if err != nil {
		logger.Error("error getting domain", tag.Error(err))
		return nil, err
	}
	domainName := domainEntry.GetInfo().Name

	ctx, cancel := rpc.NewContextWithTimeoutAndHeaders(c.rereplicator.replicationTimeout)
	defer cancel()
	response, err := c.rereplicator.adminClient.GetWorkflowExecutionRawHistory(ctx, &adminservice.GetWorkflowExecutionRawHistoryRequest{
		Domain: domainName,
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		FirstEventId:    firstEventID,
		NextEventId:     nextEventID,
		MaximumPageSize: pageSize,
		NextPageToken:   token,
	})

	if err != nil {
		logger.Error("error getting history", tag.Error(err))
		return nil, err
	}

	return response, nil
}

func (c *historyRereplicationContext) getPrevRunID(domainID string, workflowID string, runID string) (string, error) {

	var token []byte // use nil since we are only getting the first event batch, for the start event
	pageSize := int32(1)
	response, err := c.getHistory(domainID, workflowID, runID, common.FirstEventID, common.EndEventID, token, pageSize)
	if err != nil {
		return "", err
	}
	if len(response.HistoryBatches) == 0 {
		// is it possible that remote mutable state / history are deleted, while mutable state still accessible from cache
		// treat this case entity not exists
		return "", serviceerror.NewNotFound("")
	}

	blob := response.HistoryBatches[0]
	historyEvents, err := c.deserializeBlob(blob)
	if err != nil {
		return "", err
	}

	firstEvent := historyEvents[0]
	attr := firstEvent.GetWorkflowExecutionStartedEventAttributes()
	if attr == nil {
		// malformed first event batch
		return "", ErrFirstHistoryRawEventBatch
	}
	return attr.GetContinuedExecutionRunId(), nil
}

func (c *historyRereplicationContext) getNextRunID(blob *commonproto.DataBlob) (string, error) {

	historyEvents, err := c.deserializeBlob(blob)
	if err != nil {
		return "", err
	}

	lastEvent := historyEvents[len(historyEvents)-1]
	attr := lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes()
	if attr == nil {
		// either workflow has not finished, or finished but not continue as new
		return "", nil
	}
	return attr.GetNewExecutionRunId(), nil
}

func (c *historyRereplicationContext) deserializeBlob(blob *commonproto.DataBlob) ([]*commonproto.HistoryEvent, error) {
	var historyEvents []*commonproto.HistoryEvent

	switch blob.GetEncodingType() {
	case enums.EncodingTypeProto3:
		he, err := c.rereplicator.serializer.DeserializeBatchEvents(&serialization.DataBlob{
			Encoding: common.EncodingTypeThriftRW,
			Data:     blob.Data,
		})
		if err != nil {
			return nil, err
		}
		historyEvents = he
	default:
		return nil, ErrUnknownEncodingType
	}

	return historyEvents, nil
}
