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

package sysworkflow

import (
	"errors"
	"strconv"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

/**
IMPORTANT: Under the assumption that history is immutable, the following iterator will deterministically return identical
blobs from Next regardless of the current cluster and regardless of the number of times this iteration occurs. This property
makes any concurrent uploads of history safe.
*/

type (
	// HistoryBlobIterator is used to get history blobs
	HistoryBlobIterator interface {
		Next() (*HistoryBlob, error)
		HasNext() bool
	}

	historyBlobIterator struct {
		logger        bark.Logger
		metricsClient metrics.Client

		// the following defines the state of the iterator
		blobPageToken        int
		persistencePageToken []byte
		finishedIteration    bool

		// the following are only used to read history and dynamic config
		historyManager       persistence.HistoryManager
		historyV2Manager     persistence.HistoryV2Manager
		domainID             string
		workflowID           string
		runID                string
		eventStoreVersion    int32
		branchToken          []byte
		lastFirstEventID     int64
		config               *Config
		domain               string
		clusterName          string
		closeFailoverVersion int64
	}
)

var (
	errIteratorDepleted = errors.New("iterator is depleted")
)

// NewHistoryBlobIterator returns a new HistoryBlobIterator
func NewHistoryBlobIterator(
	logger bark.Logger,
	metricsClient metrics.Client,
	request ArchiveRequest,
	container *SysWorkerContainer,
	domainName string,
	clusterName string,
) HistoryBlobIterator {
	return &historyBlobIterator{
		logger:        logger,
		metricsClient: metricsClient,

		blobPageToken:        common.FirstBlobPageToken,
		persistencePageToken: nil,
		finishedIteration:    false,

		historyManager:       container.HistoryManager,
		historyV2Manager:     container.HistoryV2Manager,
		domainID:             request.DomainID,
		workflowID:           request.WorkflowID,
		runID:                request.RunID,
		eventStoreVersion:    request.EventStoreVersion,
		branchToken:          request.BranchToken,
		lastFirstEventID:     request.LastFirstEventID,
		config:               container.Config,
		domain:               domainName,
		clusterName:          clusterName,
		closeFailoverVersion: request.CloseFailoverVersion,
	}
}

// Next returns historyBlob and advances iterator.
// Returns error if iterator is empty, or if history could not be read.
// If error is returned then no iterator is state is advanced.
func (i *historyBlobIterator) Next() (*HistoryBlob, error) {
	if !i.HasNext() {
		i.logger.Error("called Next on depleted iterator")
		i.metricsClient.IncCounter(metrics.HistoryBlobIteratorScope, metrics.SysWorkerNextOnDepletedIterator)
		return nil, errIteratorDepleted
	}
	events, nextPersistencePageToken, historyEndReached, err := i.readBlobEvents(i.persistencePageToken)
	if err != nil {
		i.logger.WithError(err).Error("failed to read history events to construct blob")
		i.metricsClient.IncCounter(metrics.HistoryBlobIteratorScope, metrics.SysWorkerHistoryReadEventsFailures)
		return nil, err
	}

	// only if no error was encountered reading history does the state of the iterator get advanced
	i.finishedIteration = historyEndReached
	i.persistencePageToken = nextPersistencePageToken

	firstEvent := events[0]
	lastEvent := events[len(events)-1]
	eventCount := int64(len(events))
	header := &HistoryBlobHeader{
		DomainName:           &i.domain,
		DomainID:             &i.domainID,
		WorkflowID:           &i.workflowID,
		RunID:                &i.runID,
		CurrentPageToken:     common.StringPtr(strconv.Itoa(i.blobPageToken)),
		NextPageToken:        common.StringPtr(strconv.Itoa(common.LastBlobNextPageToken)),
		FirstFailoverVersion: firstEvent.Version,
		LastFailoverVersion:  lastEvent.Version,
		FirstEventID:         firstEvent.EventId,
		LastEventID:          lastEvent.EventId,
		UploadDateTime:       common.StringPtr(time.Now().String()),
		UploadCluster:        &i.clusterName,
		EventCount:           &eventCount,
		CloseFailoverVersion: &i.closeFailoverVersion,
	}
	if i.HasNext() {
		i.blobPageToken++
		header.NextPageToken = common.StringPtr(strconv.Itoa(i.blobPageToken))
	}
	return &HistoryBlob{
		Header: header,
		Body: &shared.History{
			Events: events,
		},
	}, nil
}

// HasNext returns true if there are more items to iterate over.
func (i *historyBlobIterator) HasNext() bool {
	return !i.finishedIteration
}

// readBlobEvents gets history events, starting from page identified by given pageToken.
// Reads events until all of history has been read or enough events have been fetched to satisfy blob size target.
// If empty pageToken is given, then iteration will start from the beginning of history.
// Does not modify any iterator state (i.e. calls to readBlobEvents are idempotent).
// Returns the following four things:
// 1. HistoryEvents: Either all of history starting from given pageToken or enough history to satisfy blob size target.
// 2. NextPageToken: The page token that should be used to fetch the next chunk of history
// 3. HistoryEndReached: True if fetched all history beyond page starting from given pageToken
// 4. Error: Any error that occurred while reading history
func (i *historyBlobIterator) readBlobEvents(pageToken []byte) ([]*shared.HistoryEvent, []byte, bool, error) {
	historyEvents, size, nextPageToken, err := i.readHistory(pageToken)
	if err != nil {
		return nil, nil, false, err
	}
	// Exceeding target blob size is fine because blob will be compressed anyways (just generally want to avoid creating very small blobs).
	for len(nextPageToken) > 0 && size < i.config.TargetArchivalBlobSize(i.domain) {
		currHistoryEvents, currSize, currNextPageToken, err := i.readHistory(nextPageToken)
		if err != nil {
			return nil, nil, false, err
		}
		historyEvents = append(historyEvents, currHistoryEvents...)
		size += currSize
		nextPageToken = currNextPageToken
	}
	return historyEvents, nextPageToken, len(nextPageToken) == 0, nil
}

// readHistory fetches a single page of history events identified by given pageToken.
// Does not modify any iterator state (i.e. calls to readHistory are idempotent).
// Returns historyEvents, size, nextPageToken and error.
func (i *historyBlobIterator) readHistory(pageToken []byte) ([]*shared.HistoryEvent, int, []byte, error) {
	if i.eventStoreVersion == persistence.EventStoreVersionV2 {
		req := &persistence.ReadHistoryBranchRequest{
			BranchToken:   i.branchToken,
			MinEventID:    common.FirstEventID,
			MaxEventID:    i.lastFirstEventID,
			PageSize:      i.config.HistoryPageSize(i.domain),
			NextPageToken: pageToken,
		}
		return persistence.ReadFullPageV2Events(i.historyV2Manager, req)
	}
	req := &persistence.GetWorkflowExecutionHistoryRequest{
		DomainID: i.domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(i.workflowID),
			RunId:      common.StringPtr(i.runID),
		},
		FirstEventID:  common.FirstEventID,
		NextEventID:   i.lastFirstEventID,
		PageSize:      i.config.HistoryPageSize(i.domain),
		NextPageToken: pageToken,
	}
	resp, err := i.historyManager.GetWorkflowExecutionHistory(req)
	if err != nil {
		return nil, 0, nil, err
	}
	return resp.History.Events, resp.Size, resp.NextPageToken, nil
}
