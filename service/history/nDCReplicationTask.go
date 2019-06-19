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

package history

import (
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCReplicationTask interface {
		getDomainID() string
		getExecution() shared.WorkflowExecution
		getWorkflowID() string
		getRunID() string
		getEventTime() time.Time
		getFirstEvent() *shared.HistoryEvent
		getLastEvent() *shared.HistoryEvent
		getVersion() int64
		getSourceCluster() string
		getEvents() []*shared.HistoryEvent
		getNewRunEvents() []*shared.HistoryEvent
		getLogger() log.Logger
		getVersionHistory() *persistence.VersionHistory
		getRequest() *h.ReplicateEventsRequest
	}

	nDCReplicationTaskImpl struct {
		sourceCluster       string
		domainID            string
		execution           shared.WorkflowExecution
		version             int64
		firstEvent          *shared.HistoryEvent
		lastEvent           *shared.HistoryEvent
		eventTime           time.Time
		historyEvents       []*shared.HistoryEvent
		newRunHistoryEvents []*shared.HistoryEvent

		request *h.ReplicateEventsRequest

		startTime time.Time
		logger    log.Logger
	}
)

var (
	// ErrInvalidDomainID is returned if domain ID is invalid
	ErrInvalidDomainID = &shared.BadRequestError{Message: "invalid domain ID"}
	// ErrInvalidExecution is returned if execution is invalid
	ErrInvalidExecution = &shared.BadRequestError{Message: "invalid execution"}
	// ErrInvalidRunID is returned if run ID is invalid
	ErrInvalidRunID = &shared.BadRequestError{Message: "invalid run ID"}
	// ErrEventIDMismatch is returned if event ID mis-matched
	ErrEventIDMismatch = &shared.BadRequestError{Message: "event ID mismatch"}
	// ErrEventVersionMismatch is returned if event version mis-matched
	ErrEventVersionMismatch = &shared.BadRequestError{Message: "event version mismatch"}
)

func newNDCReplicationTask(clusterMetadata cluster.Metadata, now time.Time, logger log.Logger,
	request *h.ReplicateEventsRequest) (*nDCReplicationTaskImpl, error) {

	if err := validateReplicateEventsRequest(request); err != nil {
		return nil, err
	}

	domainID := request.GetDomainUUID()
	execution := *request.WorkflowExecution

	version := request.GetVersion()
	sourceCluster := clusterMetadata.ClusterNameForFailoverVersion(version)

	history := request.History
	var newRunHistoryEvents []*shared.HistoryEvent
	if request.NewRunHistory != nil {
		newRunHistoryEvents = request.NewRunHistory.Events
	}
	historyEvents := history.Events
	firstEvent := history.Events[0]
	lastEvent := history.Events[len(history.Events)-1]

	eventTime := int64(0)
	for _, event := range historyEvents {
		if event.GetTimestamp() > eventTime {
			eventTime = event.GetTimestamp()
		}
	}

	logger = logger.WithTags(
		tag.WorkflowID(execution.GetWorkflowId()),
		tag.WorkflowRunID(execution.GetRunId()),
		tag.SourceCluster(sourceCluster),
		tag.IncomingVersion(version),
		tag.WorkflowFirstEventID(firstEvent.GetEventId()),
		tag.WorkflowNextEventID(lastEvent.GetTaskId()+1),
	)

	return &nDCReplicationTaskImpl{
		sourceCluster: sourceCluster,
		domainID:      domainID,
		execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(execution.GetWorkflowId()),
			RunId:      common.StringPtr(execution.GetRunId()),
		},
		version:             version,
		firstEvent:          firstEvent,
		lastEvent:           lastEvent,
		eventTime:           time.Unix(0, eventTime),
		historyEvents:       historyEvents,
		newRunHistoryEvents: newRunHistoryEvents,

		request: request,

		startTime: now, // TODO use time source
		logger:    logger,
	}, nil
}

func (t *nDCReplicationTaskImpl) getDomainID() string {
	return t.domainID
}

func (t *nDCReplicationTaskImpl) getExecution() shared.WorkflowExecution {
	return t.execution
}

func (t *nDCReplicationTaskImpl) getWorkflowID() string {
	return t.execution.GetWorkflowId()
}

func (t *nDCReplicationTaskImpl) getRunID() string {
	return t.execution.GetRunId()
}

func (t *nDCReplicationTaskImpl) getEventTime() time.Time {
	return t.eventTime
}

func (t *nDCReplicationTaskImpl) getFirstEvent() *shared.HistoryEvent {
	return t.firstEvent
}

func (t *nDCReplicationTaskImpl) getLastEvent() *shared.HistoryEvent {
	return t.lastEvent
}

func (t *nDCReplicationTaskImpl) getVersion() int64 {
	return t.version
}

func (t *nDCReplicationTaskImpl) getSourceCluster() string {
	return t.sourceCluster
}

func (t *nDCReplicationTaskImpl) getEvents() []*shared.HistoryEvent {
	return t.historyEvents
}

func (t *nDCReplicationTaskImpl) getNewRunEvents() []*shared.HistoryEvent {
	return t.newRunHistoryEvents
}

func (t *nDCReplicationTaskImpl) getLogger() log.Logger {
	return t.logger
}

func (t *nDCReplicationTaskImpl) getVersionHistory() *persistence.VersionHistory {
	panic("implement this")
}

func (t *nDCReplicationTaskImpl) getRequest() *h.ReplicateEventsRequest {
	return t.request
}

func validateReplicateEventsRequest(request *h.ReplicateEventsRequest) error {
	if valid := validateUUID(request.GetDomainUUID()); !valid {
		return ErrInvalidDomainID
	}
	if request.WorkflowExecution == nil {
		return ErrInvalidExecution
	}
	if valid := validateUUID(request.WorkflowExecution.GetRunId()); !valid {
		return ErrInvalidRunID
	}
	if request.History == nil || len(request.History.Events) == 0 {
		return ErrEmptyHistoryRawEventBatch
	}
	if request.GetFirstEventId() != request.History.Events[0].GetEventId() ||
		request.GetNextEventId() != request.History.Events[len(request.History.Events)-1].GetEventId()+1 {
		return ErrEventIDMismatch
	}

	for _, event := range request.History.Events {
		if event.GetVersion() != request.GetVersion() {
			return ErrEventVersionMismatch
		}
	}

	if request.NewRunHistory != nil {
		for _, event := range request.NewRunHistory.Events {
			if event.GetVersion() != request.GetVersion() {
				return ErrEventVersionMismatch
			}
		}
	}

	return nil
}

func validateUUID(input string) bool {
	if uuid.Parse(input) == nil {
		return false
	}
	return true
}
