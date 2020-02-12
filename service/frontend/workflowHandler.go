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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	h "github.com/temporalio/temporal/.gen/go/history"
	m "github.com/temporalio/temporal/.gen/go/matching"
	gen "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/client"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/elasticsearch/validator"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/quotas"
	"github.com/temporalio/temporal/common/resource"
)

type (
	// WorkflowHandler - Thrift handler interface for workflow service
	WorkflowHandler struct {
		resource.Resource

		tokenSerializer           common.TaskTokenSerializer
		rateLimiter               quotas.Policy
		config                    *Config
		versionChecker            client.VersionChecker
		domainHandler             domain.Handler
		visibilityQueryValidator  *validator.VisibilityQueryValidator
		searchAttributesValidator *validator.SearchAttributesValidator
	}

	getHistoryContinuationToken struct {
		RunID             string
		FirstEventID      int64
		NextEventID       int64
		IsWorkflowRunning bool
		PersistenceToken  []byte
		TransientDecision *gen.TransientDecisionInfo
		BranchToken       []byte
		ReplicationInfo   map[string]*gen.ReplicationInfo
	}
)

var (
	errDomainNotSet                               = &gen.BadRequestError{Message: "Domain not set on request."}
	errTaskTokenNotSet                            = &gen.BadRequestError{Message: "Task token not set on request."}
	errInvalidTaskToken                           = &gen.BadRequestError{Message: "Invalid TaskToken."}
	errTaskListNotSet                             = &gen.BadRequestError{Message: "TaskList is not set on request."}
	errTaskListTypeNotSet                         = &gen.BadRequestError{Message: "TaskListType is not set on request."}
	errExecutionNotSet                            = &gen.BadRequestError{Message: "Execution is not set on request."}
	errWorkflowIDNotSet                           = &gen.BadRequestError{Message: "WorkflowId is not set on request."}
	errActivityIDNotSet                           = &gen.BadRequestError{Message: "ActivityID is not set on request."}
	errInvalidRunID                               = &gen.BadRequestError{Message: "Invalid RunId."}
	errInvalidNextPageToken                       = &gen.BadRequestError{Message: "Invalid NextPageToken."}
	errNextPageTokenRunIDMismatch                 = &gen.BadRequestError{Message: "RunID in the request does not match the NextPageToken."}
	errQueryNotSet                                = &gen.BadRequestError{Message: "WorkflowQuery is not set on request."}
	errQueryTypeNotSet                            = &gen.BadRequestError{Message: "QueryType is not set on request."}
	errRequestNotSet                              = &gen.BadRequestError{Message: "Request is nil."}
	errNoPermission                               = &gen.BadRequestError{Message: "No permission to do this operation."}
	errRequestIDNotSet                            = &gen.BadRequestError{Message: "RequestId is not set on request."}
	errWorkflowTypeNotSet                         = &gen.BadRequestError{Message: "WorkflowType is not set on request."}
	errInvalidRetention                           = &gen.BadRequestError{Message: "RetentionDays is invalid."}
	errInvalidExecutionStartToCloseTimeoutSeconds = &gen.BadRequestError{Message: "A valid ExecutionStartToCloseTimeoutSeconds is not set on request."}
	errInvalidTaskStartToCloseTimeoutSeconds      = &gen.BadRequestError{Message: "A valid TaskStartToCloseTimeoutSeconds is not set on request."}
	errQueryDisallowedForDomain                   = &gen.BadRequestError{Message: "Domain is not allowed to query, please contact cadence team to re-enable queries."}
	errClusterNameNotSet                          = &gen.BadRequestError{Message: "Cluster name is not set."}
	errEmptyReplicationInfo                       = &gen.BadRequestError{Message: "Replication task info is not set."}

	// err for archival
	errHistoryNotFound = &gen.BadRequestError{Message: "Requested workflow history not found, may have passed retention period."}

	// err for string too long
	errDomainTooLong       = &gen.BadRequestError{Message: "Domain length exceeds limit."}
	errWorkflowTypeTooLong = &gen.BadRequestError{Message: "WorkflowType length exceeds limit."}
	errWorkflowIDTooLong   = &gen.BadRequestError{Message: "WorkflowID length exceeds limit."}
	errSignalNameTooLong   = &gen.BadRequestError{Message: "SignalName length exceeds limit."}
	errTaskListTooLong     = &gen.BadRequestError{Message: "TaskList length exceeds limit."}
	errRequestIDTooLong    = &gen.BadRequestError{Message: "RequestID length exceeds limit."}
	errIdentityTooLong     = &gen.BadRequestError{Message: "Identity length exceeds limit."}

	frontendServiceRetryPolicy = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a thrift handler for the cadence service
func NewWorkflowHandler(
	resource resource.Resource,
	config *Config,
	replicationMessageSink messaging.Producer,
) *WorkflowHandler {
	return &WorkflowHandler{
		Resource:        resource,
		config:          config,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		rateLimiter: quotas.NewMultiStageRateLimiter(
			func() float64 {
				return float64(config.RPS())
			},
			func(domain string) float64 {
				return float64(config.DomainRPS(domain))
			},
		),
		versionChecker: client.NewVersionChecker(),
		domainHandler: domain.NewHandler(
			config.MinRetentionDays(),
			config.MaxBadBinaries,
			resource.GetLogger(),
			resource.GetMetadataManager(),
			resource.GetClusterMetadata(),
			domain.NewDomainReplicator(replicationMessageSink, resource.GetLogger()),
			resource.GetArchivalMetadata(),
			resource.GetArchiverProvider(),
		),
		visibilityQueryValidator: validator.NewQueryValidator(config.ValidSearchAttributes),
		searchAttributesValidator: validator.NewSearchAttributesValidator(
			resource.GetLogger(),
			config.ValidSearchAttributes,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
		),
	}
}

func (wh *WorkflowHandler) getRawHistory(
	scope metrics.Scope,
	domainID string,
	execution gen.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *gen.TransientDecisionInfo,
	branchToken []byte,
) ([]*gen.DataBlob, []byte, error) {
	rawHistory := []*gen.DataBlob{}
	shardID := common.WorkflowIDToHistoryShard(*execution.WorkflowId, wh.config.NumHistoryShards)

	resp, err := wh.GetHistoryManager().ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
	})
	if err != nil {
		return nil, nil, err
	}

	var encoding *gen.EncodingType
	for _, data := range resp.HistoryEventBlobs {
		switch data.Encoding {
		case common.EncodingTypeJSON:
			encoding = gen.EncodingTypeJSON.Ptr()
		case common.EncodingTypeThriftRW:
			encoding = gen.EncodingTypeThriftRW.Ptr()
		default:
			panic(fmt.Sprintf("Invalid encoding type for raw history, encoding type: %s", data.Encoding))
		}
		rawHistory = append(rawHistory, &gen.DataBlob{
			EncodingType: encoding,
			Data:         data.Data,
		})
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}

		blob, err := wh.GetPayloadSerializer().SerializeEvent(transientDecision.ScheduledEvent, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &gen.DataBlob{
			EncodingType: gen.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		})

		blob, err = wh.GetPayloadSerializer().SerializeEvent(transientDecision.StartedEvent, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &gen.DataBlob{
			EncodingType: gen.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		})
	}

	return rawHistory, resp.NextPageToken, nil
}

func (wh *WorkflowHandler) getHistory(
	scope metrics.Scope,
	domainID string,
	execution gen.WorkflowExecution,
	firstEventID, nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientDecision *gen.TransientDecisionInfo,
	branchToken []byte,
) (*gen.History, []byte, error) {

	var historyEvents []*gen.HistoryEvent
	var size int

	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(*execution.WorkflowId, wh.config.NumHistoryShards)
	var err error
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageV2Events(wh.GetHistoryManager(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(shardID),
	})
	if err != nil {
		return nil, nil, err
	}

	scope.RecordTimer(metrics.HistorySize, time.Duration(size))

	isLastPage := len(nextPageToken) == 0
	if err := verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
		wh.GetLogger().Error("getHistory: incomplete history",
			tag.WorkflowDomainID(domainID),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
		return nil, nil, err
	}

	if len(nextPageToken) == 0 && transientDecision != nil {
		if err := wh.validateTransientDecisionEvents(nextEventID, transientDecision); err != nil {
			scope.IncCounter(metrics.CadenceErrIncompleteHistoryCounter)
			wh.GetLogger().Error("getHistory error",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient decision events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientDecision.ScheduledEvent, transientDecision.StartedEvent)
	}

	executionHistory := &gen.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) validateTransientDecisionEvents(
	expectedNextEventID int64,
	decision *gen.TransientDecisionInfo,
) error {

	if decision.ScheduledEvent.GetEventId() == expectedNextEventID &&
		decision.StartedEvent.GetEventId() == expectedNextEventID+1 {
		return nil
	}

	return fmt.Errorf(
		"invalid transient decision: "+
			"expectedScheduledEventID=%v expectedStartedEventID=%v but have scheduledEventID=%v startedEventID=%v",
		expectedNextEventID,
		expectedNextEventID+1,
		decision.ScheduledEvent.GetEventId(),
		decision.StartedEvent.GetEventId())
}

// startRequestProfile initiates recording of request metrics
func (wh *WorkflowHandler) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
	// timer should be emitted with the all tag
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

// getDefaultScope returns a default scope to use for request metrics
func (wh *WorkflowHandler) getDefaultScope(scope int) metrics.Scope {
	return wh.GetMetricsClient().Scope(scope).Tagged(metrics.DomainUnknownTag())
}

func frontendInternalServiceError(fmtStr string, args ...interface{}) error {
	// NOTE: For internal error, we can't return thrift error from cadence-frontend.
	// Because in uber internal metrics, thrift errors are counted as user errors.
	return fmt.Errorf(fmtStr, args...)
}

func (wh *WorkflowHandler) error(err error, scope metrics.Scope, tagsForErrorLog ...tag.Tag) error {
	switch err := err.(type) {
	case *gen.InternalServiceError:
		wh.GetLogger().WithTags(tagsForErrorLog...).Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return frontendInternalServiceError("cadence internal error, msg: %v", err.Message)
	case *gen.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.DomainNotActiveError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *gen.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return err
	case *gen.EntityNotExistsError:
		scope.IncCounter(metrics.CadenceErrEntityNotExistsCounter)
		return err
	case *gen.WorkflowExecutionAlreadyStartedError:
		scope.IncCounter(metrics.CadenceErrExecutionAlreadyStartedCounter)
		return err
	case *gen.DomainAlreadyExistsError:
		scope.IncCounter(metrics.CadenceErrDomainAlreadyExistsCounter)
		return err
	case *gen.CancellationAlreadyRequestedError:
		scope.IncCounter(metrics.CadenceErrCancellationAlreadyRequestedCounter)
		return err
	case *gen.QueryFailedError:
		scope.IncCounter(metrics.CadenceErrQueryFailedCounter)
		return err
	case *gen.LimitExceededError:
		scope.IncCounter(metrics.CadenceErrLimitExceededCounter)
		return err
	case *gen.ClientVersionNotSupportedError:
		scope.IncCounter(metrics.CadenceErrClientVersionNotSupportedCounter)
		return err
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			scope.IncCounter(metrics.CadenceErrContextTimeoutCounter)
			return err
		}
	}

	wh.GetLogger().WithTags(tagsForErrorLog...).Error("Uncategorized error",
		tag.Error(err))
	scope.IncCounter(metrics.CadenceFailures)
	return frontendInternalServiceError("cadence internal uncategorized error, msg: %v", err.Error())
}

func (wh *WorkflowHandler) validateTaskListType(t *gen.TaskListType, scope metrics.Scope) error {
	if t == nil {
		return wh.error(errTaskListTypeNotSet, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateTaskList(t *gen.TaskList, scope metrics.Scope) error {
	if t == nil || t.Name == nil || t.GetName() == "" {
		return wh.error(errTaskListNotSet, scope)
	}
	if len(t.GetName()) > wh.config.MaxIDLengthLimit() {
		return wh.error(errTaskListTooLong, scope)
	}
	return nil
}

func (wh *WorkflowHandler) validateExecutionAndEmitMetrics(w *gen.WorkflowExecution, scope metrics.Scope) error {
	err := validateExecution(w)
	if err != nil {
		return wh.error(err, scope)
	}
	return nil
}

func validateExecution(w *gen.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.WorkflowId == nil || w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}

func (wh *WorkflowHandler) createPollForDecisionTaskResponse(
	ctx context.Context,
	scope metrics.Scope,
	domainID string,
	matchingResp *m.PollForDecisionTaskResponse,
	branchToken []byte,
) (*gen.PollForDecisionTaskResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no decision task to be send to worker / caller
		return &gen.PollForDecisionTaskResponse{}, nil
	}

	var history *gen.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &gen.History{
			Events: []*gen.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}
		domain, dErr := wh.GetDomainCache().GetDomainByID(domainID)
		if dErr != nil {
			return nil, dErr
		}
		scope = scope.Tagged(metrics.DomainTag(domain.GetInfo().Name))
		history, persistenceToken, err = wh.getHistory(
			scope,
			domainID,
			*matchingResp.WorkflowExecution,
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(domain.GetInfo().Name)),
			nil,
			matchingResp.DecisionInfo,
			branchToken,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&getHistoryContinuationToken{
				RunID:             matchingResp.WorkflowExecution.GetRunId(),
				FirstEventID:      firstEventID,
				NextEventID:       nextEventID,
				PersistenceToken:  persistenceToken,
				TransientDecision: matchingResp.DecisionInfo,
				BranchToken:       branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &gen.PollForDecisionTaskResponse{
		TaskToken:                 matchingResp.TaskToken,
		WorkflowExecution:         matchingResp.WorkflowExecution,
		WorkflowType:              matchingResp.WorkflowType,
		PreviousStartedEventId:    matchingResp.PreviousStartedEventId,
		StartedEventId:            matchingResp.StartedEventId,
		Query:                     matchingResp.Query,
		BacklogCountHint:          matchingResp.BacklogCountHint,
		Attempt:                   matchingResp.Attempt,
		History:                   history,
		NextPageToken:             continuation,
		WorkflowExecutionTaskList: matchingResp.WorkflowExecutionTaskList,
		ScheduledTimestamp:        matchingResp.ScheduledTimestamp,
		StartedTimestamp:          matchingResp.StartedTimestamp,
		Queries:                   matchingResp.Queries,
	}

	return resp, nil
}

func verifyHistoryIsComplete(
	events []*gen.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return fmt.Errorf("invalid history: contains zero events")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // atleast one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return &gen.InternalServiceError{
				Message: fmt.Sprintf(
					"invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID),
			}
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return &gen.InternalServiceError{
		Message: fmt.Sprintf(
			"incomplete history: "+
				"expected events [%v-%v] but got events [%v-%v] of length %v:"+
				"isFirstPage=%v,isLastPage=%v,pageSize=%v",
			expectedFirstEventID,
			expectedLastEventID,
			firstEventID,
			lastEventID,
			nEvents,
			isFirstPage,
			isLastPage,
			pageSize),
	}
}

func deserializeHistoryToken(bytes []byte) (*getHistoryContinuationToken, error) {
	token := &getHistoryContinuationToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func serializeHistoryToken(token *getHistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func createServiceBusyError() *gen.ServiceBusyError {
	err := &gen.ServiceBusyError{}
	err.Message = "Too many outstanding requests to the cadence service"
	return err
}

func isFailoverRequest(updateRequest *gen.UpdateDomainRequest) bool {
	return updateRequest.ReplicationConfiguration != nil && updateRequest.ReplicationConfiguration.GetActiveClusterName() != ""
}

func (wh *WorkflowHandler) historyArchived(ctx context.Context, request *gen.GetWorkflowExecutionHistoryRequest, domainID string) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunId() == "" {
		return false
	}
	getMutableStateRequest := &h.GetMutableStateRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  request.Execution,
	}
	_, err := wh.GetHistoryClient().GetMutableState(ctx, getMutableStateRequest, versionHeaders(ctx)...)
	if err == nil {
		return false
	}
	switch err.(type) {
	case *gen.EntityNotExistsError:
		// the only case in which history is assumed to be archived is if getting mutable state returns entity not found error
		return true
	}
	return false
}

func (wh *WorkflowHandler) getArchivedHistory(
	ctx context.Context,
	request *gen.GetWorkflowExecutionHistoryRequest,
	domainID string,
	scope metrics.Scope,
) (*gen.GetWorkflowExecutionHistoryResponse, error) {
	entry, err := wh.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	URIString := entry.GetConfig().HistoryArchivalURI
	if URIString == "" {
		// if URI is empty, it means the domain has never enabled for archival.
		// the error is not "workflow has passed retention period", because
		// we have no way to tell if the requested workflow exists or not.
		return nil, wh.error(errHistoryNotFound, scope)
	}

	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	historyArchiver, err := wh.GetArchiverProvider().GetHistoryArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return nil, wh.error(err, scope)
	}

	resp, err := historyArchiver.Get(ctx, URI, &archiver.GetHistoryRequest{
		DomainID:      domainID,
		WorkflowID:    request.GetExecution().GetWorkflowId(),
		RunID:         request.GetExecution().GetRunId(),
		NextPageToken: request.GetNextPageToken(),
		PageSize:      int(request.GetMaximumPageSize()),
	})
	if err != nil {
		return nil, wh.error(err, scope)
	}

	history := &gen.History{}
	for _, batch := range resp.HistoryBatches {
		history.Events = append(history.Events, batch.Events...)
	}
	return &gen.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      common.BoolPtr(true),
	}, nil
}

func (wh *WorkflowHandler) convertIndexedKeyToThrift(keys map[string]interface{}) map[string]gen.IndexedValueType {
	converted := make(map[string]gen.IndexedValueType)
	for k, v := range keys {
		converted[k] = common.ConvertIndexedValueTypeToThriftType(v, wh.GetLogger())
	}
	return converted
}

func (wh *WorkflowHandler) isListRequestPageSizeTooLarge(pageSize int32, domain string) bool {
	return wh.config.EnableReadVisibilityFromES(domain) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
}

// TODO: Remove this func after history service gRPC migration is complete
// It sets version headers in YARPC format
func versionHeaders(ctx context.Context) []yarpc.CallOption {
	headers := client.GetHeadersValue(ctx, common.LibraryVersionHeaderName, common.FeatureVersionHeaderName, common.ClientImplHeaderName)
	return []yarpc.CallOption{
		yarpc.WithHeader(common.LibraryVersionHeaderName, headers[0]),
		yarpc.WithHeader(common.FeatureVersionHeaderName, headers[1]),
		yarpc.WithHeader(common.ClientImplHeaderName, headers[2]),
	}
}
