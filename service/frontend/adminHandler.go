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
	"github.com/olivere/elastic"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/admin/adminserviceserver"
	h "github.com/uber/cadence/.gen/go/history"
	hist "github.com/uber/cadence/.gen/go/history"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
	historyService "github.com/uber/cadence/service/history"
)

var _ adminserviceserver.Interface = (*AdminHandler)(nil)

type (
	// AdminHandler - Thrift handler interface for admin service
	AdminHandler struct {
		status                int32
		numberOfHistoryShards int
		service.Service
		history       history.Client
		domainCache   cache.DomainCache
		metricsClient metrics.Client
		historyV2Mgr  persistence.HistoryV2Manager
		startWG       sync.WaitGroup
		params        *service.BootstrapParams
	}

	getWorkflowRawHistoryV2Token struct {
		DomainName        string
		WorkflowID        string
		RunID             string
		StartEventID      int64
		StartEventVersion int64
		EndEventID        int64
		EndEventVersion   int64
		PersistenceToken  []byte
		VersionHistories  *gen.VersionHistories
	}
)

// NewAdminHandler creates a thrift handler for the cadence admin service
func NewAdminHandler(
	sVice service.Service,
	numberOfHistoryShards int,
	domainCache cache.DomainCache,
	historyV2Mgr persistence.HistoryV2Manager,
	params *service.BootstrapParams,
) *AdminHandler {
	handler := &AdminHandler{
		status:                common.DaemonStatusInitialized,
		numberOfHistoryShards: numberOfHistoryShards,
		Service:               sVice,
		domainCache:           domainCache,
		historyV2Mgr:          historyV2Mgr,
		params:                params,
	}
	// prevent us from trying to serve requests before handler's Start() is complete
	handler.startWG.Add(1)
	return handler
}

// RegisterHandler register this handler, must be called before Start()
func (adh *AdminHandler) RegisterHandler() {
	adh.Service.GetDispatcher().Register(adminserviceserver.New(adh))
}

// Start starts the handler
func (adh *AdminHandler) Start() error {
	if !atomic.CompareAndSwapInt32(&adh.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return nil
	}

	adh.domainCache.Start()

	adh.history = adh.GetClientBean().GetHistoryClient()
	adh.metricsClient = adh.Service.GetMetricsClient()
	adh.startWG.Done()
	return nil
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	if !atomic.CompareAndSwapInt32(&adh.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	adh.Service.Stop()
	adh.domainCache.Stop()
}

// AddSearchAttribute add search attribute to whitelist
func (adh *AdminHandler) AddSearchAttribute(ctx context.Context, request *admin.AddSearchAttributeRequest) error {
	// validate request
	if request == nil {
		return &gen.BadRequestError{Message: "Request is not provided"}
	}
	if len(request.GetSearchAttribute()) == 0 {
		return &gen.BadRequestError{Message: "SearchAttributes are not provided"}
	}

	searchAttr := request.GetSearchAttribute()
	currentValidAttr, _ := adh.params.DynamicConfig.GetMapValue(
		dynamicconfig.ValidSearchAttributes, nil, definition.GetDefaultIndexedKeys())
	for k, v := range searchAttr {
		if definition.IsSystemIndexedKey(k) {
			return &gen.BadRequestError{Message: fmt.Sprintf("Key [%s] is reserverd by system", k)}
		}
		if _, exist := currentValidAttr[k]; exist {
			return &gen.BadRequestError{Message: fmt.Sprintf("Key [%s] is already whitelist", k)}
		}

		currentValidAttr[k] = int(v)
	}

	// update dynamic config
	err := adh.params.DynamicConfig.UpdateValue(dynamicconfig.ValidSearchAttributes, currentValidAttr)
	if err != nil {
		return &gen.InternalServiceError{Message: fmt.Sprintf("Failed to update dynamic config, err: %v", err)}
	}

	// update elasticsearch mapping, new added field will not be able to remove or update
	index := adh.params.ESConfig.GetVisibilityIndex()
	for k, v := range searchAttr {
		valueType := convertIndexedValueTypeToESDataType(v)
		if len(valueType) == 0 {
			return &gen.BadRequestError{Message: fmt.Sprintf("Unknown value type, %v", v)}
		}
		err := adh.params.ESClient.PutMapping(ctx, index, definition.Attr, k, valueType)
		if elastic.IsNotFound(err) {
			err = adh.params.ESClient.CreateIndex(ctx, index)
			if err != nil {
				return &gen.InternalServiceError{Message: fmt.Sprintf("Failed to create ES index, err: %v", err)}
			}
			err = adh.params.ESClient.PutMapping(ctx, index, definition.Attr, k, valueType)
		}
		if err != nil {
			return &gen.InternalServiceError{Message: fmt.Sprintf("Failed to update ES mapping, err: %v", err)}
		}
	}

	return nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeWorkflowExecution(ctx context.Context, request *admin.DescribeWorkflowExecutionRequest) (resp *admin.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminDescribeWorkflowExecutionScope
	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	shardID := common.WorkflowIDToHistoryShard(*request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDstr := string(shardID)
	shardIDForOutput := strconv.Itoa(shardID)

	historyHost, err := adh.GetMembershipMonitor().Lookup(common.HistoryServiceName, shardIDstr)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	domainID, err := adh.domainCache.GetDomainID(request.GetDomain())

	historyAddr := historyHost.GetAddress()
	resp2, err := adh.history.DescribeMutableState(ctx, &hist.DescribeMutableStateRequest{
		DomainUUID: &domainID,
		Execution:  request.Execution,
	})
	if err != nil {
		return &admin.DescribeWorkflowExecutionResponse{}, err
	}
	return &admin.DescribeWorkflowExecutionResponse{
		ShardId:                common.StringPtr(shardIDForOutput),
		HistoryAddr:            common.StringPtr(historyAddr),
		MutableStateInDatabase: resp2.MutableStateInDatabase,
		MutableStateInCache:    resp2.MutableStateInCache,
	}, err
}

// RemoveTask returns information about the internal states of a history host
func (adh *AdminHandler) RemoveTask(ctx context.Context, request *gen.RemoveTaskRequest) (retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminRemoveTaskScope
	if request == nil || request.ShardID == nil || request.Type == nil || request.TaskID == nil {
		return adh.error(errRequestNotSet, scope)
	}
	err := adh.history.RemoveTask(ctx, request)
	return err
}

// CloseShard returns information about the internal states of a history host
func (adh *AdminHandler) CloseShard(ctx context.Context, request *gen.CloseShardRequest) (retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminCloseShardTaskScope
	if request == nil || request.ShardID == nil {
		return adh.error(errRequestNotSet, scope)
	}
	err := adh.history.CloseShard(ctx, request)
	return err
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *gen.DescribeHistoryHostRequest) (resp *gen.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminDescribeHistoryHostScope
	if request == nil || (request.ShardIdForHost == nil && request.ExecutionForHost == nil && request.HostAddress == nil) {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.ExecutionForHost != nil {
		if err := validateExecution(request.ExecutionForHost); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	resp, err := adh.history.DescribeHistoryHost(ctx, request)
	return resp, err
}

// GetWorkflowExecutionRawHistory - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *admin.GetWorkflowExecutionRawHistoryRequest,
) (resp *admin.GetWorkflowExecutionRawHistoryResponse, retError error) {

	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope := metrics.AdminGetWorkflowExecutionRawHistoryScope
	sw := adh.startRequestProfile(scope)
	defer sw.Stop()
	var err error
	var size int

	domainID, err := adh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}
	domainScope := adh.metricsClient.Scope(scope, metrics.DomainTag(request.GetDomain()))

	execution := request.Execution
	if len(execution.GetWorkflowId()) == 0 {
		return nil, &gen.BadRequestError{Message: "Invalid WorkflowID."}
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if len(execution.GetRunId()) == 0 || uuid.Parse(execution.GetRunId()) == nil {
		return nil, &gen.BadRequestError{Message: "Invalid RunID."}
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return nil, &gen.BadRequestError{Message: "Invalid PageSize."}
	}

	var token *getHistoryContinuationToken
	// initialize or validate the token
	// token will be used as a source of truth
	if request.NextPageToken != nil {
		token, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}

		if execution.GetRunId() != token.RunID ||
			// we guarantee to use the first event ID provided in the request
			request.GetFirstEventId() != token.FirstEventID ||
			// the next event ID in the request must be <= next event ID from mutable state, when initialized
			// so as long as customer do not change next event ID during pagination,
			// next event ID in the token <= next event ID in the request.
			request.GetNextEventId() < token.NextEventID {
			return nil, &gen.BadRequestError{Message: "Invalid pagination token."}
		}

		// for the rest variables in the token, since we do not do hmac,
		// the only thing can be done is to trust the token:
		// IsWorkflowRunning: not used
		// TransientDecision: not used
		// PersistenceToken: trust
		// ReplicationInfo: trust

	} else {
		firstEventID := request.GetFirstEventId()
		nextEventID := request.GetNextEventId()
		if firstEventID < 0 || firstEventID > nextEventID {
			return nil, &gen.BadRequestError{Message: "Invalid FirstEventID && NextEventID combination."}
		}

		response, err := adh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  execution,
		})
		if err != nil {
			return nil, err
		}

		// check if the input next event ID is > actual next event ID in the mutable state
		// since we should not leak invalid events
		if nextEventID > response.GetNextEventId() {
			nextEventID = response.GetNextEventId()
		}
		token = &getHistoryContinuationToken{
			RunID:            execution.GetRunId(),
			BranchToken:      response.CurrentBranchToken,
			FirstEventID:     firstEventID,
			NextEventID:      nextEventID,
			PersistenceToken: nil, // this is the initialized value
			ReplicationInfo:  response.ReplicationInfo,
		}
	}

	if token.FirstEventID >= token.NextEventID {
		return &admin.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches:  []*gen.DataBlob{},
			ReplicationInfo: token.ReplicationInfo,
			NextPageToken:   nil, // no further pagination
		}, nil
	}

	// TODO need to deal with transient decision if to be used by client getting history
	var historyBatches []*gen.History
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowId(), adh.numberOfHistoryShards)
	_, historyBatches, token.PersistenceToken, size, err = historyService.PaginateHistory(
		adh.historyV2Mgr,
		true, // this means that we are getting history by batch
		token.BranchToken,
		token.FirstEventID,
		token.NextEventID,
		token.PersistenceToken,
		pageSize,
		common.IntPtr(shardID),
	)
	if err != nil {
		if _, ok := err.(*gen.EntityNotExistsError); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &admin.GetWorkflowExecutionRawHistoryResponse{
				HistoryBatches:  []*gen.DataBlob{},
				ReplicationInfo: token.ReplicationInfo,
				NextPageToken:   nil, // no further pagination
			}, nil
		}
		return nil, err
	}

	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// domains along with the individual domains stats
	adh.metricsClient.RecordTimer(scope, metrics.HistorySize, time.Duration(size))
	domainScope.RecordTimer(metrics.HistorySize, time.Duration(size))

	blobs := []*gen.DataBlob{}
	for _, historyBatch := range historyBatches {
		blob, err := adh.GetPayloadSerializer().SerializeBatchEvents(historyBatch.Events, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, &gen.DataBlob{
			EncodingType: gen.EncodingTypeThriftRW.Ptr(),
			Data:         blob.Data,
		})
	}

	result := &admin.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  blobs,
		ReplicationInfo: token.ReplicationInfo,
	}
	if len(token.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeHistoryToken(token)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// GetWorkflowExecutionRawHistoryV2 - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
) (resp *admin.GetWorkflowExecutionRawHistoryV2Response, retError error) {

	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope := metrics.AdminGetWorkflowExecutionRawHistoryV2Scope
	sw := adh.startRequestProfile(scope)
	defer sw.Stop()

	if err := adh.validateGetWorkflowExecutionRawHistoryV2Request(
		request,
	); err != nil {
		return nil, adh.error(err, scope)
	}
	domainID, err := adh.domainCache.GetDomainID(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}

	execution := request.Execution
	var pageToken *getWorkflowRawHistoryV2Token
	var versionHistory *persistence.VersionHistory
	if request.NextPageToken == nil {
		response, err := adh.history.GetMutableState(ctx, &h.GetMutableStateRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  execution,
		})
		if err != nil {
			return nil, adh.error(err, scope)
		}

		versionHistories := persistence.NewVersionHistoriesFromThrift(
			response.GetVersionHistories(),
		)
		versionHistory, err = adh.updateEventRange(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, adh.error(err, scope)
		}

		pageToken = adh.generatePaginationToken(request, versionHistories)
	} else {
		pageToken, err = deserializeRawHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, adh.error(err, scope)
		}
		versionHistories := pageToken.VersionHistories
		if versionHistories == nil {
			return nil, adh.error(&gen.BadRequestError{Message: "Invalid version histories."}, scope)
		}
		versionHistory, err = adh.updateEventRange(
			request,
			persistence.NewVersionHistoriesFromThrift(versionHistories),
		)
		if err != nil {
			return nil, adh.error(err, scope)
		}
	}

	if err := adh.validatePaginationToken(
		request,
		pageToken,
	); err != nil {
		return nil, adh.error(err, scope)
	}

	pageSize := int(request.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	rawHistoryResponse, err := adh.historyV2Mgr.ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistoryV2 is exclusive exclusive.
		// ReadRawHistoryBranch is inclusive exclusive.
		MinEventID:    pageToken.StartEventID + 1,
		MaxEventID:    pageToken.EndEventID,
		PageSize:      pageSize,
		NextPageToken: pageToken.PersistenceToken,
		ShardID:       common.IntPtr(shardID),
	})
	if err != nil {
		if _, ok := err.(*gen.EntityNotExistsError); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &admin.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*gen.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: versionHistory.ToThrift(),
			}, nil
		}
		return nil, err
	}

	pageToken.PersistenceToken = rawHistoryResponse.NextPageToken
	size := rawHistoryResponse.Size
	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// domains along with the individual domains stats
	adh.metricsClient.RecordTimer(scope, metrics.HistorySize, time.Duration(size))
	domainScope := adh.metricsClient.Scope(scope, metrics.DomainTag(request.GetDomain()))
	domainScope.RecordTimer(metrics.HistorySize, time.Duration(size))

	rawBlobs := rawHistoryResponse.HistoryEventBlobs
	blobs := []*gen.DataBlob{}
	for _, blob := range rawBlobs {
		blobs = append(blobs, blob.ToThrift())
	}

	result := &admin.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: blobs,
		VersionHistory: versionHistory.ToThrift(),
	}
	if len(pageToken.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeRawHistoryToken(pageToken)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (adh *AdminHandler) validateGetWorkflowExecutionRawHistoryV2Request(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
) error {

	execution := request.Execution
	if len(execution.GetWorkflowId()) == 0 {
		return &gen.BadRequestError{Message: "Invalid WorkflowID."}
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if len(execution.GetRunId()) == 0 || uuid.Parse(execution.GetRunId()) == nil {
		return &gen.BadRequestError{Message: "Invalid RunID."}
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return &gen.BadRequestError{Message: "Invalid PageSize."}
	}

	if request.StartEventId == nil &&
		request.StartEventVersion == nil &&
		request.EndEventId == nil &&
		request.EndEventVersion == nil {
		return &gen.BadRequestError{Message: "Invalid event query range."}
	}

	if (request.StartEventId != nil && request.StartEventVersion == nil) ||
		(request.StartEventId == nil && request.StartEventVersion != nil) {
		return &gen.BadRequestError{Message: "Invalid start event id and start event version combination."}
	}

	if (request.EndEventId != nil && request.EndEventVersion == nil) ||
		(request.EndEventId == nil && request.EndEventVersion != nil) {
		return &gen.BadRequestError{Message: "Invalid end event id and end event version combination."}
	}
	return nil
}

func (adh *AdminHandler) updateEventRange(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) (*persistence.VersionHistory, error) {

	targetBranch, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	if request.StartEventId == nil || request.StartEventVersion == nil {
		firstItem, err := targetBranch.GetFirstItem()
		if err != nil {
			return nil, err
		}
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		request.StartEventId = common.Int64Ptr(common.FirstEventID - 1)
		request.StartEventVersion = common.Int64Ptr(firstItem.GetVersion())
	}
	isEndBoundarySet := true
	if request.EndEventId == nil || request.EndEventVersion == nil {
		lastItem, err := targetBranch.GetLastItem()
		if err != nil {
			return nil, err
		}
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 1 here
		request.EndEventId = common.Int64Ptr(lastItem.GetEventID() + 1)
		request.EndEventVersion = common.Int64Ptr(lastItem.GetVersion())
		isEndBoundarySet = false
	}

	if request.GetStartEventId() < 0 {
		return nil, &gen.BadRequestError{Message: "Invalid FirstEventID && NextEventID combination."}
	}

	// get branch based on the end event
	if isEndBoundarySet {
		endItem := persistence.NewVersionHistoryItem(request.GetEndEventId(), request.GetEndEventVersion())
		idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(endItem)
		if err != nil {
			return nil, err
		}
		targetBranch, err = versionHistories.GetVersionHistory(idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := persistence.NewVersionHistoryItem(request.GetStartEventId(), request.GetStartEventVersion())
	// Start event is not on the same branch as target branch
	if !targetBranch.ContainsItem(startItem) {
		idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(startItem)
		if err != nil {
			return nil, err
		}
		startBranch, err := versionHistories.GetVersionHistory(idx)
		if err != nil {
			return nil, err
		}
		startItem, err = targetBranch.FindLCAItem(startBranch)
		if err != nil {
			return nil, err
		}
		request.StartEventId = common.Int64Ptr(startItem.GetEventID())
		request.StartEventVersion = common.Int64Ptr(startItem.GetVersion())
	}

	return targetBranch, nil
}

func (adh *AdminHandler) generatePaginationToken(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *getWorkflowRawHistoryV2Token {

	execution := request.Execution
	return &getWorkflowRawHistoryV2Token{
		DomainName:        request.GetDomain(),
		WorkflowID:        execution.GetWorkflowId(),
		RunID:             execution.GetRunId(),
		StartEventID:      request.GetStartEventId(),
		StartEventVersion: request.GetStartEventVersion(),
		EndEventID:        request.GetEndEventId(),
		EndEventVersion:   request.GetEndEventVersion(),
		VersionHistories:  versionHistories.ToThrift(),
		PersistenceToken:  nil, // this is the initialized value
	}
}

func (adh *AdminHandler) validatePaginationToken(
	request *admin.GetWorkflowExecutionRawHistoryV2Request,
	token *getWorkflowRawHistoryV2Token,
) error {

	execution := request.Execution
	if request.GetDomain() != token.DomainName ||
		execution.GetWorkflowId() != token.WorkflowID ||
		execution.GetRunId() != token.RunID ||
		request.GetStartEventId() != token.StartEventID ||
		request.GetStartEventVersion() != token.StartEventVersion ||
		request.GetEndEventId() != token.EndEventID ||
		request.GetEndEventVersion() != token.EndEventVersion {
		return &gen.BadRequestError{Message: "Invalid pagination token."}
	}
	return nil
}

// startRequestProfile initiates recording of request metrics
func (adh *AdminHandler) startRequestProfile(scope int) tally.Stopwatch {
	adh.startWG.Wait()
	sw := adh.metricsClient.StartTimer(scope, metrics.CadenceLatency)
	adh.metricsClient.IncCounter(scope, metrics.CadenceRequests)
	return sw
}

func (adh *AdminHandler) error(err error, scope int) error {
	switch err.(type) {
	case *gen.InternalServiceError:
		adh.Service.GetLogger().Error("Internal service error", tag.Error(err))
		return err
	case *gen.BadRequestError:
		return err
	case *gen.ServiceBusyError:
		return err
	case *gen.EntityNotExistsError:
		return err
	default:
		adh.Service.GetLogger().Error("Uncategorized error", tag.Error(err))
		return &gen.InternalServiceError{Message: err.Error()}
	}
}

func convertIndexedValueTypeToESDataType(valueType gen.IndexedValueType) string {
	switch valueType {
	case gen.IndexedValueTypeString:
		return "text"
	case gen.IndexedValueTypeKeyword:
		return "keyword"
	case gen.IndexedValueTypeInt:
		return "long"
	case gen.IndexedValueTypeDouble:
		return "double"
	case gen.IndexedValueTypeBool:
		return "boolean"
	case gen.IndexedValueTypeDatetime:
		return "date"
	default:
		return ""
	}
}

func serializeRawHistoryToken(token *getWorkflowRawHistoryV2Token) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(token)
	return bytes, err
}

func deserializeRawHistoryToken(bytes []byte) (*getWorkflowRawHistoryV2Token, error) {
	token := &getWorkflowRawHistoryV2Token{}
	err := json.Unmarshal(bytes, token)
	return token, err
}
